package crud

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
	"log"
	"strconv"
	"strings"
)

// roleToIds returns the set of role values matching the requested filter.
// 3-bit bitmask: ECON_OUT=1, ECON_IN=2, INITIATOR=4.
// Values: 0=observer, 1=econ_out, 2=econ_in, 3=econ_both, 4=initiator, 5=init_out, 6=(trace-only), 7=init_both
func roleToIds(role *models.RoleType) []int {
	if role == nil {
		return []int{0, 1, 2, 3, 4, 5, 6, 7}
	}
	switch *role {
	case models.RoleSender:
		return []int{1, 3, 5, 7}
	case models.RoleReceiver:
		return []int{2, 3, 6, 7}
	case models.RoleInitiated:
		return []int{4, 5, 6, 7}
	case models.RoleObserver:
		return []int{0}
	default:
		return []int{0, 1, 2, 3, 4, 5, 6, 7}
	}
}

func roleToValues(role *models.RoleType) string {
	roleIds := roleToIds(role)
	parts := make([]string, len(roleIds))
	for i, id := range roleIds {
		parts[i] = "(" + strconv.Itoa(id) + ")"
	}
	return "(VALUES " + strings.Join(parts, ", ") + ")"
}

func roleInClause(role *models.RoleType) string {
	roleIds := roleToIds(role)
	parts := make([]string, len(roleIds))
	for i, id := range roleIds {
		parts[i] = strconv.Itoa(id)
	}
	return strings.Join(parts, ",")
}

// ltMargin is the safety margin for utime→lt conversion via blocks table lookup.
const ltMargin = 5000000

// maxBatchSize caps the LATERAL inner LIMIT to prevent huge queries.
const maxBatchSize = 10000

// PageCursor is the opaque pagination cursor for account actions.
type PageCursor struct {
	TraceEndLt int64  `json:"t"`
	TraceId    string `json:"tr"`
}

func EncodePageCursor(c *PageCursor) string {
	if c == nil {
		return ""
	}
	b, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodePageCursor(s string) (*PageCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c PageCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// cursorCompareOp returns "<" for DESC or ">" for ASC pagination.
func cursorCompareOp(sort_order string) string {
	if sort_order == "asc" {
		return ">"
	}
	return "<"
}

// buildAccountActionsQuery builds the LATERAL query that returns visible actions
// for an account+role, joined with the actions table and filtered by supported types.
//
// It returns two queries:
//  1. The main actions query (visible actions after all filters)
//  2. A "scan edge" query that returns just the last (trace_end_lt, trace_id)
//     scanned by the LATERAL, regardless of whether any actions survived filtering.
//     This is used to advance the cursor when all actions in a batch are filtered out.
func buildAccountActionsQueries(
	req models.AccountActionsRequest,
	utime_req models.UtimeRequest,
	lt_req models.LtRequest,
	sort_order string,
	batchSize int,
	cursor *PageCursor,
) (mainQuery string, edgeQuery string, args []any) {
	// $1 = supported action types, $2 = account address
	args = []any{req.SupportedActionTypes, string(req.AccountAddress)}
	nextArg := func(val any) string {
		args = append(args, val)
		return fmt.Sprintf("$%d", len(args))
	}

	// Utime→lt conversion CTE
	hasUtime := utime_req.StartUtime != nil || utime_req.EndUtime != nil
	var ltBoundsCTE string
	var utimeActionFilters []string

	if hasUtime {
		var ltBoundsSelects []string
		if v := utime_req.StartUtime; v != nil {
			argUt := nextArg(int64(*v))
			ltBoundsSelects = append(ltBoundsSelects, fmt.Sprintf(
				`COALESCE((SELECT start_lt - %d FROM blocks WHERE workchain = -1 AND gen_utime <= %s ORDER BY gen_utime DESC LIMIT 1), 0) AS lt_lower`,
				ltMargin, argUt))
			utimeActionFilters = append(utimeActionFilters, fmt.Sprintf("A.trace_end_utime >= %s", argUt))
		}
		if v := utime_req.EndUtime; v != nil {
			argUt := nextArg(int64(*v))
			ltBoundsSelects = append(ltBoundsSelects, fmt.Sprintf(
				`COALESCE((SELECT end_lt + %d FROM blocks WHERE workchain = -1 AND gen_utime >= %s ORDER BY gen_utime ASC LIMIT 1), 9223372036854775807) AS lt_upper`,
				ltMargin, argUt))
			utimeActionFilters = append(utimeActionFilters, fmt.Sprintf("A.trace_end_utime < %s", argUt))
		}
		ltBoundsCTE = "WITH lt_bounds AS (SELECT " + strings.Join(ltBoundsSelects, ", ") + ") "
	}

	// LATERAL filters
	cmpOp := cursorCompareOp(sort_order)
	lateralFilters := []string{
		"aa.account = $2::tonaddr",
		"aa.role = roles.role",
	}
	if cursor != nil {
		argLt := nextArg(cursor.TraceEndLt)
		argTid := nextArg(cursor.TraceId)
		lateralFilters = append(lateralFilters,
			fmt.Sprintf("(aa.trace_end_lt, aa.trace_id) %s (%s, %s)", cmpOp, argLt, argTid))
	} else if v := lt_req.EndLt; v != nil {
		lateralFilters = append(lateralFilters, "aa.trace_end_lt "+cmpOp+" "+nextArg(int64(*v)))
	}
	if v := lt_req.StartLt; v != nil {
		lateralFilters = append(lateralFilters, "aa.trace_end_lt >= "+nextArg(int64(*v)))
	}
	if hasUtime {
		if utime_req.StartUtime != nil {
			lateralFilters = append(lateralFilters, "aa.trace_end_lt >= (SELECT lt_lower FROM lt_bounds)")
		}
		if utime_req.EndUtime != nil {
			lateralFilters = append(lateralFilters, "aa.trace_end_lt <= (SELECT lt_upper FROM lt_bounds)")
		}
	}

	lateralWhere := strings.Join(lateralFilters, " AND ")
	values := roleToValues(req.Role)
	roleIn := roleInClause(req.Role)

	// Action filters
	actionFilters := []string{
		"A.end_lt IS NOT NULL",
		"A.type = ANY($1)",
		"NOT(A.ancestor_type && $1::varchar[])",
	}
	actionFilters = append(actionFilters, utimeActionFilters...)
	actionWhere := strings.Join(actionFilters, " AND ")

	// The LATERAL subquery (shared between main and edge queries)
	lateralSQL := fmt.Sprintf(
		`%s AS roles(role)
		CROSS JOIN LATERAL (
			SELECT aa.trace_id, aa.action_id, aa.trace_end_lt
			FROM action_accounts aa
			WHERE %s
			ORDER BY aa.trace_end_lt %s, aa.trace_id %s
			LIMIT %d
		) aa`,
		values,
		lateralWhere,
		sort_order, sort_order,
		batchSize)

	// Main query: visible actions with deduplication
	mainQuery = fmt.Sprintf(
		`%sSELECT DISTINCT ON (aa.trace_end_lt, aa.trace_id, A.action_id) %s
		FROM %s
		JOIN action_accounts AA2
			ON AA2.trace_id = aa.trace_id
			AND AA2.account = $2::tonaddr
			AND AA2.role IN (%s)
		JOIN actions A
			ON A.trace_id = AA2.trace_id
			AND A.action_id = AA2.action_id
		WHERE %s
		ORDER BY aa.trace_end_lt %s, aa.trace_id %s, A.action_id, A.end_lt %s`,
		ltBoundsCTE,
		actionsColumnQuery,
		lateralSQL,
		roleIn,
		actionWhere,
		sort_order, sort_order, sort_order,
	)

	// Edge query: just the last scanned position from the LATERAL.
	// Used to advance cursor when main query returns 0 visible actions.
	edgeQuery = fmt.Sprintf(
		`%sSELECT aa.trace_end_lt, aa.trace_id
		FROM %s
		ORDER BY aa.trace_end_lt %s, aa.trace_id %s
		LIMIT 1`,
		ltBoundsCTE,
		lateralSQL,
		// For DESC, last scanned = smallest lt → ASC limit 1
		// For ASC, last scanned = largest lt → DESC limit 1
		flipOrder(sort_order), flipOrder(sort_order),
	)

	return mainQuery, edgeQuery, args
}

func flipOrder(order string) string {
	if order == "asc" {
		return "desc"
	}
	return "asc"
}

func (db *DbClient) QueryAccountActions(
	req models.AccountActionsRequest,
	utime_req models.UtimeRequest,
	lt_req models.LtRequest,
	lim_req models.LimitRequest,
	settings models.RequestSettings,
) ([]models.Action, models.AddressBook, models.Metadata, string, error) {
	if len(req.SupportedActionTypes) == 0 {
		req.SupportedActionTypes = []string{"latest"}
	}
	req.SupportedActionTypes = models.ExpandActionTypeShortcuts(req.SupportedActionTypes)

	sort_order := "desc"
	var err error
	if v := lim_req.Sort; v != nil {
		sort_order, err = getSortOrder(*v)
		if err != nil {
			return nil, nil, nil, "", err
		}
	}

	limit := int(settings.DefaultLimit)
	if lim_req.Limit != nil {
		limit = max(1, int(*lim_req.Limit))
		if limit > settings.MaxLimit {
			return nil, nil, nil, "", models.IndexError{Code: 422,
				Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// Single context covers the entire operation including all loop iterations.
	ctx, cancel := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel()

	// Decode cursor from request. Falls back to end_lt if no cursor provided.
	var cursor *PageCursor
	if req.Cursor != nil {
		cursor, err = DecodePageCursor(*req.Cursor)
		if err != nil {
			return nil, nil, nil, "", models.IndexError{Code: 422, Message: "invalid cursor"}
		}
	}

	// Collect actions grouped by trace until we have `limit` traces.
	traceActions := make(map[string][]models.RawAction)
	traceOrder := []string{}
	batchSize := limit * 5
	maxIterations := 10

	for i := 0; i < maxIterations && len(traceOrder) < limit; i++ {
		if ctx.Err() != nil {
			break
		}

		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}

		mainQuery, edgeQuery, args := buildAccountActionsQueries(
			req, utime_req, lt_req, sort_order, batchSize, cursor)

		if settings.DebugRequest && i == 0 {
			log.Println("Debug account actions query:", mainQuery)
			log.Println("Debug account actions args:", args)
		}

		raw_actions, err := queryRawActionsCtx(ctx, mainQuery, args, conn)
		if err != nil {
			return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
		}

		if len(raw_actions) == 0 {
			// No visible actions in this batch. But the LATERAL may have scanned rows
			// that were all filtered out. Use the edge query to find the last scanned
			// position and advance the cursor past it.
			edgeCursor, err := queryEdgeCursor(ctx, edgeQuery, args, conn)
			if err != nil || edgeCursor == nil {
				break // truly no more data
			}
			cursor = edgeCursor
			batchSize = min(batchSize*2, maxBatchSize)
			continue
		}

		for idx := range raw_actions {
			ra := &raw_actions[idx]
			tid := ""
			if ra.TraceId != nil {
				tid = string(*ra.TraceId)
			}
			if _, seen := traceActions[tid]; !seen {
				if len(traceOrder) >= limit {
					continue
				}
				traceOrder = append(traceOrder, tid)
			}
			traceActions[tid] = append(traceActions[tid], *ra)
		}

		// Advance cursor to the last row in the batch.
		last := raw_actions[len(raw_actions)-1]
		lastTid := ""
		if last.TraceId != nil {
			lastTid = string(*last.TraceId)
		}
		cursor = &PageCursor{TraceEndLt: last.TraceEndLt, TraceId: lastTid}

		batchSize = min(batchSize*2, maxBatchSize)
	}

	// Build next-page cursor from the last trace included.
	// All actions in a trace share the same trace_end_lt, so any action works.
	var nextCursor string
	if len(traceOrder) >= limit {
		lastTid := traceOrder[len(traceOrder)-1]
		if acts, ok := traceActions[lastTid]; ok && len(acts) > 0 {
			nextCursor = EncodePageCursor(&PageCursor{
				TraceEndLt: acts[0].TraceEndLt,
				TraceId:    lastTid,
			})
		}
	}

	// Flatten into ordered action list.
	actions := []models.Action{}
	addr_map := map[string]bool{}
	for _, tid := range traceOrder {
		for idx := range traceActions[tid] {
			parse.CollectAddressesFromAction(&addr_map, &traceActions[tid][idx])
			action, err := parse.ParseRawAction(&traceActions[tid][idx])
			if err != nil {
				return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
			}
			actions = append(actions, *action)
		}
	}

	if req.IncludeAccounts != nil && *req.IncludeAccounts {
		actions, err = queryActionsAccountsImpl(actions, conn)
		if err != nil {
			return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	if req.IncludeTransactions != nil && *req.IncludeTransactions {
		actions, err = queryActionsTransactionsImpl(actions, conn, settings)
		if err != nil {
			return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}
	if len(addr_map) > 0 {
		addr_list := make([]string, 0, len(addr_map))
		for k := range addr_map {
			addr_list = append(addr_list, k)
		}
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, "", models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return actions, book, metadata, nextCursor, nil
}

// queryRawActionsCtx runs a raw action query with the given context.
func queryRawActionsCtx(ctx context.Context, query string, args []any, conn *pgxpool.Conn) ([]models.RawAction, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []models.RawAction{}
	for rows.Next() {
		if loc, err := parse.ScanRawAction(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, err
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return res, nil
}

// queryEdgeCursor runs the edge query to find the last LATERAL scan position.
// Returns nil if no rows were scanned (truly no more data).
func queryEdgeCursor(ctx context.Context, query string, args []any, conn *pgxpool.Conn) (*PageCursor, error) {
	row := conn.QueryRow(ctx, query, args...)
	var lt int64
	var tid string
	if err := row.Scan(&lt, &tid); err != nil {
		return nil, err
	}
	return &PageCursor{TraceEndLt: lt, TraceId: tid}, nil
}
