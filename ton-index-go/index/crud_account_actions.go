package index

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
)

func roleToIds(role *RoleType) []int {
	if role == nil {
		return []int{1, 2, 3}
	}
	switch *role {
	case RoleSender:
		return []int{1, 3}
	case RoleReceiver:
		return []int{2, 3}
	default:
		return []int{1, 2, 3}
	}
}
func roleToValues(role *RoleType) string {
	roleIds := roleToIds(role)
	var roles []string = make([]string, 0)
	for _, id := range roleIds {
		roles = append(roles, "("+strconv.Itoa(id)+")")
	}
	return "(VALUES " + strings.Join(roles, ", ") + ")"
}

func (db *DbClient) QueryAccountActions(
	req AccountActionsRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]TraceActions, AddressBook, Metadata, error) {
	if len(req.SupportedActionTypes) == 0 {
		req.SupportedActionTypes = []string{"latest"}
	}
	req.SupportedActionTypes = ExpandActionTypeShortcuts(req.SupportedActionTypes)

	// Step 1: get trace_ids from trace_accounts
	limit_str, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, nil, nil, err
	}

	sort_order := "desc"
	if v := lim_req.Sort; v != nil {
		sort_order, err = getSortOrder(*v)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	values := roleToValues(req.Role)

	lateral_filters := []string{
		fmt.Sprintf("account = '%s'::tonaddr", req.AccountAddress),
		"role = roles.role",
	}
	if v := lt_req.EndLt; v != nil {
		lateral_filters = append(lateral_filters, fmt.Sprintf("trace_end_lt < %d", *v))
	}
	if v := lt_req.StartLt; v != nil {
		lateral_filters = append(lateral_filters, fmt.Sprintf("trace_end_lt >= %d", *v))
	}
	if v := utime_req.EndUtime; v != nil {
		lateral_filters = append(lateral_filters, fmt.Sprintf("trace_end_utime <= %d", *v))
	}
	if v := utime_req.StartUtime; v != nil {
		lateral_filters = append(lateral_filters, fmt.Sprintf("trace_end_utime >= %d", *v))
	}

	lateralWhere := strings.Join(lateral_filters, " AND ")

	traceQuery := fmt.Sprintf(
		`SELECT DISTINCT ON (trace_end_lt, trace_id) t.trace_id, t.trace_end_lt
		FROM %s AS roles(role)
		CROSS JOIN LATERAL (
			SELECT trace_id, trace_end_lt FROM trace_accounts
			WHERE %s
			ORDER BY trace_end_lt %s
			%s
		) t
		ORDER BY trace_end_lt %s, trace_id
		%s`,
		values, lateralWhere, sort_order, limit_str, sort_order, limit_str,
	)

	if settings.DebugRequest {
		log.Println("Debug trace query:", traceQuery)
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel()

	rows, err := conn.Query(ctx, traceQuery)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	type traceInfo struct {
		traceId    string
		traceEndLt int64
	}
	var traceInfos []traceInfo
	for rows.Next() {
		var ti traceInfo
		if err := rows.Scan(&ti.traceId, &ti.traceEndLt); err != nil {
			rows.Close()
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		traceInfos = append(traceInfos, ti)
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	if len(traceInfos) == 0 {
		return []TraceActions{}, AddressBook{}, Metadata{}, nil
	}

	// Step 2: get actions for those trace_ids, filtered by role via action_accounts
	traceIdValues := make([]string, len(traceInfos))
	for i, ti := range traceInfos {
		traceIdValues[i] = fmt.Sprintf("'%s'", ti.traceId)
	}
	traceIdIn := strings.Join(traceIdValues, ", ")

	roleIds := roleToIds(req.Role)
	roleStrs := make([]string, len(roleIds))
	for i, id := range roleIds {
		roleStrs[i] = strconv.Itoa(id)
	}
	roleIn := strings.Join(roleStrs, ", ")

	from_query := `action_accounts as AA join actions as A on A.trace_id = AA.trace_id and A.action_id = AA.action_id`

	filter_list := []string{
		fmt.Sprintf("AA.trace_id IN (%s)", traceIdIn),
		fmt.Sprintf("AA.account = '%s'::tonaddr", req.AccountAddress),
		fmt.Sprintf("AA.role in (%s)", roleIn),
		"A.end_lt is not NULL",
		"NOT(A.ancestor_type && $1::varchar[])",
	}

	if v := req.IncludeActionTypes; len(v) > 0 {
		filter_str := filterByArray("A.type", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	} else {
		filter_list = append(filter_list, "A.type = ANY($1)")
	}

	if v := req.ExcludeActionTypes; len(v) > 0 {
		filter_str := filterByArray("A.type", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, fmt.Sprintf("not (%s)", filter_str))
		}
	}

	clmn_query := fmt.Sprintf("distinct on (AA.trace_end_lt, AA.trace_id, AA.action_end_lt, AA.action_id) %s", actionsColumnQuery)
	orderby_query := fmt.Sprintf(" order by AA.trace_end_lt %s, AA.trace_id %s, AA.action_end_lt %s, AA.action_id %s",
		sort_order, sort_order, sort_order, sort_order)
	filter_query := " where " + strings.Join(filter_list, " and ")

	actionsQuery := `select ` + clmn_query + ` from ` + from_query + filter_query + orderby_query

	if settings.DebugRequest {
		log.Println("Debug actions query:", actionsQuery)
	}

	var args []any
	args = append(args, req.SupportedActionTypes)

	raw_actions, err := queryRawActionsImplV2(actionsQuery, args, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// Parse actions and collect addresses
	actions := []Action{}
	addr_map := map[string]bool{}
	for idx := range raw_actions {
		CollectAddressesFromAction(&addr_map, &raw_actions[idx])
		action, err := ParseRawAction(&raw_actions[idx])
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		actions = append(actions, *action)
	}

	// Optional: include accounts
	if req.IncludeAccounts != nil && *req.IncludeAccounts {
		actions, err = queryActionsAccountsImpl(actions, conn)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}

	// Optional: include transactions
	if req.IncludeTransactions != nil && *req.IncludeTransactions {
		actions, err = queryActionsTransactionsImpl(actions, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}

	// Step 3: group actions by trace_id, preserving order from query results
	traceOrder := []string{}
	traceOrderSeen := map[string]bool{}
	actionsByTrace := map[string][]Action{}
	for _, action := range actions {
		tid := string(*action.TraceId)
		if !traceOrderSeen[tid] {
			traceOrder = append(traceOrder, tid)
			traceOrderSeen[tid] = true
		}
		actionsByTrace[tid] = append(actionsByTrace[tid], action)
	}

	traceActions := make([]TraceActions, 0, len(traceOrder))
	for _, tid := range traceOrder {
		traceActions = append(traceActions, TraceActions{
			TraceId: HashType(tid),
			Actions: actionsByTrace[tid],
		})
	}

	// Build address book + metadata
	book := AddressBook{}
	metadata := Metadata{}
	if len(addr_map) > 0 {
		addr_list := make([]string, 0, len(addr_map))
		for k := range addr_map {
			addr_list = append(addr_list, k)
		}
		if !settings.NoAddressBook {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if !settings.NoMetadata {
			metadata, err = QueryMetadataImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return traceActions, book, metadata, nil
}
