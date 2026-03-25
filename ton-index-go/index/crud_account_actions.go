package index

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
)

// roleToIds returns the set of role values matching the requested filter.
// 3-bit bitmask: ECON_OUT=1, ECON_IN=2, INITIATOR=4.
// Values: 0=observer, 1=econ_out, 2=econ_in, 3=econ_both, 4=initiator, 5=init_out, 6=(trace-only), 7=init_both
func roleToIds(role *RoleType) []int {
	if role == nil {
		// Default: all participants including observers
		return []int{0, 1, 2, 3, 4, 5, 6, 7}
	}
	switch *role {
	case RoleSender:
		// Outgoing: bit 0 set (value flowed out)
		return []int{1, 3, 5, 7}
	case RoleReceiver:
		// Incoming: bit 1 set (value flowed in)
		return []int{2, 3, 6, 7}
	case RoleInitiated:
		// Initiated by account: bit 2 set
		return []int{4, 5, 6, 7}
	case RoleObserver:
		// Mentioned but no economic role
		return []int{0}
	default:
		return []int{0, 1, 2, 3, 4, 5, 6, 7}
	}
}

func roleToValues(role *RoleType) string {
	roleIds := roleToIds(role)
	parts := make([]string, len(roleIds))
	for i, id := range roleIds {
		parts[i] = "(" + strconv.Itoa(id) + ")"
	}
	return "(VALUES " + strings.Join(parts, ", ") + ")"
}

func (db *DbClient) QueryAccountActions(
	req AccountActionsRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Action, AddressBook, Metadata, error) {
	if len(req.SupportedActionTypes) == 0 {
		req.SupportedActionTypes = []string{"latest"}
	}
	req.SupportedActionTypes = ExpandActionTypeShortcuts(req.SupportedActionTypes)

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

	// Positional args: $1 = supported action types, $2 = account address.
	args := []any{req.SupportedActionTypes, string(req.AccountAddress)}
	nextArg := func(val any) string {
		args = append(args, val)
		return fmt.Sprintf("$%d", len(args))
	}

	// Lateral filters for trace selection.
	lateralFilters := []string{
		"account = $2::tonaddr",
		"role = roles.role",
	}
	if v := lt_req.EndLt; v != nil {
		lateralFilters = append(lateralFilters, "trace_end_lt < "+nextArg(int64(*v)))
	}
	if v := lt_req.StartLt; v != nil {
		lateralFilters = append(lateralFilters, "trace_end_lt >= "+nextArg(int64(*v)))
	}
	if v := utime_req.EndUtime; v != nil {
		lateralFilters = append(lateralFilters, "trace_end_utime < "+nextArg(int64(*v)))
	}
	if v := utime_req.StartUtime; v != nil {
		lateralFilters = append(lateralFilters, "trace_end_utime >= "+nextArg(int64(*v)))
	}

	lateralWhere := strings.Join(lateralFilters, " AND ")
	values := roleToValues(req.Role)

	// Action-level filters applied after joining actions for the selected traces.
	actionFilters := []string{
		"A.end_lt IS NOT NULL",
		"NOT(A.ancestor_type && $1::varchar[])",
	}
	if v := req.IncludeActionTypes; len(v) > 0 {
		filter_str := filterByArray("A.type", v)
		if len(filter_str) > 0 {
			actionFilters = append(actionFilters, filter_str)
		}
	} else {
		actionFilters = append(actionFilters, "A.type = ANY($1)")
	}
	if v := req.ExcludeActionTypes; len(v) > 0 {
		filter_str := filterByArray("A.type", v)
		if len(filter_str) > 0 {
			actionFilters = append(actionFilters, "NOT ("+filter_str+")")
		}
	}

	actionWhere := strings.Join(actionFilters, " AND ")

	// Two-level query:
	// LATERAL on action_accounts → distinct trace_ids (LIMIT controls trace count)
	// JOIN actions for those traces, filtered by supported types + ancestor_type
	query := fmt.Sprintf(
		`WITH trace_page AS (
			SELECT DISTINCT ON (aa.trace_end_lt, aa.trace_id)
				aa.trace_end_lt, aa.trace_id
			FROM %s AS roles(role)
			CROSS JOIN LATERAL (
				SELECT DISTINCT trace_id, trace_end_lt
				FROM action_accounts
				WHERE %s
				ORDER BY trace_end_lt %s, trace_id
				%s
			) aa
			ORDER BY aa.trace_end_lt %s, aa.trace_id
			%s
		)
		SELECT %s
		FROM trace_page tp
		JOIN actions A ON A.trace_id = tp.trace_id
		WHERE %s
		ORDER BY tp.trace_end_lt %s, A.end_lt %s`,
		values,
		lateralWhere,
		sort_order,
		limit_str,
		sort_order,
		limit_str,
		actionsColumnQuery,
		actionWhere,
		sort_order, sort_order,
	)

	if settings.DebugRequest {
		log.Println("Debug account actions query:", query)
		log.Println("Debug account actions args:", args)
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	raw_actions, err := queryRawActionsImplV2(query, args, conn, settings)
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

	return actions, book, metadata, nil
}
