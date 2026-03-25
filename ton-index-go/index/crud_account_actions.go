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

	// Build positional args.
	// $1 = supported action types, $2 = account address, then dynamic.
	args := []any{req.SupportedActionTypes, string(req.AccountAddress)}
	nextArg := func(val any) string {
		args = append(args, val)
		return fmt.Sprintf("$%d", len(args))
	}

	// Lateral filters: always account and role.
	lateralFilters := []string{
		"account = $2::tonaddr",
		"role = roles.role",
	}
	if v := lt_req.EndLt; v != nil {
		if req.AfterActionId != nil {
			// Composite cursor: exact pagination using (trace_end_lt, action_id).
			argLt := nextArg(int64(*v))
			argAid := nextArg(string(*req.AfterActionId))
			lateralFilters = append(lateralFilters,
				fmt.Sprintf("(trace_end_lt, action_id) < (%s, %s)", argLt, argAid))
		} else {
			// Simple cursor: trace_end_lt only (backward compatible).
			argLt := nextArg(int64(*v))
			lateralFilters = append(lateralFilters, "trace_end_lt < "+argLt)
		}
	}
	if v := lt_req.StartLt; v != nil {
		argLt := nextArg(int64(*v))
		lateralFilters = append(lateralFilters, "trace_end_lt >= "+argLt)
	}
	if v := utime_req.EndUtime; v != nil {
		argUt := nextArg(int64(*v))
		lateralFilters = append(lateralFilters, "trace_end_utime < "+argUt)
	}
	if v := utime_req.StartUtime; v != nil {
		argUt := nextArg(int64(*v))
		lateralFilters = append(lateralFilters, "trace_end_utime >= "+argUt)
	}

	lateralWhere := strings.Join(lateralFilters, " AND ")
	values := roleToValues(req.Role)

	// Action-level filters applied after the JOIN.
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

	query := fmt.Sprintf(
		`SELECT DISTINCT ON (aa.trace_end_lt, aa.action_id) %s
		FROM %s AS roles(role)
		CROSS JOIN LATERAL (
			SELECT trace_id, action_id, trace_end_lt, action_end_lt
			FROM action_accounts
			WHERE %s
			ORDER BY trace_end_lt %s, action_id %s
			%s
		) aa
		JOIN actions A ON A.trace_id = aa.trace_id AND A.action_id = aa.action_id
		WHERE %s
		ORDER BY aa.trace_end_lt %s, aa.action_id %s
		%s`,
		actionsColumnQuery,
		values,
		lateralWhere,
		sort_order, sort_order,
		limit_str,
		actionWhere,
		sort_order, sort_order,
		limit_str,
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
