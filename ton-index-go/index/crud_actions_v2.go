package index

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func (db *DbClient) QueryActionsV2(
	act_req ActionRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Action, AddressBook, Metadata, error) {
	if len(act_req.SupportedActionTypes) == 0 {
		act_req.SupportedActionTypes = []string{"v1"}
	}
	act_req.SupportedActionTypes = ExpandActionTypeShortcuts(act_req.SupportedActionTypes)
	query, args, err := buildActionsQueryV2(act_req, utime_req, lt_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// check block
	if seqno := act_req.McSeqno; seqno != nil {
		exists, err := queryBlockExists(*seqno, conn, settings)
		if err != nil {
			return nil, nil, nil, err
		}
		if !exists {
			return nil, nil, nil, IndexError{Code: 404, Message: fmt.Sprintf("masterchain block %d not found", *seqno)}
		}
	}

	raw_actions, err := queryRawActionsImplV2(query, args, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	actions := []Action{}
	book := AddressBook{}
	metadata := Metadata{}
	addr_map := map[string]bool{}
	for idx := range raw_actions {
		CollectAddressesFromAction(&addr_map, &raw_actions[idx])
		action, err := ParseRawAction(&raw_actions[idx])
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		actions = append(actions, *action)
	}
	if len(addr_map) > 0 && !settings.NoAddressBook {
		addr_list := []string{}
		for k := range addr_map {
			addr_list = append(addr_list, string(k))
		}
		book, err = QueryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}

		metadata, err = QueryMetadataImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return actions, book, metadata, nil
}

func buildActionsQueryV2(act_req ActionRequest, utime_req UtimeRequest, lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, []any, error) {
	clmn_query_default := `A.trace_id, A.action_id, A.start_lt, A.end_lt, A.start_utime, A.end_utime, 
		A.trace_end_lt, A.trace_end_utime, A.trace_mc_seqno_end, A.source, A.source_secondary,
		A.destination, A.destination_secondary, A.asset, A.asset_secondary, A.asset2, A.asset2_secondary, A.opcode, A.tx_hashes,
		A.type, (A.ton_transfer_data).content, (A.ton_transfer_data).encrypted, A.value, A.amount,
		(A.jetton_transfer_data).response_destination, (A.jetton_transfer_data).forward_amount, (A.jetton_transfer_data).query_id,
		(A.jetton_transfer_data).custom_payload, (A.jetton_transfer_data).forward_payload, (A.jetton_transfer_data).comment,
		(A.jetton_transfer_data).is_encrypted_comment, (A.nft_transfer_data).is_purchase, (A.nft_transfer_data).price,
		(A.nft_transfer_data).query_id, (A.nft_transfer_data).custom_payload, (A.nft_transfer_data).forward_payload,
		(A.nft_transfer_data).forward_amount, (A.nft_transfer_data).response_destination, (A.nft_transfer_data).nft_item_index,
		(A.jetton_swap_data).dex, (A.jetton_swap_data).sender, ((A.jetton_swap_data).dex_incoming_transfer).amount,
		((A.jetton_swap_data).dex_incoming_transfer).asset, ((A.jetton_swap_data).dex_incoming_transfer).source,
		((A.jetton_swap_data).dex_incoming_transfer).destination, ((A.jetton_swap_data).dex_incoming_transfer).source_jetton_wallet,
		((A.jetton_swap_data).dex_incoming_transfer).destination_jetton_wallet, ((A.jetton_swap_data).dex_outgoing_transfer).amount,
		((A.jetton_swap_data).dex_outgoing_transfer).asset, ((A.jetton_swap_data).dex_outgoing_transfer).source,
		((A.jetton_swap_data).dex_outgoing_transfer).destination, ((A.jetton_swap_data).dex_outgoing_transfer).source_jetton_wallet,
		((A.jetton_swap_data).dex_outgoing_transfer).destination_jetton_wallet, (A.jetton_swap_data).peer_swaps,
		(A.change_dns_record_data).key, (A.change_dns_record_data).value_schema, (A.change_dns_record_data).value,
		(A.change_dns_record_data).flags, (A.nft_mint_data).nft_item_index,
		(A.dex_withdraw_liquidity_data).dex,
		(A.dex_withdraw_liquidity_data).amount1,
		(A.dex_withdraw_liquidity_data).amount2,
		(A.dex_withdraw_liquidity_data).asset1_out,
		(A.dex_withdraw_liquidity_data).asset2_out,
		(A.dex_withdraw_liquidity_data).user_jetton_wallet_1,
		(A.dex_withdraw_liquidity_data).user_jetton_wallet_2,
		(A.dex_withdraw_liquidity_data).dex_jetton_wallet_1,
		(A.dex_withdraw_liquidity_data).dex_jetton_wallet_2,
		(A.dex_withdraw_liquidity_data).lp_tokens_burnt,
		(A.dex_deposit_liquidity_data).dex,
		(A.dex_deposit_liquidity_data).amount1,
		(A.dex_deposit_liquidity_data).amount2,
		(A.dex_deposit_liquidity_data).asset1,
		(A.dex_deposit_liquidity_data).asset2,
		(A.dex_deposit_liquidity_data).user_jetton_wallet_1,
		(A.dex_deposit_liquidity_data).user_jetton_wallet_2,
		(A.dex_deposit_liquidity_data).lp_tokens_minted,
		(A.staking_data).provider,
		(A.staking_data).ts_nft,
		(A.staking_data).tokens_burnt,
		(A.staking_data).tokens_minted,
		A.success,
		A.trace_external_hash,
		A.value_extra_currencies,
		(A.multisig_create_order_data).query_id,
		(A.multisig_create_order_data).order_seqno,
		(A.multisig_create_order_data).is_created_by_signer,
		(A.multisig_create_order_data).is_signed_by_creator,
		(A.multisig_create_order_data).creator_index,
		(A.multisig_create_order_data).expiration_date,
		(A.multisig_create_order_data).order_boc,
		(A.multisig_approve_data).signer_index,
		(A.multisig_approve_data).exit_code,
		(A.multisig_execute_data).query_id,
		(A.multisig_execute_data).order_seqno,
		(A.multisig_execute_data).expiration_date,
		(A.multisig_execute_data).approvals_num,
		(A.multisig_execute_data).signers_hash,
		(A.multisig_execute_data).order_boc,
		(A.vesting_send_message_data).query_id,
		(A.vesting_send_message_data).message_boc,
		(A.vesting_add_whitelist_data).query_id,
		(A.vesting_add_whitelist_data).accounts_added,
		(A.evaa_supply_data).sender_jetton_wallet,
		(A.evaa_supply_data).recipient_jetton_wallet,
		(A.evaa_supply_data).master_jetton_wallet,
		(A.evaa_supply_data).master,
		(A.evaa_supply_data).asset_id,
		(A.evaa_supply_data).is_ton,
		(A.evaa_withdraw_data).recipient_jetton_wallet,
		(A.evaa_withdraw_data).master_jetton_wallet,
		(A.evaa_withdraw_data).master,
		(A.evaa_withdraw_data).fail_reason,
		(A.evaa_withdraw_data).asset_id,
		(A.evaa_liquidate_data).fail_reason,
		(A.evaa_liquidate_data).debt_amount,
		(A.evaa_liquidate_data).asset_id,
		(A.jvault_claim_data).claimed_jettons,
		(A.jvault_claim_data).claimed_amounts,
		(A.jvault_stake_data).period,
		(A.jvault_stake_data).minted_stake_jettons,
		(A.jvault_stake_data).stake_wallet`
	clmn_query := clmn_query_default
	from_query := `actions as A`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", nil, err
	}

	sort_order := "desc"
	if v := lim_req.Sort; v != nil {
		sort_order, err = getSortOrder(*v)
		if err != nil {
			return "", nil, err
		}
	}
	// time
	order_by_now := false
	join_accounts := false
	if v := act_req.AccountAddress; v != nil && len(*v) > 0 {
		join_accounts = true
	}
	if v := utime_req.StartUtime; v != nil {
		field := "A.trace_end_utime"
		if join_accounts {
			field = "AA.trace_end_utime"
		}
		filter_list = append(filter_list, fmt.Sprintf("%s >= %d", field, *v))
		order_by_now = true
	}
	if v := utime_req.EndUtime; v != nil {
		field := "A.trace_end_utime"
		if join_accounts {
			field = "AA.trace_end_utime"
		}
		filter_list = append(filter_list, fmt.Sprintf("%s <= %d", field, *v))
		order_by_now = true
	}
	if v := lt_req.StartLt; v != nil {
		field := "A.trace_end_lt"
		if join_accounts {
			field = "AA.trace_end_lt"
		}
		filter_list = append(filter_list, fmt.Sprintf("%s >= %d", field, *v))
	}
	if v := lt_req.EndLt; v != nil {
		field := "A.trace_end_lt"
		if join_accounts {
			field = "AA.trace_end_lt"
		}
		filter_list = append(filter_list, fmt.Sprintf("%s <= %d", field, *v))
	}
	if v := act_req.AccountAddress; v != nil && len(*v) > 0 {
		filter_str := fmt.Sprintf("AA.account = '%s'::tonaddr", *v)
		filter_list = append(filter_list, filter_str)

		from_query = `action_accounts as AA join actions as A on A.trace_id = AA.trace_id and A.action_id = AA.action_id`
		if order_by_now {
			clmn_query = `distinct on (AA.trace_end_utime, AA.trace_id, AA.action_end_utime, AA.action_id) ` + clmn_query_default
		} else {
			clmn_query = `distinct on (AA.trace_end_lt, AA.trace_id, AA.action_end_lt, AA.action_id) ` + clmn_query_default
		}
	}
	if v := act_req.TransactionHash; v != nil {
		filter_str := filterByArray("T.hash", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		from_query = `actions as A join transactions as T on A.trace_id = T.trace_id and A.tx_hashes @> array[T.hash::tonhash]`
		if order_by_now {
			clmn_query = `distinct on (A.trace_end_utime, A.trace_id, A.end_utime, A.action_id) ` + clmn_query_default
		} else {
			clmn_query = `distinct on (A.trace_end_lt, A.trace_id, A.end_lt, A.action_id) ` + clmn_query_default
		}
	}
	if v := act_req.MessageHash; len(v) > 0 {
		filter_str := fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v))
		filter_list = append(filter_list, filter_str)

		from_query = `actions as A join messages as M on A.trace_id = M.trace_id and array[M.tx_hash::tonhash] @> A.tx_hashes`
		if order_by_now {
			clmn_query = `distinct on (A.trace_end_utime, A.trace_id, A.end_utime, A.action_id) ` + clmn_query_default
		} else {
			clmn_query = `distinct on (A.trace_end_lt, A.trace_id, A.end_lt, A.action_id) ` + clmn_query_default
		}
	}
	if v := act_req.IncludeActionTypes; len(v) > 0 {
		filter_str := filterByArray("A.type", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := act_req.ExcludeActionTypes; len(v) > 0 {
		filter_str := filterByArray("A.type", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, fmt.Sprintf("not (%s)", filter_str))
		}
	}
	if v := act_req.McSeqno; v != nil {
		filter_list = append(filter_list, `E.state = 'complete'`)
		filter_list = append(filter_list, fmt.Sprintf("E.mc_seqno_end = %d", *v))
		from_query = `actions as A join traces as E on A.trace_id = E.trace_id`
		clmn_query = clmn_query_default
	}
	if v := act_req.ActionId; v != nil {
		from_query = `actions as A`
		filter_str := filterByArray("A.action_id", v)
		if len(filter_str) > 0 {
			filter_list = []string{filter_str}
		}
		clmn_query = clmn_query_default
	}
	if v := act_req.TraceId; v != nil {
		from_query = `actions as A`
		filter_str := filterByArray("A.trace_id", v)
		if len(filter_str) > 0 {
			filter_list = []string{filter_str}
		}
	}
	if strings.Contains(from_query, "action_accounts") {
		if order_by_now {
			orderby_query = fmt.Sprintf(" order by AA.trace_end_utime %s, AA.trace_id %s, AA.action_end_utime %s, AA.action_id %s",
				sort_order, sort_order, sort_order, sort_order)
		} else {
			orderby_query = fmt.Sprintf(" order by AA.trace_end_lt %s, AA.trace_id %s, AA.action_end_lt %s, AA.action_id %s",
				sort_order, sort_order, sort_order, sort_order)
		}
	} else {
		if order_by_now {
			orderby_query = fmt.Sprintf(" order by A.trace_end_utime %s, A.trace_id %s, A.end_utime %s, A.action_id %s",
				sort_order, sort_order, sort_order, sort_order)
		} else {
			orderby_query = fmt.Sprintf(" order by A.trace_end_lt %s, A.trace_id %s, A.end_lt %s, A.action_id %s",
				sort_order, sort_order, sort_order, sort_order)
		}
	}
	filter_list = append(filter_list, "A.end_lt is not NULL")
	if v := act_req.IncludeActionTypes; len(v) == 0 {
		filter_list = append(filter_list, "A.type = ANY($1)")
	}
	filter_list = append(filter_list, "NOT(A.ancestor_type && $1::varchar[])")

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	var args []any
	args = append(args, act_req.SupportedActionTypes)

	//log.Println(query)
	return query, args, nil
}

func queryRawActionsImplV2(query string, args []any, conn *pgxpool.Conn, settings RequestSettings) ([]RawAction, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []RawAction{}
	for rows.Next() {
		if loc, err := ScanRawAction(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}
