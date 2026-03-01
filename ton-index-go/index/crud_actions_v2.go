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
		act_req.SupportedActionTypes = []string{"latest"}
	}
	act_req.SupportedActionTypes = ExpandActionTypeShortcuts(act_req.SupportedActionTypes)
	query, args, err := buildActionsQueryV2(act_req, utime_req, lt_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query, "Args:", args)
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
	if act_req.IncludeAccounts != nil && *act_req.IncludeAccounts {
		actions, err = queryActionsAccountsImpl(actions, conn)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if act_req.IncludeTransactions != nil && *act_req.IncludeTransactions {
		actions, err = queryActionsTransactionsImpl(actions, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if len(addr_map) > 0 {
		addr_list := []string{}
		for k := range addr_map {
			addr_list = append(addr_list, string(k))
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

func buildActionsQueryV2(act_req ActionRequest, utime_req UtimeRequest, lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, []any, error) {
	clmn_query_default := `A.trace_id, A.action_id, A.start_lt, A.end_lt, A.start_utime, A.end_utime, 
		A.trace_end_lt, A.trace_end_utime, A.trace_mc_seqno_end, A.source, A.source_secondary,
		A.destination, A.destination_secondary, A.asset, A.asset_secondary, A.asset2, A.asset2_secondary, A.opcode, A.tx_hashes,
		A.type, (A.ton_transfer_data).content, (A.ton_transfer_data).encrypted, A.value, A.amount,
		(A.jetton_transfer_data).response_destination, (A.jetton_transfer_data).forward_amount, (A.jetton_transfer_data).query_id,
		(A.jetton_transfer_data).custom_payload, (A.jetton_transfer_data).forward_payload, (A.jetton_transfer_data).comment,
		(A.jetton_transfer_data).is_encrypted_comment, (A.nft_transfer_data).is_purchase, (A.nft_transfer_data).price,
		(A.nft_transfer_data).query_id, (A.nft_transfer_data).custom_payload, (A.nft_transfer_data).forward_payload,
		(A.nft_transfer_data).forward_amount, (A.nft_transfer_data).response_destination, (A.nft_transfer_data).nft_item_index, (A.nft_transfer_data).marketplace, (A.nft_transfer_data).real_prev_owner,
		(A.nft_transfer_data).marketplace_address,
		(A.nft_transfer_data).payout_amount,
		(A.nft_transfer_data).payout_comment_encrypted,
		(A.nft_transfer_data).payout_comment_encoded,
		(A.nft_transfer_data).payout_comment,
		(A.nft_transfer_data).royalty_amount,
		(A.nft_listing_data).nft_item_index,
		(A.nft_listing_data).full_price,
		(A.nft_listing_data).marketplace_fee,
		(A.nft_listing_data).royalty_amount,
		(A.nft_listing_data).mp_fee_factor,
		(A.nft_listing_data).mp_fee_base,
		(A.nft_listing_data).royalty_fee_base,
		(A.nft_listing_data).max_bid,
		(A.nft_listing_data).min_bid,
		(A.nft_listing_data).marketplace_fee_address,
		(A.nft_listing_data).royalty_address,
		(A.nft_listing_data).marketplace,
		(A.jetton_swap_data).dex, (A.jetton_swap_data).sender, ((A.jetton_swap_data).dex_incoming_transfer).amount,
		((A.jetton_swap_data).dex_incoming_transfer).asset, ((A.jetton_swap_data).dex_incoming_transfer).source,
		((A.jetton_swap_data).dex_incoming_transfer).destination, ((A.jetton_swap_data).dex_incoming_transfer).source_jetton_wallet,
		((A.jetton_swap_data).dex_incoming_transfer).destination_jetton_wallet, ((A.jetton_swap_data).dex_outgoing_transfer).amount,
		((A.jetton_swap_data).dex_outgoing_transfer).asset, ((A.jetton_swap_data).dex_outgoing_transfer).source,
		((A.jetton_swap_data).dex_outgoing_transfer).destination, ((A.jetton_swap_data).dex_outgoing_transfer).source_jetton_wallet,
		((A.jetton_swap_data).dex_outgoing_transfer).destination_jetton_wallet, (A.jetton_swap_data).peer_swaps,
		(A.jetton_swap_data).min_out_amount,
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
		(A.dex_withdraw_liquidity_data).burned_nft_index,
		(A.dex_withdraw_liquidity_data).burned_nft_address,
		(A.dex_withdraw_liquidity_data).tick_lower,
		(A.dex_withdraw_liquidity_data).tick_upper,
		(A.dex_deposit_liquidity_data).dex,
		(A.dex_deposit_liquidity_data).amount1,
		(A.dex_deposit_liquidity_data).amount2,
		(A.dex_deposit_liquidity_data).asset1,
		(A.dex_deposit_liquidity_data).asset2,
		(A.dex_deposit_liquidity_data).user_jetton_wallet_1,
		(A.dex_deposit_liquidity_data).user_jetton_wallet_2,
		(A.dex_deposit_liquidity_data).lp_tokens_minted,
		(A.dex_deposit_liquidity_data).target_asset_1,
		(A.dex_deposit_liquidity_data).target_asset_2,
		(A.dex_deposit_liquidity_data).target_amount_1,
		(A.dex_deposit_liquidity_data).target_amount_2,
		(A.dex_deposit_liquidity_data).vault_excesses,
		(A.dex_deposit_liquidity_data).tick_lower,
		(A.dex_deposit_liquidity_data).tick_upper,
		(A.dex_deposit_liquidity_data).nft_index,
		(A.dex_deposit_liquidity_data).nft_address,
		(A.staking_data).provider,
		(A.staking_data).ts_nft,
		(A.staking_data).tokens_burnt,
		(A.staking_data).tokens_minted,
		A.success,
		2 as finality,
		A.trace_external_hash,
		A.trace_external_hash_norm,
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
		(A.jvault_stake_data).stake_wallet,
		(A.tonco_deploy_pool_data).jetton0_router_wallet,
		(A.tonco_deploy_pool_data).jetton1_router_wallet,
		(A.tonco_deploy_pool_data).jetton0_minter,
		(A.tonco_deploy_pool_data).jetton1_minter,
		(A.tonco_deploy_pool_data).tick_spacing,
		(A.tonco_deploy_pool_data).initial_price_x96,
		(A.tonco_deploy_pool_data).protocol_fee,
		(A.tonco_deploy_pool_data).lp_fee_base,
		(A.tonco_deploy_pool_data).lp_fee_current,
		(A.tonco_deploy_pool_data).pool_active,
		(A.coffee_create_pool_data).amount_1,
		(A.coffee_create_pool_data).amount_2,
		(A.coffee_create_pool_data).initiator_1,
		(A.coffee_create_pool_data).initiator_2,
		(A.coffee_create_pool_data).provided_asset,
		(A.coffee_create_pool_data).lp_tokens_minted,
		(A.coffee_create_pool_data).pool_creator_contract,
		(A.coffee_staking_deposit_data).minted_item_address,
		(A.coffee_staking_deposit_data).minted_item_index,
		(A.coffee_staking_withdraw_data).nft_address,
		(A.coffee_staking_withdraw_data).nft_index,
		(A.coffee_staking_withdraw_data).points,
		(A.layerzero_send_data).send_request_id,
		(A.layerzero_send_data).msglib_manager,
		(A.layerzero_send_data).msglib,
		(A.layerzero_send_data).uln,
		(A.layerzero_send_data).native_fee,
		(A.layerzero_send_data).zro_fee,
		(A.layerzero_send_data).endpoint,
		(A.layerzero_send_data).channel,
		(A.layerzero_packet_data).src_oapp,
		(A.layerzero_packet_data).dst_oapp,
		(A.layerzero_packet_data).src_eid,
		(A.layerzero_packet_data).dst_eid,
		(A.layerzero_packet_data).nonce,
		(A.layerzero_packet_data).guid,
		(A.layerzero_packet_data).message,
		(A.layerzero_dvn_verify_data).nonce,
		(A.layerzero_dvn_verify_data).status,
		(A.layerzero_dvn_verify_data).dvn,
		(A.layerzero_dvn_verify_data).proxy,
		(A.layerzero_dvn_verify_data).uln,
		(A.layerzero_dvn_verify_data).uln_connection,
		(A.cocoon_worker_payout_data).payout_type,
		(A.cocoon_worker_payout_data).query_id,
		(A.cocoon_worker_payout_data).new_tokens,
		(A.cocoon_worker_payout_data).worker_state,
		(A.cocoon_worker_payout_data).worker_tokens,
		(A.cocoon_proxy_payout_data).query_id,
		(A.cocoon_proxy_charge_data).query_id,
		(A.cocoon_proxy_charge_data).new_tokens_used,
		(A.cocoon_proxy_charge_data).expected_address,
		(A.cocoon_client_top_up_data).query_id,
		(A.cocoon_register_proxy_data).query_id,
		(A.cocoon_unregister_proxy_data).query_id,
		(A.cocoon_unregister_proxy_data).seqno,
		(A.cocoon_client_register_data).query_id,
		(A.cocoon_client_register_data).nonce,
		(A.cocoon_client_change_secret_hash_data).query_id,
		(A.cocoon_client_change_secret_hash_data).new_secret_hash,
		(A.cocoon_client_request_refund_data).query_id,
		(A.cocoon_client_request_refund_data).via_wallet,
		(A.cocoon_grant_refund_data).query_id,
		(A.cocoon_grant_refund_data).new_tokens_used,
		(A.cocoon_grant_refund_data).expected_address,
		(A.cocoon_client_increase_stake_data).query_id,
		(A.cocoon_client_increase_stake_data).new_stake,
		(A.cocoon_client_withdraw_data).query_id,
		(A.cocoon_client_withdraw_data).withdraw_amount,
		A.ancestor_type,
		ARRAY[]::text[]`
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

		from_query = `actions as A join messages as M on A.trace_id = M.trace_id and array[M.tx_hash::tonhash] <@ A.tx_hashes`
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
		filter_list = append(filter_list, fmt.Sprintf("A.trace_mc_seqno_end = %d", *v))
		from_query = `actions as A`
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
		prefix := "A"
		if join_accounts {
			prefix = "AA"
		}
		filter_str := filterByArray(prefix+".trace_id", v)
		if len(filter_str) > 0 {
			if join_accounts {
				filter_list = append(filter_list, filter_str)
			} else {
				from_query = `actions as A`
				filter_list = []string{filter_str}
			}
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

func queryActionsTransactionsImpl(actions []Action, conn *pgxpool.Conn, settings RequestSettings) ([]Action, error) {
	if len(actions) == 0 {
		return actions, nil
	}

	// Collect all unique transaction hashes from all actions
	txHashSet := make(map[HashType]bool)
	for _, action := range actions {
		for _, txHash := range action.TxHashes {
			txHashSet[txHash] = true
		}
	}

	if len(txHashSet) == 0 {
		return actions, nil
	}

	// Convert set to slice
	txHashes := make([]HashType, 0, len(txHashSet))
	for hash := range txHashSet {
		txHashes = append(txHashes, hash)
	}

	// Build query using buildTransactionsQuery with multiple hashes
	// Only the Hash field of TransactionRequest is needed for this query; other fields are left at their zero values intentionally.
	tx_req := TransactionRequest{Hash: txHashes}
	query, err := buildTransactionsQuery(
		BlockRequest{}, tx_req, MessageRequest{},
		UtimeRequest{}, LtRequest{}, LimitRequest{}, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	txs, err := queryTransactionsImpl(query, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	// Create a map of transactions by hash
	txMap := make(map[HashType]*Transaction)
	for i := range txs {
		txMap[txs[i].Hash] = &txs[i]
	}

	// Assign transactions to actions
	for i := range actions {
		actions[i].Transactions = make([]*Transaction, 0, len(actions[i].TxHashes))
		for _, txHash := range actions[i].TxHashes {
			if tx, ok := txMap[txHash]; ok {
				actions[i].Transactions = append(actions[i].Transactions, tx)
			}
		}
	}

	return actions, nil
}
