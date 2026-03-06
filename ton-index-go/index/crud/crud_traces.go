package crud

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"

	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
)

func buildTracesQuery(trace_req TracesRequest, utime_req UtimeRequest, lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `E.trace_id, E.external_hash, E.mc_seqno_start, E.mc_seqno_end, 
				   E.start_lt, E.start_utime, E.end_lt, E.end_utime, 
				   E.state, E.edges_, E.nodes_, E.pending_edges_, E.classification_state`

	from_query := `traces as E`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``

	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	sort_order := "desc"
	if v := lim_req.Sort; v != nil {
		sort_order, err = getSortOrder(*v)
		if err != nil {
			return "", err
		}
	}

	// time window + which column to order by
	order_by_now := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_utime >= %d", *v))
		order_by_now = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_utime <= %d", *v))
		order_by_now = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_lt <= %d", *v))
	}

	if order_by_now {
		orderby_query = fmt.Sprintf(" order by E.end_utime %s, E.trace_id %s", sort_order, sort_order)
	} else {
		orderby_query = fmt.Sprintf(" order by E.end_lt %s, E.trace_id %s", sort_order, sort_order)
	}

	// —— Filters that used to cause JOINs — now as EXISTS subqueries —— //

	// account → EXISTS over transactions
	if v := trace_req.AccountAddress; v != nil && len(*v) > 0 {
		filter_list = append(filter_list,
			fmt.Sprintf(`EXISTS (
				SELECT 1
				FROM transactions AS T
				WHERE T.trace_id = E.trace_id
				  AND T.account = '%s'
			)`, *v))
	}

	// transaction hashes → EXISTS over transactions
	if v := trace_req.TransactionHash; v != nil {
		if cond := filterByArray("T.hash", v); len(cond) > 0 {
			filter_list = append(filter_list,
				fmt.Sprintf(`EXISTS (
					SELECT 1
					FROM transactions AS T
					WHERE T.trace_id = E.trace_id
					  AND %s
				)`, cond))
		}
	}

	// message hashes → EXISTS over messages (either raw or normalized)
	if v := trace_req.MessageHash; len(v) > 0 {
		cond := fmt.Sprintf("(%s OR %s)",
			filterByArray("M.msg_hash", v),
			filterByArray("M.msg_hash_norm", v),
		)
		filter_list = append(filter_list,
			fmt.Sprintf(`EXISTS (
				SELECT 1
				FROM messages AS M
				WHERE M.trace_id = E.trace_id
				  AND %s
			)`, cond))
	}

	// —— Filters that are native to traces —— //

	if v := trace_req.TraceId; v != nil {
		if cond := filterByArray("E.trace_id", v); len(cond) > 0 {
			filter_list = append(filter_list, cond)
		}
	}
	if v := trace_req.McSeqno; v != nil {
		filter_list = append(filter_list, `E.state = 'complete'`)
		filter_list = append(filter_list, fmt.Sprintf("E.mc_seqno_end = %d", *v))
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}

	query := `select ` + clmn_query +
		` from ` + from_query +
		filter_query +
		orderby_query +
		limit_query

	return query, nil
}

func queryTracesImpl(query string, includeActions bool, supportedActionTypes []string, conn *pgxpool.Conn, settings RequestSettings) ([]Trace, []string, error) {
	traces := []Trace{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			if loc, err := parse.ScanTrace(rows); err == nil {
				loc.Transactions = make(map[HashType]*Transaction)
				traces = append(traces, *loc)
			} else {
				return nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	traces_map := map[HashType]int{}
	trace_id_list := []HashType{}
	addr_map := map[string]bool{}
	for idx := range traces {
		traces_map[*traces[idx].TraceId] = idx
		if settings.MaxTraceTransactions > 0 && traces[idx].TraceMeta.Transactions > int64(settings.MaxTraceTransactions) {
			traces[idx].IsIncomplete = true
			traces[idx].Warning = "trace is too large"
		} else {
			trace_id_list = append(trace_id_list, *traces[idx].TraceId)
		}
	}
	if len(trace_id_list) > 0 {
		if includeActions {
			arrayFilter := filterByArray("A.trace_id", trace_id_list)
			typeFilter := `A.type = ANY($1) AND NOT(A.ancestor_type && $1::varchar[])`
			if len(arrayFilter) > 0 {
				typeFilter = " AND " + typeFilter
			}
			query := `select A.trace_id, A.action_id, A.start_lt, A.end_lt, A.start_utime, A.end_utime, 
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
				ARRAY[]::text[] from actions as A where ` +
				arrayFilter + typeFilter + `order by trace_id, start_lt, end_lt`
			actions, err := queryRawActionsImpl(query, conn, settings, supportedActionTypes)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed query actions: %s", err.Error())}
			}
			for idx := range traces {
				if traces[idx].Actions == nil {
					new_actions := make([]*Action, 0)
					traces[idx].Actions = &new_actions
				}
			}
			for idx := range actions {
				raw_action := &actions[idx]

				parse.CollectAddressesFromAction(&addr_map, raw_action)

				action, err := parse.ParseRawAction(raw_action)
				if err != nil {
					return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to parse action: %s", err.Error())}
				}
				*traces[traces_map[*action.TraceId]].Actions = append(*traces[traces_map[*action.TraceId]].Actions, action)
			}
		}
		{
			query := `select T.account, T.hash, T.lt, T.block_workchain, T.block_shard, T.block_seqno, T.mc_block_seqno, T.trace_id, 
				T.prev_trans_hash, T.prev_trans_lt, T.now, T.orig_status, T.end_status, T.total_fees, T.total_fees_extra_currencies, 
				T.account_state_hash_before, T.account_state_hash_after, T.descr, T.aborted, T.destroyed, T.credit_first, T.is_tock, 
				T.installed, T.storage_fees_collected, T.storage_fees_due, T.storage_status_change, T.credit_due_fees_collected, T.credit, 
				T.credit_extra_currencies, T.compute_skipped, T.skipped_reason, T.compute_success, T.compute_msg_state_used, T.compute_account_activated, 
				T.compute_gas_fees, T.compute_gas_used, T.compute_gas_limit, T.compute_gas_credit, T.compute_mode, T.compute_exit_code, T.compute_exit_arg, 
				T.compute_vm_steps, T.compute_vm_init_state_hash, T.compute_vm_final_state_hash, T.action_success, T.action_valid, T.action_no_funds, 
				T.action_status_change, T.action_total_fwd_fees, T.action_total_action_fees, T.action_result_code, T.action_result_arg, 
				T.action_tot_actions, T.action_spec_actions, T.action_skipped_actions, T.action_msgs_created, T.action_action_list_hash, 
				T.action_tot_msg_size_cells, T.action_tot_msg_size_bits, T.bounce, T.bounce_msg_size_cells, T.bounce_msg_size_bits, 
				T.bounce_req_fwd_fees, T.bounce_msg_fees, T.bounce_fwd_fees, T.split_info_cur_shard_pfx_len, T.split_info_acc_split_depth, 
				T.split_info_this_addr, T.split_info_sibling_addr, false as emulated, 2 as finality from transactions as T where ` + filterByArray("T.trace_id", trace_id_list) + ` order by T.trace_id, T.lt, T.account`
			txs, err := queryTransactionsImpl(query, conn, settings)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed query transactions: %s", err.Error())}
			}
			for idx := range txs {
				tx := &txs[idx]

				parse.CollectAddressesFromTransactions(&addr_map, tx)
				if v := tx.TraceId; v != nil {
					trace := &traces[traces_map[*v]]
					trace.TransactionsOrder = append(trace.TransactionsOrder, tx.Hash)
					trace.Transactions[tx.Hash] = tx
				}
			}
		}
	}
	for idx := range traces {
		if len(traces[idx].TransactionsOrder) > 0 {
			trace, err := parse.AssembleTraceTxsFromMap(&traces[idx].TransactionsOrder, &traces[idx].Transactions)
			if err != nil {
				if len(traces[idx].Warning) > 0 {
					traces[idx].Warning += ", " + err.Error()
				} else {
					traces[idx].Warning = err.Error()
				}
				// return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to assemble trace: %s", err.Error())}
			}
			if trace != nil {
				traces[idx].Trace = trace
			}
		}
	}

	// TODO: use .Keys method from 1.23 version
	addr_list := []string{}
	for k := range addr_map {
		addr_list = append(addr_list, k)
	}

	return traces, addr_list, nil
}

// Exported methods

func (db *DbClient) QueryTraces(
	trace_req TracesRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Trace, AddressBook, Metadata, error) {
	query, err := buildTracesQuery(trace_req, utime_req, lt_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	// log.Println(query)
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
	if seqno := trace_req.McSeqno; seqno != nil {
		exists, err := queryBlockExists(*seqno, conn, settings)
		if err != nil {
			return nil, nil, nil, err
		}
		if !exists {
			return nil, nil, nil, IndexError{Code: 404, Message: fmt.Sprintf("masterchain block %d not found", *seqno)}
		}
	}

	res, addr_list, err := queryTracesImpl(query, trace_req.IncludeActions, trace_req.SupportedActionTypes, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	if len(addr_list) > 0 {
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

	return res, book, metadata, nil
}

func (db *DbClient) QueryBalanceChanges(
	req BalanceChangesRequest,
	settings RequestSettings,
) (BalanceChangesResult, error) {
	trace_id := req.TraceId
	if trace_id == nil && req.ActionId == nil {
		return BalanceChangesResult{}, IndexError{Code: 400, Message: "trace_id or action_id is required"}
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return BalanceChangesResult{}, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	if trace_id == nil && req.ActionId != nil {
		query := "SELECT trace_id FROM actions WHERE action_id = $1"
		err := conn.QueryRow(ctx, query, *req.ActionId).Scan(&trace_id)
		if err != nil {
			return BalanceChangesResult{}, IndexError{Code: 404, Message: "action_id not found"}
		}
	}

	if trace_id == nil {
		return BalanceChangesResult{}, IndexError{Code: 400, Message: "trace_id is required"}
	}

	trace_changes, actions_changes, err := CalculateBalanceChanges(HashType(*trace_id), conn)
	if err != nil {
		return BalanceChangesResult{}, IndexError{Code: 500, Message: err.Error()}
	}
	var targetChanges *BalanceChanges = trace_changes
	if req.ActionId != nil {
		if v, ok := actions_changes[HashType(*req.ActionId)]; ok {
			targetChanges = v
		} else {
			return BalanceChangesResult{}, nil
		}
	}
	jetton_changes := make(map[AccountAddress]map[AccountAddress]string)
	for accountAddress, jettons := range targetChanges.Jettons {
		jetton_changes[accountAddress] = make(map[AccountAddress]string)
		for jetton, balance := range jettons {
			jetton_changes[accountAddress][jetton] = balance.String()
		}
	}
	return BalanceChangesResult{
		Ton:     targetChanges.get_summarized_balance_changes(),
		Fees:    targetChanges.Fees,
		Jettons: jetton_changes,
	}, nil
}

func (db *DbClient) QueryTransactionsExternalHashes(ctx context.Context, txIDs []HashType,
	settings RequestSettings) ([]HashType, error) {

	if len(txIDs) == 0 {
		return nil, nil
	}

	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()

	stringTxIDs := make([]string, len(txIDs))
	for i, hash := range txIDs {
		stringTxIDs[i] = string(hash)
	}

	query := `
        SELECT DISTINCT tr.external_hash 
        FROM traces tr
        INNER JOIN transactions tx ON tr.trace_id = tx.trace_id
        WHERE tx.hash = ANY($1)
        AND tr.external_hash IS NOT NULL`

	rows, err := conn.Query(ctx, query, pq.Array(stringTxIDs))
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	var externalHashes []HashType
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		externalHashes = append(externalHashes, HashType(hash))
	}

	if err = rows.Err(); err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	return externalHashes, nil
}
