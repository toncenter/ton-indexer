package index

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/lib/pq"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xssnick/tonutils-go/address"
)

// utils
func getAccountAddressFriendly(account string, code_hash *string, is_testnet bool) string {
	addr, err := address.ParseRawAddr(strings.Trim(account, " "))
	if err != nil {
		return "addr_none"
	}
	bouncable := true
	if code_hash == nil || WalletsHashMap[*code_hash] {
		bouncable = false
	}
	addr.SetBounce(bouncable)
	addr.SetTestnetOnly(is_testnet)
	return addr.String()
}

func getSortOrder(order SortType) (string, error) {
	switch strings.ToLower(string(order)) {
	case "desc", "d":
		return "desc", nil
	case "asc", "a":
		return "asc", nil
	}
	return "", IndexError{Code: 422, Message: "wrong value for sort parameter"}
}

// query builders
func limitQuery(lim LimitRequest, settings RequestSettings) (string, error) {
	query := ``
	if lim.Limit == nil {
		// set default value
		lim.Limit = new(int32)
		*lim.Limit = int32(settings.DefaultLimit)
	}
	if lim.Limit != nil {
		limit := max(1, *lim.Limit)
		if limit > int32(settings.MaxLimit) {
			return "", IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
		query += fmt.Sprintf(" limit %d", limit)
	}
	if lim.Offset != nil {
		offset := max(0, *lim.Offset)
		query += fmt.Sprintf(" offset %d", offset)
	}
	return query, nil
}

func buildBlocksQuery(
	blk_req BlockRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) (string, error) {
	query := `select blocks.* from blocks`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	// filters
	if v := blk_req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("workchain = %d", *v))
	}
	if v := blk_req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("shard = %d", *v))
	}
	if v := blk_req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("seqno = %d", *v))
	}
	if v := blk_req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("mc_block_seqno = %d", *v))
	}

	order_col := "gen_utime"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("gen_utime >= %d", *v))
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("gen_utime <= %d", *v))
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("start_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("start_lt <= %d", *v))
	}
	if v := lim_req.Sort; v != nil {
		sort_order, err := getSortOrder(*v)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}

	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildTransactionsQuery(
	blk_req BlockRequest,
	tx_req TransactionRequest,
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) (string, error) {
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
	T.split_info_this_addr, T.split_info_sibling_addr, false as emulated from`
	from_query := ` transactions as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	sort_order := `desc`
	if lim_req.Sort != nil {
		sort_order, err = getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
	}

	// filters
	order_by_now := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now >= %d", *v))
		order_by_now = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now <= %d", *v))
		order_by_now = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt <= %d", *v))
	}
	if order_by_now {
		orderby_query = fmt.Sprintf(" order by T.now %s, T.lt %s, account asc", sort_order, sort_order)
	} else {
		orderby_query = fmt.Sprintf(" order by T.lt %s, account asc", sort_order)
	}

	if v := blk_req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_workchain = %d", *v))
	}
	if v := blk_req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_shard = %d", *v))
	}
	if v := blk_req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_seqno = %d", *v))
	}
	if v := blk_req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.mc_block_seqno = %d", *v))
		orderby_query = fmt.Sprintf(" order by T.lt %s, account asc", sort_order)
	}

	if v := tx_req.Account; len(v) > 0 {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("T.account = '%s'", v[0]))
			if order_by_now {
				orderby_query = fmt.Sprintf(" order by account asc, T.now %s, T.lt %s", sort_order, sort_order)
			} else {
				orderby_query = fmt.Sprintf(" order by account asc, T.lt %s", sort_order)
			}
		} else if len(v) > 1 {
			filter_str := filterByArray("T.account", v)
			filter_list = append(filter_list, filter_str)
		}
	}
	// TODO: implement ExcludeAccount logic
	if v := tx_req.Hash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.hash = '%s'", *v))
		orderby_query = ``
	}
	if v := tx_req.Lt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt = %d", *v))
		orderby_query = fmt.Sprintf(" order by T.lt, account %s", sort_order)
	}

	// transaction by message
	by_msg := false
	if v := msg_req.Direction; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
	}
	if v := msg_req.MessageHash; len(v) > 0 {
		by_msg = true
		filter_str := fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v))
		filter_list = append(filter_list, filter_str)
	}
	if v := msg_req.Source; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
	}
	if v := msg_req.Destination; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
	}
	if v := msg_req.BodyHash; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", *v))
	}
	if v := msg_req.Opcode; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d and M.direction = 'in'", *v))
	}
	if by_msg {
		from_query = " messages as M join transactions as T on M.tx_hash = T.hash and M.tx_lt = T.lt"
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query += from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query) // TODO: remove debug
	return query, nil
}

func buildMessagesQuery(
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) (string, error) {
	rest_columns := `M.trace_id, M.source, M.destination, M.value, 
		M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce, 
		M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm`
	clmn_query := `'', 0, M.msg_hash, '', ` + rest_columns + `, 
		max(case when M.direction='in' then M.tx_hash else null end) as in_tx_hash, 
		max(case when M.direction='out' then M.tx_hash else null end) as out_tx_hash`
	from_query := ` messages as M `
	groupby_query := ` group by M.msg_hash, ` + rest_columns
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := msg_req.Direction; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
	}
	if v := msg_req.Source; v != nil {
		if *v == "null" {
			filter_list = append(filter_list, "M.source is NULL")
		} else {
			filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
		}
	}
	if v := msg_req.Destination; v != nil {
		if *v == "null" {
			filter_list = append(filter_list, "M.destination is NULL")
		} else {
			filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
		}
	}
	if v := msg_req.Opcode; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d", *v))
	}
	if v := msg_req.MessageHash; v != nil {
		filter_str := fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v))
		filter_list = append(filter_list, filter_str)
	}
	if v := msg_req.BodyHash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", *v))
	}

	order_col := "M.created_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at >= %d", *v))
		order_col = "M.created_at"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at <= %d", *v))
		order_col = "M.created_at"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt <= %d", *v))
	}
	if v := msg_req.ExcludeExternals; v != nil && *v {
		filter_list = append(filter_list, order_col+" is not NULL")
	}
	if v := msg_req.OnlyExternals; v != nil && *v {
		filter_list = append(filter_list, order_col+" is NULL")
	}

	sort_order := "desc"
	if lim_req.Sort != nil {
		sort_order, err = getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
	}
	orderby_query = fmt.Sprintf(" order by %s %s, M.msg_hash %s", order_col, sort_order, sort_order)

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	inner_query := `select` + clmn_query
	inner_query += ` from ` + from_query
	inner_query += filter_query
	inner_query += groupby_query
	inner_query += orderby_query
	inner_query += limit_query
	query := `select MM.*, B.*, I.* from (` + inner_query + `) as MM 
	left join message_contents as B on MM.body_hash = B.hash
	left join message_contents as I on MM.init_state_hash = I.hash;`
	// log.Println(query) // TODO: remove debug
	return query, nil
}

func filterByArray[T any](clmn string, values []T) string {
	filter_list := []string{}
	for _, x := range values {
		t := reflect.ValueOf(x)
		switch t.Kind() {
		case reflect.String:
			if t.Len() > 0 {
				filter_list = append(filter_list, fmt.Sprintf("'%s'", t.String()))
			}
		default:
			filter_list = append(filter_list, fmt.Sprintf("'%v'", x))
		}
	}
	if len(filter_list) == 1 {
		return fmt.Sprintf("%s = %s", clmn, filter_list[0])
	}
	if len(filter_list) > 1 {
		vv_str := strings.Join(filter_list, ",")
		return fmt.Sprintf("%s in (%s)", clmn, vv_str)
	}
	return ``
}

func buildNFTCollectionsQuery(nft_req NFTCollectionRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` N.address, N.next_item_index, N.owner_address, N.collection_content, 
				    N.data_hash, N.code_hash, N.last_transaction_lt`
	from_query := ` nft_collections as N`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := nft_req.CollectionAddress; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildNFTItemsQuery(nft_req NFTItemRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` N.address, N.init, N.index, N.collection_address, N.owner_address, N.content, 
					N.last_transaction_lt, N.code_hash, N.data_hash,
					C.address, C.next_item_index, C.owner_address, C.collection_content, 
				    C.data_hash, C.code_hash, C.last_transaction_lt`
	from_query := ` nft_items as N left join nft_collections as C on N.collection_address = C.address`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by N.id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := nft_req.Address; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by N.owner_address, N.collection_address, N.index`
	}
	if v := nft_req.CollectionAddress; v != nil {
		if len(nft_req.CollectionAddress) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("N.collection_address = '%s'", v[0]))
			orderby_query = ` order by collection_address, index`
		} else if len(nft_req.CollectionAddress) > 1 {
			filter_str := filterByArray("N.collection_address", v)
			if len(filter_str) > 0 {
				filter_list = append(filter_list, filter_str)
			}
		}
	}
	if v := nft_req.Index; v != nil {
		if nft_req.CollectionAddress == nil {
			return ``, IndexError{Code: 422, Message: "index parameter is not allowed without the collection_address"}
		}
		filter_str := filterByArray("N.index", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildNFTTransfersQuery(transfer_req NFTTransferRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
		T.nft_item_address, T.nft_item_index, T.nft_collection_address, T.old_owner, T.new_owner, T.response_destination, T.custom_payload,
		T.forward_amount, T.forward_payload, T.trace_id`
	from_query := ` nft_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.new_owner", v)
			} else {
				f_str = filterByArray("T.old_owner", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.old_owner", v)
			f2_str := filterByArray("T.new_owner", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.ItemAddress; v != nil {
		filter_str := filterByArray("T.nft_item_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.nft_collection_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		sort_order, err := getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildJettonMastersQuery(jetton_req JettonMasterRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` J.address, J.total_supply, J.mintable, J.admin_address, J.jetton_content, 
		J.jetton_wallet_code_hash, J.code_hash, J.data_hash, J.last_transaction_lt`
	from_query := ` jetton_masters as J`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := jetton_req.MasterAddress; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := jetton_req.AdminAddress; v != nil {
		filter_str := filterByArray("J.admin_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonWalletsQuery(jetton_req JettonWalletRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `J.address, J.balance, J.owner, J.jetton, J.last_transaction_lt, J.code_hash, J.data_hash, 
		J.mintless_is_claimed, J.mintless_amount, J.mintless_start_from, J.mintless_expire_at, MJM.custom_payload_api_uri`
	from_query := `jetton_wallets as J left join mintless_jetton_masters as MJM on J.jetton = MJM.address`
	filter_list := []string{}
	filter_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	sort_column := "J.id"
	sort_order := "asc"
	if v := lim_req.Sort; v != nil {
		sort_column = "J.balance"
		sort_order, err = getSortOrder(*v)
		if err != nil {
			return "", err
		}

	}
	orderby_query := fmt.Sprintf(` order by %s %s`, sort_column, sort_order)

	if v := jetton_req.Address; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by J.address asc`
	}
	if v := jetton_req.OwnerAddress; v != nil {
		filter_str := filterByArray("J.owner", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = fmt.Sprintf(` order by J.owner, %s %s`, sort_column, sort_order)
	}
	if v := jetton_req.JettonAddress; v != nil {
		if len(jetton_req.JettonAddress) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("J.jetton = '%s'", v[0]))
			orderby_query = fmt.Sprintf(` order by J.jetton, %s %s`, sort_column, sort_order)
		} else if len(jetton_req.JettonAddress) > 1 {
			filter_str := filterByArray("J.jetton", v)
			if len(filter_str) > 0 {
				filter_list = append(filter_list, filter_str)
			}
		}
	}
	if v := jetton_req.ExcludeZeroBalance; v != nil && *v {
		filter_list = append(filter_list, "J.balance + coalesce(mintless_amount, 0) > 0")
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonTransfersQuery(transfer_req JettonTransferRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
		T.amount, T.source, T.destination, T.jetton_wallet_address, T.jetton_master_address, T.response_destination, T.custom_payload,
		T.forward_ton_amount, T.forward_payload, T.trace_id`
	from_query := `jetton_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	filter_list = append(filter_list, "T.tx_aborted is false")

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.destination", v)
			} else {
				f_str = filterByArray("T.source", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.source", v)
			f2_str := filterByArray("T.destination", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.JettonWallet; v != nil {
		filter_str := filterByArray("T.jetton_wallet_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.JettonMaster; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.jetton_master_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		sort_order, err := getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildActionsQuery(act_req ActionRequest, utime_req UtimeRequest, lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
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
		A.value_extra_currencies`
	clmn_query := clmn_query_default
	from_query := `actions as A`
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
	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildTracesQuery(trace_req TracesRequest, utime_req UtimeRequest, lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query_default := `E.trace_id, E.external_hash, E.mc_seqno_start, E.mc_seqno_end, 
						   E.start_lt, E.start_utime, E.end_lt, E.end_utime, 
						   E.state, E.edges_, E.nodes_, E.pending_edges_, E.classification_state`
	clmn_query := clmn_query_default
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

	// time
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

	if v := trace_req.AccountAddress; v != nil && len(*v) > 0 {
		filter_str := fmt.Sprintf("T.account = '%s'", *v)
		filter_list = append(filter_list, filter_str)

		from_query = `traces as E join transactions as T on E.trace_id = T.trace_id`
		if order_by_now {
			clmn_query = `distinct on (E.end_utime, E.trace_id) ` + clmn_query_default
		} else {
			clmn_query = `distinct on (E.end_lt, E.trace_id) ` + clmn_query_default
		}
	}
	if v := trace_req.TransactionHash; v != nil {
		filter_str := filterByArray("T.hash", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		from_query = `traces as E join transactions as T on E.trace_id = T.trace_id`

		if order_by_now {
			clmn_query = `distinct on (E.end_utime, E.trace_id) ` + clmn_query_default
		} else {
			clmn_query = `distinct on (E.end_lt, E.trace_id) ` + clmn_query_default
		}
	}
	if v := trace_req.MessageHash; len(v) > 0 {
		filter_str := fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v))
		filter_list = append(filter_list, filter_str)
		from_query = `traces as E join messages as M on E.trace_id = M.trace_id`

		if order_by_now {
			clmn_query = `distinct on (E.end_utime, E.trace_id) ` + clmn_query_default
		} else {
			clmn_query = `distinct on (E.end_lt, E.trace_id) ` + clmn_query_default
		}
	}

	if v := trace_req.TraceId; v != nil {
		filter_str := filterByArray("E.trace_id", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
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
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonBurnsQuery(burn_req JettonBurnRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `T.tx_hash, T.tx_lt, T.tx_now, T.tx_aborted, T.query_id,
		T.owner, T.jetton_wallet_address, T.jetton_master_address, T.amount, T.response_destination, T.custom_payload, T.trace_id`
	from_query := `jetton_burns as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := burn_req.OwnerAddress; v != nil {
		f_str := ``
		f_str = filterByArray("T.owner", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := burn_req.JettonWallet; v != nil {
		filter_str := filterByArray("T.jetton_wallet_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := burn_req.JettonMaster; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.jetton_master_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		sort_order, err := getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", err
		}
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildAccountStatesQuery(account_req AccountRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query_default := `A.account, A.hash, A.balance, A.balance_extra_currencies, A.account_status, A.frozen_hash, A.last_trans_hash, A.last_trans_lt, A.data_hash, A.code_hash, `
	clmn_query := clmn_query_default + `A.data_boc, A.code_boc`
	from_query := `latest_account_states as A`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	// build query
	if v := account_req.AccountAddress; v != nil {
		f_str := ``
		f_str = filterByArray("A.account", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := account_req.CodeHash; v != nil {
		f_str := ``
		f_str = filterByArray("A.code_hash", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := account_req.IncludeBOC; v != nil && !*v {
		clmn_query = clmn_query_default + `NULL, NULL`
	}

	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

// query implementation functions
func queryBlocksImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Block, error) {
	// blocks
	blks := []Block{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if blk, err := ScanBlock(rows); err == nil {
				blks = append(blks, *blk)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return blks, nil
}

func queryMessagesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Message, error) {
	msgs := []Message{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			msg, err := ScanMessageWithContent(rows)
			if err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
			msgs = append(msgs, *msg)
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return msgs, nil
}

func queryBlockExists(seqno int32, conn *pgxpool.Conn, settings RequestSettings) (bool, error) {
	query := fmt.Sprintf(`select seqno from blocks where workchain = -1 and shard = -9223372036854775808 and seqno = %d`, seqno)
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return false, IndexError{Code: 500, Message: err.Error()}
	}

	seqnos := []int32{}
	for rows.Next() {
		var s int32
		if err := rows.Scan(&s); err != nil {
			return false, err
		}
		seqnos = append(seqnos, s)
	}
	if rows.Err() != nil {
		return false, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return len(seqnos) > 0, nil
}

func queryTransactionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Transaction, error) {
	// transactions
	txs := []Transaction{}
	txs_map := map[HashType]int{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if tx, err := ScanTransaction(rows); err == nil {
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	acst_list := []string{}
	hash_list := []string{}
	for _, t := range txs {
		hash_list = append(hash_list, fmt.Sprintf("'%s'", t.Hash))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashBefore))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashAfter))
	}
	// account states
	if len(txs) == 0 {
		return txs, nil
	}
	if len(acst_list) > 0 {
		acst_list_str := strings.Join(acst_list, ",")
		query = fmt.Sprintf(`select S.hash, S.account, S.balance, 
		S.balance_extra_currencies, S.account_status, S.frozen_hash, 
		S.data_hash, S.code_hash from account_states S where hash in (%s)`, acst_list_str)

		acsts, err := queryAccountStatesImpl(query, conn, settings)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		acsts_map := make(map[HashType]*AccountState)
		for _, a := range acsts {
			acsts_map[a.Hash] = &a
		}
		for idx := range txs {
			if v, ok := acsts_map[txs[idx].AccountStateHashBefore]; ok {
				txs[idx].AccountStateBefore = v
			}
			if v, ok := acsts_map[txs[idx].AccountStateHashAfter]; ok {
				txs[idx].AccountStateAfter = v
			}
		}
	}

	// messages
	if len(hash_list) > 0 {
		hash_list_str := strings.Join(hash_list, ",")
		query = fmt.Sprintf(`select M.tx_hash, M.tx_lt, M.msg_hash, M.direction, M.trace_id, M.source, M.destination, M.value, 
			M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce, 
			M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm, NULL, NULL, B.*, I.* from messages as M 
			left join message_contents as B on M.body_hash = B.hash 
			left join message_contents as I on M.init_state_hash = I.hash
			where M.tx_hash in (%s)`, hash_list_str)

		msgs, err := queryMessagesImpl(query, conn, settings)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}

		for _, msg := range msgs {
			if msg.Direction == "in" {
				txs[txs_map[msg.TxHash]].InMsg = &msg
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, &msg)
			}
		}
	}

	// sort messages
	for idx := range txs {
		sort.SliceStable(txs[idx].OutMsgs, func(i, j int) bool {
			if txs[idx].OutMsgs[i].CreatedLt == nil {
				return true
			}
			if txs[idx].OutMsgs[j].CreatedLt == nil {
				return false
			}
			return *txs[idx].OutMsgs[i].CreatedLt < *txs[idx].OutMsgs[j].CreatedLt
		})
	}
	return txs, nil
}

func queryAdjacentTransactionsImpl(req AdjacentTransactionRequest, conn *pgxpool.Conn, settings RequestSettings) ([]string, error) {
	// transactions
	txs := []string{}
	query := fmt.Sprintf(`select M2.tx_hash from messages as M1 join messages as M2 on M1.msg_hash = M2.msg_hash and M1.direction != M2.direction where M1.tx_hash = '%s'`, req.Hash)
	if req.Direction != nil && (*req.Direction == "in" || *req.Direction == "out") {
		query += fmt.Sprintf(" and M1.direction = '%s'", *req.Direction)
	}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			var tx string
			err := rows.Scan(&tx)
			if err != nil {
				return nil, err
			}
			txs = append(txs, tx)
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return txs, nil
}

func QueryMetadataImpl(addr_list []string, conn *pgxpool.Conn, settings RequestSettings) (Metadata, error) {
	query := "select n.address, m.valid, 'nft_items' as type, m.name, m.symbol, m.description, m.image, m.extra from nft_items n left join address_metadata m on n.address = m.address and m.type = 'nft_items' where n.address = ANY($1)" +
		" union all " +
		"select c.address, m.valid, 'nft_collections' as type, m.name, m.symbol, m.description, m.image, m.extra  from nft_collections c left join address_metadata m on c.address = m.address and m.type = 'nft_collections' where c.address = ANY($1)" +
		" union all " +
		"select j.address, m.valid, 'jetton_masters' as type, m.name, m.symbol, m.description, m.image, m.extra  from jetton_masters j left join address_metadata m on j.address = m.address and m.type = 'jetton_masters'  where j.address = ANY($1)"

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query, pq.Array(addr_list))
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	defer rows.Close()

	tasks := []BackgroundTask{}
	token_info_map := map[string][]TokenInfo{}

	for rows.Next() {
		var row TokenInfo
		err := rows.Scan(&row.Address, &row.Valid, &row.Type, &row.Name, &row.Symbol, &row.Description, &row.Image, &row.Extra)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		if row.Valid == nil {
			data := map[string]interface{}{
				"address": row.Address,
				"type":    row.Type,
			}
			tasks = append(tasks, BackgroundTask{Type: "fetch_metadata", Data: data})
			token_info_map[row.Address] = append(token_info_map[row.Address], TokenInfo{
				Address: row.Address,
				Type:    row.Type,
				Indexed: false,
			})
		} else if *row.Valid {
			row.Indexed = true

			if _, ok := token_info_map[*row.Type]; !ok {
				token_info_map[row.Address] = []TokenInfo{}
			}
			token_info_map[row.Address] = append(token_info_map[row.Address], row)
		} else {
			token_info_map[row.Address] = []TokenInfo{}
		}
	}
	metadata := Metadata{}
	for addr, infos := range token_info_map {
		indexed := true
		for _, info := range infos {
			indexed = indexed && info.Indexed
		}
		metadata[addr] = AddressMetadata{
			TokenInfo: infos,
			IsIndexed: indexed,
		}
	}

	if len(tasks) > 0 && BackgroundTaskManager != nil {
		BackgroundTaskManager.EnqueueTasksIfPossible(tasks)
	}
	return metadata, nil
}

func SubstituteImgproxyBaseUrl(metadata *Metadata, base_url string) {
	proxied_fields := []string{"_image_small", "_image_medium", "_image_big"}

	for _, addr_meta := range *metadata {
		for _, tokenInfo := range addr_meta.TokenInfo {
			if tokenInfo.Image == nil || tokenInfo.Extra == nil {
				continue
			}

			for _, field := range proxied_fields {
				if val, exists := tokenInfo.Extra[field]; exists {
					if img_url, ok := val.(string); ok && img_url != "" {
						if result, err := url.JoinPath(base_url, img_url); err == nil {
							tokenInfo.Extra[field] = result
						} else {
							log.Printf("Error joining imgproxy base URL with image URL: %v", err)
						}
					}
				}
			}
		}
	}
}

func QueryAddressBookImpl(addr_list []string, conn *pgxpool.Conn, settings RequestSettings) (AddressBook, error) {
	book := AddressBook{}
	quote_addr_list := []string{}
	for _, item := range addr_list {
		quote_addr_list = append(quote_addr_list, fmt.Sprintf("'%s'", item))
	}

	// read address book first
	book_tmp := AddressBook{}
	addr_list_str := strings.Join(quote_addr_list, ",")
	{
		query := fmt.Sprintf(`SELECT account, code_hash FROM latest_account_states las
							WHERE account IN (%s)`, addr_list_str)
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var account string
			var code_hash *string
			if err := rows.Scan(&account, &code_hash); err == nil {
				addr_str := getAccountAddressFriendly(account, code_hash, settings.IsTestnet)
				book_tmp[strings.Trim(account, " ")] = AddressBookRow{UserFriendly: &addr_str, Domain: nil}
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	// read dns entries
	{
		query := fmt.Sprintf(`select distinct on(nft_item_owner) nft_item_owner, domain from dns_entries
			where nft_item_owner in (%s)
			and nft_item_owner = dns_wallet
			order by nft_item_owner, length(domain)`, addr_list_str)
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			var account string
			var domain *string
			if err := rows.Scan(&account, &domain); err == nil {
				if book_rec, ok := book_tmp[account]; ok {
					book_rec.Domain = domain
					book_tmp[account] = book_rec
				}
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	for _, addr := range addr_list {
		account := ``
		if addr_val := AccountAddressConverter(addr); addr_val.IsValid() {
			if addr_str, ok := addr_val.Interface().(AccountAddress); ok {
				account = string(addr_str)
			}
		}
		if rec, ok := book_tmp[account]; ok {
			book[addr] = rec
		} else {
			addr_str := getAccountAddressFriendly(account, nil, settings.IsTestnet)
			book[addr] = AddressBookRow{UserFriendly: &addr_str, Domain: nil}
		}
	}
	return book, nil
}

func queryAccountStatesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountState, error) {
	acsts := []AccountState{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountState(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryAccountStateFullImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountStateFull, error) {
	acsts := []AccountStateFull{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountStateFull(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryTopAccountBalancesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountBalance, error) {
	acsts := []AccountBalance{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountBalance(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryNFTCollectionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTCollection, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	nfts := []NFTCollection{}
	for rows.Next() {
		if nft, err := ScanNFTCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	return nfts, nil
}

func queryNFTItemsWithCollectionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTItem, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	nfts := []NFTItem{}
	for rows.Next() {
		if nft, err := ScanNFTItemWithCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return nfts, nil
}

func queryNFTTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []NFTTransfer{}
	for rows.Next() {
		if loc, err := ScanNFTTransfer(rows); err == nil {
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

func queryJettonMastersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonMaster, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonMaster{}
	for rows.Next() {
		if loc, err := ScanJettonMaster(rows); err == nil {
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

func queryJettonWalletsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonWallet, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonWallet{}
	for rows.Next() {
		if loc, err := ScanJettonWallet(rows); err == nil {
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

func queryJettonTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonTransfer{}
	for rows.Next() {
		if loc, err := ScanJettonTransfer(rows); err == nil {
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

func queryJettonBurnsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonBurn, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonBurn{}
	for rows.Next() {
		if loc, err := ScanJettonBurn(rows); err == nil {
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

func queryRawActionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings, args ...any) ([]RawAction, error) {
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

func CollectAddressesFromAction(addr_list *map[string]bool, raw_action *RawAction) bool {
	success := true

	if v := raw_action.Source; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.SourceSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Destination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DestinationSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Asset; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.AssetSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Asset2; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.Asset2Secondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonTransferResponseDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.NFTTransferResponseDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapSender; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferAsset; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferSource; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferSourceJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexIncomingTransferDestinationJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferAsset; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferSource; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferDestination; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferSourceJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.JettonSwapDexOutgoingTransferDestinationJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexDepositLiquidityDataAsset1; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexDepositLiquidityDataAsset2; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexWithdrawLiquidityDataAsset1Out; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.DexWithdrawLiquidityDataAsset2Out; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// Multisig fields
	if v := raw_action.DestinationSecondary; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// EVAA fields
	if v := raw_action.EvaaSupplySenderJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaSupplyRecipientJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaSupplyMasterJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaSupplyMaster; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaWithdrawRecipientJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaWithdrawMasterJettonWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	if v := raw_action.EvaaWithdrawMaster; v != nil {
		(*addr_list)[(string)(*v)] = true
	}

	// JVault fields
	if v := raw_action.JvaultStakeStakeWallet; v != nil {
		(*addr_list)[(string)(*v)] = true
	}
	for _, v := range raw_action.JvaultClaimClaimedJettons {
		(*addr_list)[(string)(v)] = true
	}

	// Vesting fields
	for _, v := range raw_action.VestingAddWhitelistAccountsAdded {
		(*addr_list)[(string)(v)] = true
	}

	return success
}

func collectAddressesFromTransactions(addr_list *map[string]bool, tx *Transaction) bool {
	success := true

	(*addr_list)[(string)(tx.Account)] = true
	if tx.InMsg != nil {
		if v := tx.InMsg.Source; v != nil {
			(*addr_list)[(string)(*v)] = true
		}
	}
	for idx := range tx.OutMsgs {
		if v := tx.OutMsgs[idx].Destination; v != nil {
			(*addr_list)[(string)(*v)] = true
		}
	}
	return success
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
			if loc, err := ScanTrace(rows); err == nil {
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
				A.success, A.trace_external_hash, A.value_extra_currencies,
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
				(A.jvault_stake_data).stake_wallet from actions as A where ` +
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

				CollectAddressesFromAction(&addr_map, raw_action)

				action, err := ParseRawAction(raw_action)
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
				T.split_info_this_addr, T.split_info_sibling_addr, false as emulated from transactions as T where ` + filterByArray("T.trace_id", trace_id_list) + ` order by T.trace_id, T.lt, T.account`
			txs, err := queryTransactionsImpl(query, conn, settings)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed query transactions: %s", err.Error())}
			}
			for idx := range txs {
				tx := &txs[idx]

				collectAddressesFromTransactions(&addr_map, tx)
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
			trace, err := AssembleTraceTxsFromMap(&traces[idx].TransactionsOrder, &traces[idx].Transactions)
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

func AssembleTraceTxsFromMap(tx_order *[]HashType, txs *map[HashType]*Transaction) (*TraceNode, error) {
	nodes := map[HashType]*TraceNode{}
	warning := ``
	var root *TraceNode = nil
	for _, tx_hash := range *tx_order {
		tx := (*txs)[tx_hash]
		var in_msg_hash HashType
		if in_msg := tx.InMsg; in_msg != nil {
			in_msg_hash = in_msg.MsgHash
		}
		node := TraceNode{TransactionHash: tx_hash, InMsgHash: in_msg_hash}
		if len(tx.OutMsgs) == 0 {
			node.Children = make([]*TraceNode, 0)
		}
		for _, msg := range tx.OutMsgs {
			nodes[msg.MsgHash] = &node
		}
		if parent, ok := nodes[in_msg_hash]; ok {
			delete(nodes, in_msg_hash)
			parent.Children = append(parent.Children, &node)
		} else if root == nil {
			root = &node
		} else {
			warning = "missing node in trace found"
		}
	}
	if len(warning) > 0 {
		return root, fmt.Errorf("%s", warning)
	}
	return root, nil
}

// Exported methods
func (db *DbClient) QueryMasterchainInfo(settings RequestSettings) (*MasterchainInfo, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	res := conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno desc limit 1")
	last, err := ScanBlock(res)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	res = conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno asc limit 1")
	first, err := ScanBlock(res)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	info := MasterchainInfo{last, first}
	return &info, nil
}

func (db *DbClient) QueryBlocks(
	blk_req BlockRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Block, error) {
	query, err := buildBlocksQuery(blk_req, utime_req, lt_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryShards(
	seqno int,
	settings RequestSettings,
) ([]Block, error) {
	query := fmt.Sprintf(`select B.* from shard_state as S join blocks as B 
		on S.workchain = B.workchain and S.shard = B.shard and S.seqno = B.seqno 
		where mc_seqno = %d 
		order by S.mc_seqno, S.workchain, S.shard, S.seqno`, seqno)
	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryMetadata(
	addr_list []string,
	settings RequestSettings,
) (Metadata, error) {
	raw_addr_list := []string{}
	for _, addr := range addr_list {
		addr_loc := AccountAddressConverter(addr)
		if addr_loc.IsValid() {
			if v, ok := addr_loc.Interface().(AccountAddress); ok {
				raw_addr_list = append(raw_addr_list, string(v))
			}
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return QueryMetadataImpl(raw_addr_list, conn, settings)
}

func (db *DbClient) QueryAddressBook(
	addr_list []string,
	settings RequestSettings,
) (AddressBook, error) {
	raw_addr_list := []string{}
	raw_addr_map := map[string]string{}
	for _, addr := range addr_list {
		addr_loc := AccountAddressConverter(addr)
		if addr_loc.IsValid() {
			if v, ok := addr_loc.Interface().(AccountAddress); ok {
				raw_addr_list = append(raw_addr_list, string(v))
				raw_addr_map[addr] = string(v)
			}
		} else {
			raw_addr_map[addr] = ""
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	book, err := QueryAddressBookImpl(raw_addr_list, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	new_addr_book := AddressBook{}
	for k, v := range raw_addr_map {
		if vv, ok := book[v]; ok {
			new_addr_book[k] = vv
		} else {
			new_addr_book[k] = AddressBookRow{UserFriendly: nil, Domain: nil}
		}
	}
	return new_addr_book, nil
}

func (db *DbClient) QueryTransactions(
	blk_req BlockRequest,
	tx_req TransactionRequest,
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Transaction, AddressBook, error) {
	query, err := buildTransactionsQuery(blk_req, tx_req, msg_req, utime_req, lt_req, lim_req, settings)
	if settings.DebugRequest {
		log.Println("Debug query:", query)
	}
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// check block
	if seqno := blk_req.McSeqno; seqno != nil {
		exists, err := queryBlockExists(*seqno, conn, settings)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			return nil, nil, IndexError{Code: 404, Message: fmt.Sprintf("masterchain block %d not found", *seqno)}
		}
	}

	txs, err := queryTransactionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []string{}
		for _, t := range txs {
			addr_list = append(addr_list, string(t.Account))
			if t.InMsg != nil {
				if t.InMsg.Source != nil {
					addr_list = append(addr_list, string(*t.InMsg.Source))
				}
			}
			for _, m := range t.OutMsgs {
				if m.Destination != nil {
					addr_list = append(addr_list, string(*m.Destination))
				}
			}
		}
		if len(addr_list) > 0 {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryAdjacentTransactions(
	req AdjacentTransactionRequest,
	settings RequestSettings,
) ([]Transaction, AddressBook, error) {
	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	tx_hash_list, err := queryAdjacentTransactionsImpl(req, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	if len(tx_hash_list) == 0 {
		return nil, nil, IndexError{Code: 404, Message: "adjacent transactions not found"}
	}

	for idx := range tx_hash_list {
		tx_hash_list[idx] = fmt.Sprintf("'%s'", tx_hash_list[idx])
	}
	tx_hash_str := strings.Join(tx_hash_list, ",")
	query := fmt.Sprintf(`select T.account, T.hash, T.lt, T.block_workchain, T.block_shard, T.block_seqno, T.mc_block_seqno, T.trace_id, 
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
		T.split_info_this_addr, T.split_info_sibling_addr, false as emulated from transactions as T where hash in (%s) order by lt asc`, tx_hash_str)
	txs, err := queryTransactionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []string{}
		for _, t := range txs {
			addr_list = append(addr_list, string(t.Account))
			if t.InMsg != nil {
				if t.InMsg.Source != nil {
					addr_list = append(addr_list, string(*t.InMsg.Source))
				}
			}
			for _, m := range t.OutMsgs {
				if m.Destination != nil {
					addr_list = append(addr_list, string(*m.Destination))
				}
			}
		}
		if len(addr_list) > 0 {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryMessages(
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Message, AddressBook, Metadata, error) {
	query, err := buildMessagesQuery(msg_req, utime_req, lt_req, lim_req, settings)
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

	msgs, err := queryMessagesImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, m := range msgs {
		if m.Source != nil {
			addr_list = append(addr_list, string(*m.Source))
		}
		if m.Destination != nil {
			addr_list = append(addr_list, string(*m.Destination))
		}
	}
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
	return msgs, book, metadata, nil
}

func (db *DbClient) QueryNFTCollections(
	nft_req NFTCollectionRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTCollection, AddressBook, Metadata, error) {
	query, err := buildNFTCollectionsQuery(nft_req, lim_req, settings)
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

	res, err := queryNFTCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		if t.OwnerAddress != nil {
			addr_list = append(addr_list, string(*t.OwnerAddress))
		}
	}
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

func (db *DbClient) QueryNFTItems(
	nft_req NFTItemRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTItem, AddressBook, Metadata, error) {
	query, err := buildNFTItemsQuery(nft_req, lim_req, settings)
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

	res, err := queryNFTItemsWithCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		if t.CollectionAddress != nil {
			addr_list = append(addr_list, string(*t.CollectionAddress))
		}
		if t.OwnerAddress != nil {
			addr_list = append(addr_list, string(*t.OwnerAddress))
		}
	}
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

func (db *DbClient) QueryNFTTransfers(
	transfer_req NFTTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTTransfer, AddressBook, Metadata, error) {
	query, err := buildNFTTransfersQuery(transfer_req, utime_req, lt_req, lim_req, settings)
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

	res, err := queryNFTTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.NftItemAddress))
		addr_list = append(addr_list, string(t.NftCollectionAddress))
		addr_list = append(addr_list, string(t.OldOwner))
		addr_list = append(addr_list, string(t.NewOwner))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, string(*t.ResponseDestination))
		}
	}
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

func (db *DbClient) QueryJettonMasters(
	jetton_req JettonMasterRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonMaster, AddressBook, Metadata, error) {
	query, err := buildJettonMastersQuery(jetton_req, lim_req, settings)
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

	res, err := queryJettonMastersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		if t.AdminAddress != nil {
			addr_list = append(addr_list, string(*t.AdminAddress))
		}
	}
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

func (db *DbClient) QueryJettonWallets(
	jetton_req JettonWalletRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonWallet, AddressBook, Metadata, error) {
	query, err := buildJettonWalletsQuery(jetton_req, lim_req, settings)
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

	res, err := queryJettonWalletsImpl(query, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Address))
		addr_list = append(addr_list, string(t.Owner))
		addr_list = append(addr_list, string(t.Jetton))
	}
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

func (db *DbClient) QueryJettonTransfers(
	transfer_req JettonTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonTransfer, AddressBook, Metadata, error) {
	query, err := buildJettonTransfersQuery(transfer_req, utime_req, lt_req, lim_req, settings)
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

	res, err := queryJettonTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Source))
		addr_list = append(addr_list, string(t.Destination))
		addr_list = append(addr_list, string(t.SourceWallet))
		addr_list = append(addr_list, string(t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, string(*t.ResponseDestination))
		}
	}
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

func (db *DbClient) QueryJettonBurns(
	transfer_req JettonBurnRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonBurn, AddressBook, Metadata, error) {
	query, err := buildJettonBurnsQuery(transfer_req, utime_req, lt_req, lim_req, settings)
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

	res, err := queryJettonBurnsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, string(t.Owner))
		addr_list = append(addr_list, string(t.JettonWallet))
		addr_list = append(addr_list, string(t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, string(*t.ResponseDestination))
		}
	}
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

func (db *DbClient) QueryAccountStates(
	account_req AccountRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]AccountStateFull, AddressBook, Metadata, error) {
	query, err := buildAccountStatesQuery(account_req, lim_req, settings)
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

	res, err := queryAccountStateFullImpl(query, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}
	addr_list := []string{}
	for _, t := range res {
		if t.AccountAddress != nil {
			addr_list = append(addr_list, string(*t.AccountAddress))
		}
	}
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

func (db *DbClient) QueryWalletStates(
	account_req AccountRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]WalletState, AddressBook, Metadata, error) {
	states, book, metadata, err := db.QueryAccountStates(account_req, lim_req, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	res := []WalletState{}
	for _, state := range states {
		loc, err := ParseWalletState(state)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		res = append(res, *loc)
	}
	return res, book, metadata, nil
}

func (db *DbClient) QueryTopAccountBalances(lim_req LimitRequest, settings RequestSettings) ([]AccountBalance, error) {
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, err
	}
	query := `select account, balance from latest_account_states order by balance desc` + limit_query

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryTopAccountBalancesImpl(query, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	return res, nil
}

func (db *DbClient) QueryActions(
	act_req ActionRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Action, AddressBook, Metadata, error) {
	query, err := buildActionsQuery(act_req, utime_req, lt_req, lim_req, settings)
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

	raw_actions, err := queryRawActionsImpl(query, conn, settings)
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
		metadata, err = QueryMetadataImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
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

func (db *DbClient) QueryDNSRecords(lim_req LimitRequest, req DNSRecordsRequest, settings RequestSettings) ([]DNSRecord, AddressBook, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	query := `
        SELECT nft_item_address, nft_item_owner, domain, dns_next_resolver, dns_wallet, dns_site_adnl, dns_storage_bag_id
        FROM dns_entries
        WHERE dns_wallet = $1
		ORDER BY LENGTH(domain), domain ASC ` + limit_query

	rows, err := conn.Query(ctx, query, req.WalletAddress)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	records := []DNSRecord{}
	for rows.Next() {
		var record DNSRecord
		if err := rows.Scan(&record.NftItemAddress, &record.NftItemOwner, &record.Domain,
			&record.NextResolver, &record.Wallet, &record.SiteAdnl, &record.StorageBagID); err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		records = append(records, record)
	}
	book := AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []string{}
		for _, r := range records {
			addr_list = append(addr_list, string(r.NftItemAddress))
			if r.NftItemOwner != nil {
				addr_list = append(addr_list, string(*r.NftItemOwner))
			}
			if r.Wallet != nil {
				addr_list = append(addr_list, string(*r.Wallet))
			}
		}
		if len(addr_list) > 0 {
			book, err = QueryAddressBookImpl(addr_list, conn, settings)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	return records, book, nil
}
