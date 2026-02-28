package crud

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"

	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	T.split_info_this_addr, T.split_info_sibling_addr, false as emulated, 3 as finality from`
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
	if v := tx_req.Hash; len(v) > 0 {
		filter_str := filterByArray("T.hash", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = fmt.Sprintf(" order by T.hash %s", sort_order)
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
			if tx, err := parse.ScanTransaction(rows); err == nil {
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
			M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce, 
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
		T.split_info_this_addr, T.split_info_sibling_addr, false as emulated, 2 as finality from transactions as T where hash in (%s) order by lt asc`, tx_hash_str)
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
