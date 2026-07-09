package crud

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
)

const transactionsColumns = `T.account, T.hash, T.lt, T.block_workchain, T.block_shard, T.block_seqno, T.mc_block_seqno, T.trace_id,
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
	T.split_info_this_addr, T.split_info_sibling_addr, false as emulated, 2 as finality`

func buildTransactionsQuery(
	req models.TransactionsRequest,
	settings models.RequestSettings,
) (string, []any, error) {
	args := []any{}
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()
	lim_req := req.GetLimitParams()

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
	T.split_info_this_addr, T.split_info_sibling_addr, false as emulated, 2 as finality from`
	from_query := ` transactions as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", nil, err
	}

	sort_order := `desc`
	if lim_req.Sort != nil {
		sort_order, err = getSortOrder(*lim_req.Sort)
		if err != nil {
			return "", nil, err
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

	if v := req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_workchain = %d", *v))
	}
	if v := req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_shard = %d", *v))
	}
	if v := req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_seqno = %d", *v))
	}
	if v := req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.mc_block_seqno = %d", *v))
		orderby_query = fmt.Sprintf(" order by T.lt %s, account asc", sort_order)
	}

	if v := req.Account; len(v) > 0 {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("T.account = '%s'", v[0].FilterString()))
			if order_by_now {
				orderby_query = fmt.Sprintf(" order by account %s, T.now %s, T.lt %s", sort_order, sort_order, sort_order)
			} else {
				orderby_query = fmt.Sprintf(" order by account %s, T.lt %s", sort_order, sort_order)
			}
		} else if len(v) > 1 {
			filter_str := filterByArray("T.account", v)
			filter_list = append(filter_list, filter_str)
		}
	}
	// TODO: implement ExcludeAccount logic
	if v := req.Hash; len(v) > 0 {
		filter_str := filterByArray("T.hash", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = fmt.Sprintf(" order by T.hash %s", sort_order)
	}
	if v := req.Lt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt = %d", *v))
		orderby_query = fmt.Sprintf(" order by T.lt, account %s", sort_order)
	}

	// transaction by message
	by_msg := false
	if v := req.Direction; v != nil {
		by_msg = true
		args = append(args, *v)
		filter_list = append(filter_list, fmt.Sprintf("M.direction = $%d", len(args)))
	}
	if v := req.MessageHash; len(v) > 0 {
		by_msg = true
		filter_str := fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v))
		filter_list = append(filter_list, filter_str)
	}
	if v := req.Source; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", v.FilterString()))
	}
	if v := req.Destination; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", v.FilterString()))
	}
	if v := req.BodyHash; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", v.FilterString()))
	}
	if v := req.Opcode; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d", *v))
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
	return query, args, nil
}

func queryTransactionsImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings, store *KvrocksStore, args ...any) ([]models.Transaction, error) {
	// transactions
	txs := []models.Transaction{}
	txs_map := map[models.HashType]int{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
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
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	acst_list := []string{}
	hash_list := []string{}
	for _, t := range txs {
		hash_list = append(hash_list, fmt.Sprintf("'%s'", t.Hash.FilterString()))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashBefore.FilterString()))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashAfter.FilterString()))
	}
	// account states
	if len(txs) == 0 {
		return txs, nil
	}
	if len(acst_list) > 0 && store == nil {
		acst_list_str := strings.Join(acst_list, ",")
		query = fmt.Sprintf(`select S.hash, S.account, S.balance,
		S.balance_extra_currencies, S.account_status, S.frozen_hash, 
		S.data_hash, S.code_hash from account_states S where hash in (%s)`, acst_list_str)

		acsts, err := queryAccountStatesImpl(query, conn, settings)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		acsts_map := make(map[models.HashType]*models.AccountState)
		for _, a := range acsts {
			acst := a
			acsts_map[acst.Hash] = &acst
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
		if store != nil {
			query = fmt.Sprintf(`select M.tx_hash, M.tx_lt, M.msg_hash, M.direction, M.trace_id, M.source, M.destination, M.value, 
			M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce, 
			M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm, NULL, NULL, NULL::tonhash, NULL::text, NULL::tonhash, NULL::text
			from messages as M 
			where M.tx_hash in (%s)`, hash_list_str)
		} else {
			query = fmt.Sprintf(`select M.tx_hash, M.tx_lt, M.msg_hash, M.direction, M.trace_id, M.source, M.destination, M.value, 
			M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce, 
			M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm, NULL, NULL, B.*, I.* from messages as M 
			left join message_contents as B on M.body_hash = B.hash 
			left join message_contents as I on M.init_state_hash = I.hash
			where M.tx_hash in (%s)`, hash_list_str)
		}

		msgs, err := queryMessagesImpl(query, conn, settings)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		if store == nil {
			if err := finalizeMessages(msgs, nil, settings); err != nil {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
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

func transactionPtrs(txs []models.Transaction, idxs []int) []*models.Transaction {
	if len(txs) == 0 {
		return nil
	}
	if idxs == nil {
		ptrs := make([]*models.Transaction, 0, len(txs))
		for i := range txs {
			ptrs = append(ptrs, &txs[i])
		}
		return ptrs
	}

	ptrs := make([]*models.Transaction, 0, len(idxs))
	for _, i := range idxs {
		if i >= 0 && i < len(txs) {
			ptrs = append(ptrs, &txs[i])
		}
	}
	return ptrs
}

func transactionMessagePtrs(txs []*models.Transaction) []*models.Message {
	msgs := make([]*models.Message, 0)
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		if tx.InMsg != nil {
			msgs = append(msgs, tx.InMsg)
		}
		msgs = append(msgs, tx.OutMsgs...)
	}
	return msgs
}

func finalizeTransactionSliceFromKvrocks(txs []models.Transaction, idxs []int, store *KvrocksStore, settings models.RequestSettings) error {
	return finalizeTransactionPtrsFromKvrocks(transactionPtrs(txs, idxs), store, settings)
}

func finalizeTransactionPtrsFromKvrocks(txs []*models.Transaction, store *KvrocksStore, settings models.RequestSettings) error {
	if store == nil || len(txs) == 0 {
		return nil
	}

	maxMcSeqno := uint32(0)
	hashes := make([]models.HashType, 0, len(txs)*2)
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		if tx.McSeqno > 0 && uint32(tx.McSeqno) > maxMcSeqno {
			maxMcSeqno = uint32(tx.McSeqno)
		}
		hashes = append(hashes, tx.AccountStateHashBefore, tx.AccountStateHashAfter)
	}
	pinnedCtx := store.pinReadSnapshot(kvrocksContextWithMaxKnownMcSeqno(context.Background(), maxMcSeqno))
	if len(hashes) > 0 {
		ctx, cancel_ctx := context.WithTimeout(pinnedCtx, settings.Timeout)
		defer cancel_ctx()
		acsts_map, err := store.GetAccountStates(ctx, hashes)
		if err != nil {
			return models.IndexError{Code: 500, Message: err.Error()}
		}
		for _, tx := range txs {
			if tx == nil {
				continue
			}
			if v, ok := acsts_map[tx.AccountStateHashBefore]; ok {
				tx.AccountStateBefore = v
			}
			if v, ok := acsts_map[tx.AccountStateHashAfter]; ok {
				tx.AccountStateAfter = v
			}
		}
	}

	return finalizeMessagePtrs(pinnedCtx, transactionMessagePtrs(txs), store, settings)
}

func finalizeTraceTransactionsFromKvrocks(traces []models.Trace, store *KvrocksStore, settings models.RequestSettings) error {
	if store == nil || len(traces) == 0 {
		return nil
	}
	seen := make(map[models.HashType]bool)
	txs := make([]*models.Transaction, 0)
	for i := range traces {
		for _, tx := range traces[i].Transactions {
			if tx == nil || seen[tx.Hash] {
				continue
			}
			seen[tx.Hash] = true
			txs = append(txs, tx)
		}
	}
	return finalizeTransactionPtrsFromKvrocks(txs, store, settings)
}

func queryAdjacentTransactionsImpl(req models.AdjacentTransactionRequest, conn *pgxpool.Conn, settings models.RequestSettings) ([]models.HashType, error) {
	// transactions
	txs := []models.HashType{}
	args := []any{req.Hash}
	query := `select M2.tx_hash from messages as M1 join messages as M2 on M1.msg_hash = M2.msg_hash and M1.direction != M2.direction where M1.tx_hash = $1`
	if req.Direction != nil && (*req.Direction == "in" || *req.Direction == "out") {
		args = append(args, *req.Direction)
		query += fmt.Sprintf(" and M1.direction = $%d", len(args))
	}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			var tx models.HashType
			err := rows.Scan(&tx)
			if err != nil {
				return nil, err
			}
			txs = append(txs, tx)
		}
		if rows.Err() != nil {
			return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return txs, nil
}

func collectAddressesFromTransactions(addr_list *map[models.AccountAddress]bool, tx *models.Transaction) bool {
	success := true

	(*addr_list)[tx.Account] = true
	if tx.InMsg != nil {
		if v := tx.InMsg.Source; v != nil {
			(*addr_list)[*v] = true
		}
	}
	for idx := range tx.OutMsgs {
		if v := tx.OutMsgs[idx].Destination; v != nil {
			(*addr_list)[*v] = true
		}
	}
	return success
}

// scanTransactionsImpl runs a transactions query and scans the bare rows.
// Account states and messages are attached separately, per owning DB.
func scanTransactionsImpl(query string, conn *pgxpool.Conn, settings models.RequestSettings, args ...any) ([]models.Transaction, error) {
	txs := []models.Transaction{}
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer rows.Close()

	for rows.Next() {
		if tx, err := parse.ScanTransaction(rows); err == nil {
			txs = append(txs, *tx)
		} else {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, models.IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return txs, nil
}

// enrichTransactionsImpl attaches account states and messages to the
// transactions at idxs, querying conn (the DB that owns those rows).
func enrichTransactionsImpl(txs []models.Transaction, idxs []int, conn *pgxpool.Conn, settings models.RequestSettings, store *KvrocksStore) error {
	if len(idxs) == 0 {
		return nil
	}
	txs_map := map[models.HashType]int{}
	acst_list := []string{}
	hash_list := []string{}
	for _, i := range idxs {
		txs_map[txs[i].Hash] = i
		hash_list = append(hash_list, fmt.Sprintf("'%s'", txs[i].Hash.FilterString()))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", txs[i].AccountStateHashBefore.FilterString()))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", txs[i].AccountStateHashAfter.FilterString()))
	}

	// account states
	if store == nil {
		acst_list_str := strings.Join(acst_list, ",")
		query := fmt.Sprintf(`select S.hash, S.account, S.balance,
	S.balance_extra_currencies, S.account_status, S.frozen_hash,
	S.data_hash, S.code_hash from account_states S where hash in (%s)`, acst_list_str)

		acsts, err := queryAccountStatesImpl(query, conn, settings)
		if err != nil {
			return models.IndexError{Code: 500, Message: err.Error()}
		}
		acsts_map := make(map[models.HashType]*models.AccountState)
		for _, a := range acsts {
			acst := a
			acsts_map[acst.Hash] = &acst
		}
		for _, i := range idxs {
			if v, ok := acsts_map[txs[i].AccountStateHashBefore]; ok {
				txs[i].AccountStateBefore = v
			}
			if v, ok := acsts_map[txs[i].AccountStateHashAfter]; ok {
				txs[i].AccountStateAfter = v
			}
		}
	}

	// messages
	hash_list_str := strings.Join(hash_list, ",")
	var query string
	if store != nil {
		query = fmt.Sprintf(`select M.tx_hash, M.tx_lt, M.msg_hash, M.direction, M.trace_id, M.source, M.destination, M.value,
		M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce,
		M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm, NULL, NULL, NULL::tonhash, NULL::text, NULL::tonhash, NULL::text
		from messages as M
		where M.tx_hash in (%s)`, hash_list_str)
	} else {
		query = fmt.Sprintf(`select M.tx_hash, M.tx_lt, M.msg_hash, M.direction, M.trace_id, M.source, M.destination, M.value,
		M.value_extra_currencies, M.fwd_fee, M.ihr_fee, M.extra_flags, M.created_lt, M.created_at, M.opcode, M.ihr_disabled, M.bounce,
		M.bounced, M.import_fee, M.body_hash, M.init_state_hash, M.msg_hash_norm, NULL, NULL, B.*, I.* from messages as M
		left join message_contents as B on M.body_hash = B.hash
		left join message_contents as I on M.init_state_hash = I.hash
		where M.tx_hash in (%s)`, hash_list_str)
	}

	msgs, err := queryMessagesImpl(query, conn, settings)
	if err != nil {
		return models.IndexError{Code: 500, Message: err.Error()}
	}
	if store == nil {
		if err := finalizeMessages(msgs, nil, settings); err != nil {
			return models.IndexError{Code: 500, Message: err.Error()}
		}
	}
	for _, msg := range msgs {
		if msg.Direction == "in" {
			txs[txs_map[msg.TxHash]].InMsg = &msg
		} else {
			txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, &msg)
		}
	}

	// sort messages
	for _, i := range idxs {
		sort.SliceStable(txs[i].OutMsgs, func(a, b int) bool {
			if txs[i].OutMsgs[a].CreatedLt == nil {
				return true
			}
			if txs[i].OutMsgs[b].CreatedLt == nil {
				return false
			}
			return *txs[i].OutMsgs[a].CreatedLt < *txs[i].OutMsgs[b].CreatedLt
		})
	}
	return nil
}

// txQueryParts holds the shared pieces of a transactions listing query.
type txQueryParts struct {
	fromQuery  string
	filterList []string
	args       []any
	orderBy    string
	orderByNow bool
}

func transactionsQueryParts(req models.TransactionsRequest, sortOrder string) txQueryParts {
	utime_req := req.GetUtimeParams()
	lt_req := req.GetLtParams()

	from_query := ` transactions as T`
	filter_list := []string{}
	args := []any{}

	orderByNow := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now >= %d", *v))
		orderByNow = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now <= %d", *v))
		orderByNow = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt <= %d", *v))
	}

	if v := req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_workchain = %d", *v))
	}
	if v := req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_shard = %d", *v))
	}
	if v := req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_seqno = %d", *v))
	}
	if v := req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.mc_block_seqno = %d", *v))
	}

	accountFirst := false
	if v := req.Account; len(v) > 0 {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("T.account = '%s'", v[0].FilterString()))
			accountFirst = true
		} else {
			filter_list = append(filter_list, filterByArray("T.account", v))
		}
	}
	if v := req.Lt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt = %d", *v))
	}

	by_msg := false
	if v := req.Direction; v != nil {
		by_msg = true
		args = append(args, *v)
		filter_list = append(filter_list, fmt.Sprintf("M.direction = $%d", len(args)))
	}
	if v := req.MessageHash; len(v) > 0 {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", filterByArray("M.msg_hash", v), filterByArray("M.msg_hash_norm", v)))
	}
	if v := req.Source; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", v.FilterString()))
	}
	if v := req.Destination; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", v.FilterString()))
	}
	if v := req.BodyHash; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", v.FilterString()))
	}
	if v := req.Opcode; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d", *v))
	}
	if by_msg {
		from_query = " messages as M join transactions as T on M.tx_hash = T.hash and M.tx_lt = T.lt"
	}

	var orderby_query string
	switch {
	case accountFirst && orderByNow:
		orderby_query = fmt.Sprintf(" order by account %s, T.now %s, T.lt %s", sortOrder, sortOrder, sortOrder)
	case accountFirst:
		orderby_query = fmt.Sprintf(" order by account %s, T.lt %s", sortOrder, sortOrder)
	case orderByNow:
		orderby_query = fmt.Sprintf(" order by T.now %s, T.lt %s, account %s", sortOrder, sortOrder, sortOrder)
	default:
		orderby_query = fmt.Sprintf(" order by T.lt %s, account %s", sortOrder, sortOrder)
	}

	return txQueryParts{fromQuery: from_query, filterList: filter_list, args: args, orderBy: orderby_query, orderByNow: orderByNow}
}

func buildTransactionsOffsetQuery(p txQueryParts, offset, limit int) string {
	filter_query := ``
	if len(p.filterList) > 0 {
		filter_query = ` where ` + strings.Join(p.filterList, " and ")
	}
	limit_query := fmt.Sprintf(" limit %d offset %d", max(1, limit), max(0, offset))
	return `select ` + transactionsColumns + ` from` + p.fromQuery + filter_query + p.orderBy + limit_query
}

// txOrderKey returns the row's non-null router sort key.
func txOrderKey(orderByNow bool) func(*models.Transaction) *uint64 {
	if orderByNow {
		return func(t *models.Transaction) *uint64 {
			v := uint64(t.Now)
			return &v
		}
	}
	return func(t *models.Transaction) *uint64 {
		v := uint64(t.Lt)
		return &v
	}
}

// queryTransactionsRouted returns one router-served page plus the side it came from.
func queryTransactionsRouted(
	fc *fedConns,
	req models.TransactionsRequest,
	parts txQueryParts,
	sortOrder string,
	offset, limit int,
	fetch func(query string, conn *pgxpool.Conn) ([]models.Transaction, error),
) ([]models.Transaction, bool, error) {
	query := buildTransactionsOffsetQuery(parts, offset, limit)

	// Standalone (single pool): read cold directly, one query, no classification.
	dec := routeCold
	var floor uint64
	if fc.federated {
		w := routeWindow{
			startLt:    req.StartLt,
			endLt:      req.EndLt,
			startUtime: (*uint64)(req.StartUtime),
			endUtime:   (*uint64)(req.EndUtime),
			orderByNow: parts.orderByNow,
			sortDesc:   sortOrder == "desc",
			// Transaction sort keys are never NULL.
			canHaveNullKeys: false,
		}
		dec = classifyRoute(w, fc.split, fc.utimeMargin)

		// Verify against the same floor classifyRoute used.
		floor = fc.split.Lt
		if parts.orderByNow {
			floor = fc.split.Utime + fc.utimeMargin
		}
	}

	return routedPage(fc, dec,
		func(conn *pgxpool.Conn) ([]models.Transaction, error) { return fetch(query, conn) },
		txOrderKey(parts.orderByNow), limit, floor)
}

// txEnrichmentSplit chooses where account states/messages are read from.
// Router-served pages use one DB; point lookups can mix sides and split by lt.
func txEnrichmentSplit(txs []models.Transaction, fc *fedConns, routed, servedCold bool) (hotIdx, coldIdx []int) {
	for i := range txs {
		toCold := servedCold
		if !routed {
			toCold = fc.federated && uint64(txs[i].Lt) < fc.split.Lt
		}
		if toCold {
			coldIdx = append(coldIdx, i)
		} else {
			hotIdx = append(hotIdx, i)
		}
	}
	return hotIdx, coldIdx
}

func (db *DbClient) QueryTransactions(
	req models.TransactionsRequest,
	settings models.RequestSettings,
) ([]models.Transaction, models.AddressBook, error) {
	lim_req := req.GetLimitParams()
	sortOrder := "desc"
	if v := lim_req.Sort; v != nil {
		var serr error
		sortOrder, serr = getSortOrder(*v)
		if serr != nil {
			return nil, nil, serr
		}
	}
	limit := int32(settings.DefaultLimit)
	if lim_req.Limit != nil {
		limit = max(1, *lim_req.Limit)
		if limit > int32(settings.MaxLimit) {
			return nil, nil, models.IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
	}

	fc, release, err := db.acquireFedForRequest(settings)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	var txs []models.Transaction
	routed, servedCold := false, false
	switch {
	case len(req.Hash) > 0:
		// point lookup by tx hash: a tx may be hot or cold-only, consult both
		query, args, qerr := buildTransactionsQuery(req, settings)
		if qerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: qerr.Error()}
		}
		if settings.DebugRequest {
			log.Println("Debug query:", query)
		}
		txs, err = hotThenCold(fc, len(req.Hash),
			func(conn *pgxpool.Conn) ([]models.Transaction, error) {
				return scanTransactionsImpl(query, conn, settings, args...)
			},
			func(t *models.Transaction) models.HashType { return t.Hash })
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		sort.Slice(txs, func(i, j int) bool {
			if sortOrder == "desc" {
				return txs[i].Hash > txs[j].Hash
			}
			return txs[i].Hash < txs[j].Hash
		})
		if len(txs) > int(limit) {
			txs = txs[:limit]
		}

	case req.McSeqno != nil:
		// a block's transactions live wholly on one side
		conn, cerr := fc.connForSeqno(uint64(*req.McSeqno))
		if cerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: cerr.Error()}
		}
		exists, eerr := queryBlockExists(*req.McSeqno, conn, settings)
		if eerr != nil {
			return nil, nil, eerr
		}
		if !exists {
			return nil, nil, models.IndexError{Code: 404, Message: fmt.Sprintf("masterchain block %d not found", *req.McSeqno)}
		}
		parts := transactionsQueryParts(req, sortOrder)
		offset := 0
		if lim_req.Offset != nil {
			offset = int(max(0, *lim_req.Offset))
		}
		query := buildTransactionsOffsetQuery(parts, offset, int(limit))
		if settings.DebugRequest {
			log.Println("Debug query:", query)
		}
		txs, err = scanTransactionsImpl(query, conn, settings, parts.args...)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}

	default:
		parts := transactionsQueryParts(req, sortOrder)
		offset := 0
		if lim_req.Offset != nil {
			offset = int(max(0, *lim_req.Offset))
		}
		fetch := func(query string, conn *pgxpool.Conn) ([]models.Transaction, error) {
			if settings.DebugRequest {
				log.Println("Debug query:", query)
			}
			return scanTransactionsImpl(query, conn, settings, parts.args...)
		}
		routed = true
		txs, servedCold, err = queryTransactionsRouted(fc, req, parts, sortOrder, offset, int(limit), fetch)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	hotIdx, coldIdx := txEnrichmentSplit(txs, fc, routed, servedCold)
	if len(hotIdx) > 0 {
		hotConn, herr := fc.hot()
		if herr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: herr.Error()}
		}
		if eerr := enrichTransactionsImpl(txs, hotIdx, hotConn, settings, db.Kvrocks); eerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: eerr.Error()}
		}
	}
	if len(coldIdx) > 0 {
		coldConn, cerr := fc.cold()
		if cerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: cerr.Error()}
		}
		if eerr := enrichTransactionsImpl(txs, coldIdx, coldConn, settings, db.Kvrocks); eerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: eerr.Error()}
		}
	}
	if db.Kvrocks != nil {
		release()
		if eerr := finalizeTransactionSliceFromKvrocks(txs, nil, db.Kvrocks, settings); eerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: eerr.Error()}
		}
	}

	book := models.AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []models.AccountAddress{}
		for _, t := range txs {
			addr_list = append(addr_list, t.Account)
			if t.InMsg != nil {
				if t.InMsg.Source != nil {
					addr_list = append(addr_list, *t.InMsg.Source)
				}
			}
			for _, m := range t.OutMsgs {
				if m.Destination != nil {
					addr_list = append(addr_list, *m.Destination)
				}
			}
		}
		if len(addr_list) > 0 {
			if db.Kvrocks != nil {
				release()
				book, err = QueryAddressBookImplKvrocks(addr_list, db.Kvrocks, settings)
			} else {
				coldConn, cerr := fc.cold()
				if cerr != nil {
					return nil, nil, models.IndexError{Code: 500, Message: cerr.Error()}
				}
				book, err = QueryAddressBookImpl(addr_list, coldConn, settings)
			}
			if err != nil {
				return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryAdjacentTransactions(
	req models.AdjacentTransactionRequest,
	settings models.RequestSettings,
) ([]models.Transaction, models.AddressBook, error) {
	fc, release, err := db.acquireFedForRequest(settings)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	tx_hash_list, err := hotThenCold(fc, -1,
		func(conn *pgxpool.Conn) ([]models.HashType, error) {
			return queryAdjacentTransactionsImpl(req, conn, settings)
		},
		func(h *models.HashType) models.HashType { return *h })
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	if len(tx_hash_list) == 0 {
		return nil, nil, models.IndexError{Code: 404, Message: "adjacent transactions not found"}
	}

	tx_hash_str_list := []string{}
	for idx := range tx_hash_list {
		tx_hash_str_list = append(tx_hash_str_list, fmt.Sprintf("'%s'", tx_hash_list[idx].FilterString()))
	}
	tx_hash_str := strings.Join(tx_hash_str_list, ",")
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
	txs, err := hotThenCold(fc, len(tx_hash_list),
		func(conn *pgxpool.Conn) ([]models.Transaction, error) {
			return queryTransactionsImpl(query, conn, settings, db.Kvrocks)
		},
		func(t *models.Transaction) models.HashType { return t.Hash })
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	sort.Slice(txs, func(i, j int) bool { return txs[i].Lt < txs[j].Lt })
	if db.Kvrocks != nil {
		release()
		if eerr := finalizeTransactionSliceFromKvrocks(txs, nil, db.Kvrocks, settings); eerr != nil {
			return nil, nil, models.IndexError{Code: 500, Message: eerr.Error()}
		}
	}

	book := models.AddressBook{}
	if !settings.NoAddressBook {
		addr_list := []models.AccountAddress{}
		for _, t := range txs {
			addr_list = append(addr_list, t.Account)
			if t.InMsg != nil {
				if t.InMsg.Source != nil {
					addr_list = append(addr_list, *t.InMsg.Source)
				}
			}
			for _, m := range t.OutMsgs {
				if m.Destination != nil {
					addr_list = append(addr_list, *m.Destination)
				}
			}
		}
		if len(addr_list) > 0 {
			if db.Kvrocks != nil {
				release()
				book, err = QueryAddressBookImplKvrocks(addr_list, db.Kvrocks, settings)
			} else {
				coldConn, cerr := fc.cold()
				if cerr != nil {
					return nil, nil, models.IndexError{Code: 500, Message: cerr.Error()}
				}
				book, err = QueryAddressBookImpl(addr_list, coldConn, settings)
			}
			if err != nil {
				return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryTransactionsExternalHashes(ctx context.Context, txIDs []models.HashType,
	settings models.RequestSettings) ([]models.HashType, error) {

	if len(txIDs) == 0 {
		return nil, nil
	}

	fc, release, err := db.acquireFedForRequest(settings)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer release()

	stringTxIDs := make([]string, len(txIDs))
	for i, hash := range txIDs {
		stringTxIDs[i] = string(hash)
	}

	query := `
	        SELECT DISTINCT tr.external_hash
	        FROM traces tr
	        INNER JOIN transactions tx ON tr.trace_id = tx.trace_id
	        WHERE tx.hash = ANY($1::tonhash[])
	        AND tr.external_hash IS NOT NULL`

	// point lookup by tx id: a tx may be hot or cold-only, consult both and dedupe
	fetch := func(conn *pgxpool.Conn) ([]models.HashType, error) {
		qctx, cancel := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel()
		rows, err := conn.Query(qctx, query, stringTxIDs)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()
		var out []models.HashType
		for rows.Next() {
			var hash string
			if err := rows.Scan(&hash); err != nil {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
			out = append(out, models.HashType(hash))
		}
		if err := rows.Err(); err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		return out, nil
	}
	return hotThenCold(fc, -1, fetch,
		func(h *models.HashType) models.HashType { return *h })
}
