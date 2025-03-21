package index

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"sort"
	"strings"
)

func (db *DbClient) QueryPendingActions(
	settings RequestSettings,
	emulatedContext *EmulatedTracesContext,
) ([]Action, AddressBook, Metadata, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	raw_actions, err := queryPendingActionsImpl(emulatedContext, conn, settings)
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	var actions []Action
	book := AddressBook{}
	addr_map := map[string]bool{}
	metadata := Metadata{}

	for _, raw_action := range raw_actions {
		collectAddressesFromAction(&addr_map, &raw_action)
		action, err := ParseRawAction(&raw_action)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		actions = append(actions, *action)
	}

	if len(addr_map) > 0 && !settings.NoAddressBook {
		var addr_list []string
		for k := range addr_map {
			addr_list = append(addr_list, k)
		}
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}

		metadata, err = queryMetadataImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}

	return actions, book, metadata, nil
}

func (db *DbClient) QueryPendingTraces(settings RequestSettings, emulatedContext *EmulatedTracesContext) ([]Trace, AddressBook, Metadata, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, addr_list, err := queryPendingTracesImpl(emulatedContext, conn, settings)
	if err != nil {
		//log.Println(query)
		return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	metadata := Metadata{}

	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		metadata, err = queryMetadataImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}

	return res, book, metadata, nil
}

func (db *DbClient) QueryPendingTransactions(
	settings RequestSettings,
	emulatedContext *EmulatedTracesContext,
) ([]Transaction, AddressBook, error) {
	if emulatedContext.IsEmptyContext() {
		return nil, nil, IndexError{Code: 404, Message: "emulated traces not found"}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	txs, err := queryPendingTransactions(emulatedContext, conn, settings, true)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	var addr_list []string
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
	book := AddressBook{}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return txs, book, nil
}

func queryCompletedEmulatedTraces(emulatedContext *EmulatedTracesContext,
	conn *pgxpool.Conn, settings RequestSettings) ([]string, error) {

	trace_ids := make([]string, 0)
	for trace_id, _ := range emulatedContext.emulatedTransactions {
		trace_ids = append(trace_ids, fmt.Sprintf("'%s'", trace_id))
	}
	if len(trace_ids) > 0 {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		query := fmt.Sprintf("select DISTINCT M.msg_hash from messages M join traces T on T.trace_id = M.trace_id"+
			" where M.msg_hash in (%s) and T.state='complete'",
			strings.Join(trace_ids, ","))
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		completed_trace_ids_in_db := make([]string, 0)
		for rows.Next() {
			var trace_id string
			if err := rows.Scan(&trace_id); err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
			completed_trace_ids_in_db = append(completed_trace_ids_in_db, trace_id)
		}
		return completed_trace_ids_in_db, nil
	}
	return nil, nil
}

func queryPendingTransactions(emulatedContext *EmulatedTracesContext, conn *pgxpool.Conn, settings RequestSettings, filterCompletedTransaction bool) ([]Transaction, error) {
	var txs []Transaction
	// find transactions that already present in db
	if filterCompletedTransaction {
		// Find fully completed traces
		completed_traces, err := queryCompletedEmulatedTraces(emulatedContext, conn, settings)
		if err != nil {
			return nil, err
		}
		emulatedContext.RemoveTraces(completed_traces)

		// Filter partially completed traces
		var msg_hashes []string
		msg_hash_to_tx := make(map[string]string)
		for _, msgs := range emulatedContext.emulatedMessages {
			for _, msg := range msgs {
				msg_hash_to_tx[msg.MsgHash] = msg.TxHash
				msg_hashes = append(msg_hashes, fmt.Sprintf("'%s'", msg.MsgHash))
			}
		}
		if len(msg_hashes) > 0 {
			msg_hashes_in := strings.Join(msg_hashes, ",")
			query := fmt.Sprintf(`select msg_hash from messages where msg_hash in (%s) and direction = 'in'`, msg_hashes_in)
			var present_msg_hashes []string
			ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
			defer cancel_ctx()
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
			defer rows.Close()
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err != nil {
					return nil, IndexError{Code: 500, Message: err.Error()}
				}
				present_msg_hashes = append(present_msg_hashes, s)
				println(s)
			}
			var tx_hashes_to_remove []string
			for _, msg_hash := range present_msg_hashes {
				tx_hashes_to_remove = append(tx_hashes_to_remove, msg_hash_to_tx[msg_hash])
			}
			emulatedContext.RemoveTransactions(tx_hashes_to_remove)
		}
	}
	txs_map := map[HashType]int{}
	{
		rows := emulatedContext.GetTransactions()
		for _, row := range rows {
			if tx, err := ScanTransaction(row); err == nil {
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}

	var hash_list []string
	for _, t := range txs {
		hash_list = append(hash_list, string(t.Hash))
	}
	if len(txs) == 0 {
		return txs, nil
	}
	if len(hash_list) > 0 {
		rows := emulatedContext.GetMessages(hash_list)
		for _, row := range rows {
			msg, err := ScanMessageWithContent(row)
			if err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
			if msg.Direction == "in" {
				txs[txs_map[msg.TxHash]].InMsg = msg
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, msg)
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

func queryPendingTracesImpl(emulatedContext *EmulatedTracesContext, conn *pgxpool.Conn, settings RequestSettings) ([]Trace, []string, error) {
	var traces []Trace
	trace_ids := make([]string, 0)
	for trace_id, _ := range emulatedContext.emulatedTransactions {
		trace_ids = append(trace_ids, fmt.Sprintf("'%s'", trace_id))
	}
	if len(trace_ids) > 0 {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		query := fmt.Sprintf("select DISTINCT M.msg_hash from messages M join traces T on T.trace_id = M.trace_id where M.msg_hash in (%s) and T.state='complete'",
			strings.Join(trace_ids, ","))
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		completed_trace_ids_in_db := make([]string, 0)
		for rows.Next() {
			var trace_id string
			if err := rows.Scan(&trace_id); err != nil {
				return nil, nil, IndexError{Code: 500, Message: err.Error()}
			}
			completed_trace_ids_in_db = append(completed_trace_ids_in_db, trace_id)
		}
		emulatedContext.RemoveTraces(completed_trace_ids_in_db)
	}
	traceRows := emulatedContext.GetTraces()
	for _, row := range traceRows {
		if loc, err := ScanTrace(row); err == nil {
			loc.Transactions = make(map[HashType]*Transaction)
			actions := make([]*Action, 0)
			loc.Actions = &actions
			traces = append(traces, *loc)
		} else {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	events_map := map[HashType]int{}
	var trace_id_list []HashType
	addr_map := map[string]bool{}
	for idx := range traces {
		events_map[traces[idx].TraceId] = idx
		trace_id_list = append(trace_id_list, traces[idx].TraceId)
	}
	if len(trace_id_list) > 0 {
		{
			txs, err := queryPendingTransactions(emulatedContext, conn, settings, false)
			if err != nil {
				return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed query transactions: %s", err.Error())}
			}
			for idx := range txs {
				tx := &txs[idx]

				collectAddressesFromTransactions(&addr_map, tx)
				if v := tx.TraceId; v != nil {
					event := &traces[events_map[*v]]
					event.TransactionsOrder = append(event.TransactionsOrder, tx.Hash)
					event.Transactions[tx.Hash] = tx
				}
			}
		}
	}
	for idx := range traces {
		if len(traces[idx].TransactionsOrder) > 0 {
			trace, err := assembleTraceTxsFromMap(&traces[idx].TransactionsOrder, &traces[idx].Transactions)
			if err != nil {
				if len(traces[idx].Warning) > 0 {
					traces[idx].Warning += ", " + err.Error()
				} else {
					traces[idx].Warning = err.Error()
				}
			}
			if trace != nil {
				traces[idx].Trace = trace
			}
		}
	}
	actions := make([]RawAction, 0)
	for _, row := range emulatedContext.GetActions() {
		if loc, err := ScanRawAction(row); err == nil {
			actions = append(actions, *loc)
		} else {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	for idx := range actions {
		raw_action := &actions[idx]

		action, err := ParseRawAction(raw_action)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to parse action: %s", err.Error())}
		}
		*traces[events_map[action.TraceId]].Actions = append(*traces[events_map[action.TraceId]].Actions, action)
	}
	//
	var addr_list []string
	for k := range addr_map {
		addr_list = append(addr_list, k)
	}
	//
	return traces, addr_list, nil
}

func queryPendingActionsImpl(
	emulatedContext *EmulatedTracesContext,
	conn *pgxpool.Conn,
	settings RequestSettings,
) ([]RawAction, error) {
	completed_traces, err := queryCompletedEmulatedTraces(emulatedContext, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	emulatedContext.RemoveTraces(completed_traces)

	var raw_actions []RawAction
	for _, actions := range emulatedContext.GetActions() {
		if action, err := ScanRawAction(actions); err == nil {
			raw_actions = append(raw_actions, *action)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}

	return raw_actions, nil
}
