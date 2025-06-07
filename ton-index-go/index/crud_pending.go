package index

import (
	"context"
	"fmt"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func (db *DbClient) QueryPendingActions(settings RequestSettings, emulatedContext *EmulatedTracesContext, request PendingActionsRequest) ([]models.Action, models.AddressBook, models.Metadata, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	raw_actions, err := queryPendingActionsImpl(emulatedContext, conn, settings, request)
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	actions := []models.Action{}
	book := models.AddressBook{}
	addr_map := map[string]bool{}
	metadata := models.Metadata{}

	for _, raw_action := range raw_actions {
		CollectAddressesFromAction(&addr_map, &raw_action)
		action, err := ParseRawAction(&raw_action)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		actions = append(actions, *action)
	}

	if len(addr_map) > 0 && !settings.NoAddressBook {
		var addr_list []string
		for k := range addr_map {
			addr_list = append(addr_list, k)
		}
		book, err = QueryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}

		metadata, err = QueryMetadataImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	return actions, book, metadata, nil
}

func (db *DbClient) QueryPendingTraces(settings RequestSettings, emulatedContext *EmulatedTracesContext, request PendingTracesRequest) ([]models.Trace, models.AddressBook, models.Metadata, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, addr_list, err := queryPendingTracesImpl(emulatedContext, conn, settings, request)
	if err != nil {
		//log.Println(query)
		return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}

	book := models.AddressBook{}
	metadata := models.Metadata{}

	if len(addr_list) > 0 {
		book, err = QueryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		metadata, err = QueryMetadataImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}

	return res, book, metadata, nil
}

func (db *DbClient) QueryPendingTransactions(
	settings RequestSettings,
	emulatedContext *EmulatedTracesContext,
) ([]models.Transaction, models.AddressBook, error) {
	if emulatedContext.IsEmptyContext() {
		return nil, nil, models.IndexError{Code: 404, Message: "emulated traces not found"}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	txs, err := QueryPendingTransactionsImpl(emulatedContext, conn, settings, true)
	if err != nil {
		return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
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
	book := models.AddressBook{}
	if len(addr_list) > 0 {
		book, err = QueryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: err.Error()}
		}
	}
	return txs, book, nil
}

func queryCompletedEmulatedTraces(emulatedContext *EmulatedTracesContext,
	conn *pgxpool.Conn, settings RequestSettings, classified_only bool) ([]string, error) {
	external_hash_map := make(map[string]string)
	trace_external_hashes := make([]string, 0)
	for traceKey, trace := range emulatedContext.emulatedTraces {
		if trace.ExternalHash == nil {
			continue
		}
		external_hash_map[string(*trace.ExternalHash)] = traceKey
		trace_external_hashes = append(trace_external_hashes, fmt.Sprintf("'%s'", *trace.ExternalHash))
	}
	if len(trace_external_hashes) > 0 {
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		query := "select DISTINCT M.msg_hash from messages M join traces T on T.trace_id = M.trace_id"
		if classified_only {
			query += " join blocks_classified BC on BC.mc_seqno = T.mc_seqno_end"
		}
		query += fmt.Sprintf(" where M.msg_hash in (%s) and T.state='complete'",
			strings.Join(trace_external_hashes, ","))
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, models.IndexError{Code: 500, Message: err.Error()}
		}
		completed_trace_ids_in_db := make([]string, 0)
		for rows.Next() {
			var external_hash string
			if err := rows.Scan(&external_hash); err != nil {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
			completed_trace_ids_in_db = append(completed_trace_ids_in_db, external_hash_map[external_hash])
		}
		return completed_trace_ids_in_db, nil
	}
	return nil, nil
}

func QueryPendingTransactionsImpl(emulatedContext *EmulatedTracesContext, conn *pgxpool.Conn, settings RequestSettings, filterCompletedTransaction bool) ([]models.Transaction, error) {
	var txs []models.Transaction
	// find transactions that already present in db
	if filterCompletedTransaction {
		// Find fully completed traces
		completed_traces, err := queryCompletedEmulatedTraces(emulatedContext, conn, settings, false)
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
				return nil, models.IndexError{Code: 500, Message: err.Error()}
			}
			defer rows.Close()
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err != nil {
					return nil, models.IndexError{Code: 500, Message: err.Error()}
				}
				present_msg_hashes = append(present_msg_hashes, s)
			}
			var tx_hashes_to_remove []string
			for _, msg_hash := range present_msg_hashes {
				tx_hashes_to_remove = append(tx_hashes_to_remove, msg_hash_to_tx[msg_hash])
			}
			emulatedContext.RemoveTransactions(tx_hashes_to_remove)
		}
	}
	txs_map := map[models.HashType]int{}
	{
		rows := emulatedContext.GetTransactions()
		for _, row := range rows {
			if tx, err := ScanTransaction(row); err == nil {
				if external_hash, ok := emulatedContext.txHashTraceExternalHash[string(tx.Hash)]; ok {
					hash := models.HashType(external_hash)
					tx.TraceExternalHash = &hash
				}
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				return nil, models.IndexError{Code: 500, Message: err.Error()}
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
				return nil, models.IndexError{Code: 500, Message: err.Error()}
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

func queryPendingTracesImpl(emulatedContext *EmulatedTracesContext, conn *pgxpool.Conn, settings RequestSettings, request PendingTracesRequest) ([]models.Trace, []string, error) {
	var traces []models.Trace
	completed_traces, err := queryCompletedEmulatedTraces(emulatedContext, conn, settings, true)
	if err != nil {
		return nil, nil, err
	}
	emulatedContext.RemoveTraces(completed_traces)
	traceRows := emulatedContext.GetTraces()
	for _, trace := range traceRows {
		trace.Transactions = make(map[models.HashType]*models.Transaction)
		actions := make([]*models.Action, 0)
		trace.Actions = &actions
		traces = append(traces, *trace)
	}
	events_map := map[models.HashType]int{}
	var trace_id_list []models.HashType
	addr_map := map[string]bool{}
	for idx := range traces {
		events_map[*traces[idx].ExternalHash] = idx
		trace_id_list = append(trace_id_list, *traces[idx].ExternalHash)
	}
	fully_emulated_traces := make(map[models.HashType]bool)

	if len(trace_id_list) > 0 {
		txs, err := QueryPendingTransactionsImpl(emulatedContext, conn, settings, false)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed query transactions: %s", err.Error())}
		}
		for idx := range txs {
			tx := &txs[idx]
			collectAddressesFromTransactions(&addr_map, tx)
			if v := tx.TraceExternalHash; v != nil {
				trace := &traces[events_map[*v]]
				trace.TransactionsOrder = append(trace.TransactionsOrder, tx.Hash)
				trace.Transactions[tx.Hash] = tx
				if idx == 0 && tx.Emulated {
					fully_emulated_traces[*trace.ExternalHash] = true
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
			}
			if trace != nil {
				traces[idx].Trace = trace
			}
		}
	}
	var addr_list []string
	actions := make([]models.RawAction, 0)
	for _, rawAction := range emulatedContext.GetRawActions(request.SupportedActionTypes) {
		actions = append(actions, *rawAction)
	}
	for idx := range actions {
		raw_action := &actions[idx]
		CollectAddressesFromAction(&addr_map, raw_action)

		action, err := ParseRawAction(raw_action)
		if err != nil {
			return nil, nil, models.IndexError{Code: 500, Message: fmt.Sprintf("failed to parse action: %s", err.Error())}
		}
		*traces[events_map[*action.TraceExternalHash]].Actions = append(*traces[events_map[*action.TraceExternalHash]].Actions, action)
	}
	//
	for k := range addr_map {
		addr_list = append(addr_list, k)
	}
	//
	return traces, addr_list, nil
}

func queryPendingActionsImpl(emulatedContext *EmulatedTracesContext, conn *pgxpool.Conn, settings RequestSettings, request PendingActionsRequest) ([]models.RawAction, error) {
	completed_traces, err := queryCompletedEmulatedTraces(emulatedContext, conn, settings, true)
	if err != nil {
		return nil, models.IndexError{Code: 500, Message: err.Error()}
	}
	emulatedContext.RemoveTraces(completed_traces)

	var raw_actions []models.RawAction
	for _, action := range emulatedContext.GetRawActions(request.SupportedActionTypes) {
		raw_actions = append(raw_actions, *action)
	}

	return raw_actions, nil
}
