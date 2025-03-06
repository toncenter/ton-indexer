package index

import (
	"github.com/jackc/pgx/v5"
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
	"log"
)

type EmulatedTracesContext struct {
	emulatedTransactionsRaw   map[string]map[string]string
	emulatedTraces            map[string]*emulated.TraceRow
	emulatedActions           map[string][]*emulated.ActionRow
	emulatedTransactions      map[string][]*emulated.TransactionRow
	emulatedMessageContents   map[string]*emulated.MessageContentRow
	emulatedMessageInitStates map[string]*emulated.MessageContentRow
	emulatedMessages          map[string][]*emulated.MessageRow
	traceIds                  []string
	emulatedOnly              bool
}

func NewEmptyContext(emulated_only bool) *EmulatedTracesContext {
	return &EmulatedTracesContext{
		emulatedTraces:            make(map[string]*emulated.TraceRow),
		emulatedActions:           make(map[string][]*emulated.ActionRow),
		emulatedTransactionsRaw:   make(map[string]map[string]string),
		emulatedTransactions:      make(map[string][]*emulated.TransactionRow),
		emulatedMessageContents:   make(map[string]*emulated.MessageContentRow),
		emulatedMessageInitStates: make(map[string]*emulated.MessageContentRow),
		emulatedMessages:          make(map[string][]*emulated.MessageRow),
		traceIds:                  make([]string, 0),
		emulatedOnly:              emulated_only,
	}
}

func (c *EmulatedTracesContext) SetEmulatedOnly(emulatedOnly bool) {
	c.emulatedOnly = emulatedOnly
}
func (c *EmulatedTracesContext) IsEmulatedOnly() bool {
	return c.emulatedOnly
}
func (c *EmulatedTracesContext) IsEmptyContext() bool {
	return len(c.emulatedTransactions) == 0
}

func (c *EmulatedTracesContext) RemoveTraces(trace_ids []string) {
	for _, trace_id := range trace_ids {
		delete(c.emulatedTransactions, trace_id)
		delete(c.emulatedMessages, trace_id)
		delete(c.emulatedTraces, trace_id)
		delete(c.emulatedActions, trace_id)
	}
}

func (c *EmulatedTracesContext) GetTransactionsByTraceIdAndHash(trace_id HashType, tx_hashes []*HashType) []*emulated.TransactionRow {
	txs := make([]*emulated.TransactionRow, 0)
	for _, tx_hash := range tx_hashes {
		for _, tx := range c.emulatedTransactions[string(trace_id)] {
			if tx.Hash == string(*tx_hash) {
				txs = append(txs, tx)
			}
		}
	}
	return txs
}

func (c *EmulatedTracesContext) FilterTraceActions(trace_action_map map[string][]string) {
	for trace_id, action_ids := range trace_action_map {
		filtered_actions := make([]*emulated.ActionRow, 0)
		for _, action_id := range action_ids {
			for _, action := range c.emulatedActions[trace_id] {
				if action.ActionId == action_id {
					filtered_actions = append(filtered_actions, action)
				}
			}
		}
		c.emulatedActions[trace_id] = filtered_actions
	}
}

func (c *EmulatedTracesContext) RemoveTransactions(transaction_hashes []string) {
	for _, tx_hash := range transaction_hashes {
		for trace_id, txs := range c.emulatedTransactions {
			filtered_txs := make([]*emulated.TransactionRow, 0)
			for _, tx := range txs {
				if tx.Hash != tx_hash {
					filtered_txs = append(filtered_txs, tx)
				}
			}
			if len(filtered_txs) > 0 {
				c.emulatedTransactions[trace_id] = filtered_txs
			} else {
				delete(c.emulatedTransactions, trace_id)
				delete(c.emulatedMessages, trace_id)
			}
		}
	}
}

func (c *EmulatedTracesContext) GetTransactions() []pgx.Row {
	rows := make([]pgx.Row, 0)
	for _, txs := range c.emulatedTransactions {
		for _, tx := range txs {
			rows = append(rows, emulated.NewRow(tx))
		}
	}
	return rows
}

func (c *EmulatedTracesContext) GetTraces() []pgx.Row {
	rows := make([]pgx.Row, 0)
	for _, trace := range c.emulatedTraces {
		rows = append(rows, emulated.NewRow(trace))
	}
	return rows
}

func (c *EmulatedTracesContext) GetActions() []pgx.Row {
	rows := make([]pgx.Row, 0)
	for _, actions := range c.emulatedActions {
		for _, action := range actions {
			rows = append(rows, emulated.NewRow(action))
		}
	}
	return rows
}

func (c *EmulatedTracesContext) GetMessages(transaction_hashes []string) []pgx.Row {
	rows := make([]pgx.Row, 0)
	for _, tx_hash := range transaction_hashes {
		for _, msg := range c.emulatedMessages[tx_hash] {
			var init_state *emulated.MessageContentRow
			var message_content *emulated.MessageContentRow
			if msg.InitStateHash != nil {
				if state, ok := c.emulatedMessageInitStates[*msg.InitStateHash]; ok {
					init_state = state
				}
			}
			if content, ok := c.emulatedMessageContents[msg.MsgHash]; ok {
				message_content = content
			}
			if init_state == nil {
				rows = append(rows, emulated.NewRow(msg, message_content))
			} else {
				rows = append(rows, emulated.NewRow(msg, message_content, init_state))
			}
		}
	}
	return rows
}

func (c *EmulatedTracesContext) FillFromRawData(rawData map[string]map[string]string) error {
	for k, v := range rawData {
		c.emulatedTransactionsRaw[k] = v
	}
	for trace_id := range rawData {
		message_count := 0
		transaction_count := 0
		pending_messages := 0
		trace, err := emulated.ConvertHSet(c.emulatedTransactionsRaw[trace_id], trace_id)
		if err != nil {
			log.Printf("error while converting hset: %s", err.Error())
			continue
		}
		c.traceIds = append(c.traceIds, trace_id)

		trace_row := emulated.TraceRow{
			TraceId:      trace_id,
			McSeqnoStart: 0,
			McSeqnoEnd:   nil,
			EndLt:        nil,
			EndUtime:     nil,
			TraceState:   "pending",
		}
		if trace.Classified {
			trace_row.ClassificationState = "ok"
		} else {
			trace_row.ClassificationState = "unclassified"
		}

		for _, node := range trace.Nodes {
			var should_save = true
			if c.emulatedOnly && !node.Emulated {
				should_save = false
			}
			transactionRow, err := node.GetTransactionRow()
			if err != nil {
				return err
			}

			if node.Key == trace_id {
				trace_row.StartLt = transactionRow.Lt
				trace_row.StartUtime = *transactionRow.Now
			}
			if should_save {
				c.emulatedTransactions[trace_id] = append(c.emulatedTransactions[trace_id], &transactionRow)
			}
			messages, contents, initStates, err := node.GetMessages()
			if err != nil {
				return err
			}
			for _, msg := range messages {
				if node.Key == trace_id && msg.Source == nil {
					trace_row.ExternalHash = &msg.MsgHash
				}
				if should_save {
					c.emulatedMessages[transactionRow.Hash] = append(c.emulatedMessages[transactionRow.Hash], &msg)
				}
				// count all out messages + external in
				if msg.Source == nil || msg.Direction == "out" {
					message_count++
					if node.Emulated {
						pending_messages++
					}
				}
			}
			if should_save {
				for k, content := range contents {
					c.emulatedMessageContents[k] = &content
				}
				for k, state := range initStates {
					c.emulatedMessageInitStates[k] = &state
				}
			}
			transaction_count++

		}
		trace_row.Transactions = int64(transaction_count)
		trace_row.Messages = int64(message_count)
		trace_row.PendingMessages = int64(pending_messages)
		c.emulatedTraces[trace_id] = &trace_row
		for _, a := range trace.Actions {
			row, err := a.GetActionRow()
			if err != nil {
				return err
			}
			c.emulatedActions[trace_id] = append(c.emulatedActions[trace_id], &row)
		}
	}
	return nil
}

func (c *EmulatedTracesContext) FilterTransactionsByAccounts(accounts []AccountAddress) error {
	for k, txs := range c.emulatedTransactions {
		filtered_txs := make([]*emulated.TransactionRow, 0)
		for _, tx := range txs {
			is_requested_account := false

			for _, account := range accounts {
				if tx.Account == account.String() {
					is_requested_account = true
					break
				}
			}

			if is_requested_account {
				filtered_txs = append(filtered_txs, tx)
			}
		}
		if len(filtered_txs) > 0 {
			c.emulatedTransactions[k] = filtered_txs
		} else {
			delete(c.emulatedTransactions, k)
		}
	}
	return nil
}
