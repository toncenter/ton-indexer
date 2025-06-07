package index

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/toncenter/ton-indexer/ton-index-go/index/emulated"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"log"
	"sort"
	"strconv"
)

type EmulatedTracesContext struct {
	emulatedTransactionsRaw   map[string]map[string]string
	emulatedTraces            map[string]*models.Trace
	emulatedActions           map[string][]*models.RawAction
	emulatedTransactions      map[string][]*models.Transaction
	emulatedMessageContents   map[string]*models.MessageContent
	emulatedMessageInitStates map[string]*models.MessageContent
	emulatedMessages          map[string][]*models.Message
	traceKeys                 []string
	txHashTraceExternalHash   map[string]string
	emulatedOnly              bool
}

func NewEmptyContext(emulated_only bool) *EmulatedTracesContext {
	return &EmulatedTracesContext{
		emulatedTraces:            make(map[string]*models.Trace),
		emulatedActions:           make(map[string][]*models.RawAction),
		emulatedTransactionsRaw:   make(map[string]map[string]string),
		emulatedTransactions:      make(map[string][]*models.Transaction),
		emulatedMessageContents:   make(map[string]*models.MessageContent),
		emulatedMessageInitStates: make(map[string]*models.MessageContent),
		emulatedMessages:          make(map[string][]*models.Message),
		traceKeys:                 make([]string, 0),
		txHashTraceExternalHash:   make(map[string]string),
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

func (c *EmulatedTracesContext) RemoveTraces(trace_keys []string) {
	for _, trace_key := range trace_keys {
		delete(c.emulatedTransactions, trace_key)
		delete(c.emulatedMessages, trace_key)
		delete(c.emulatedTraces, trace_key)
		delete(c.emulatedActions, trace_key)
	}
}

func (c *EmulatedTracesContext) GetTransactionsByTraceIdAndHash(trace_id models.HashType, tx_hashes []*models.HashType) []*models.Transaction {
	txs := make([]*models.Transaction, 0)
	for _, tx_hash := range tx_hashes {
		for _, tx := range c.emulatedTransactions[string(trace_id)] {
			if tx.Hash == *tx_hash {
				txs = append(txs, tx)
			}
		}
	}
	return txs
}

func (c *EmulatedTracesContext) FilterTraceActions(trace_action_map map[string][]string) {
	for trace_id, action_ids := range trace_action_map {
		filtered_actions := make([]*models.RawAction, 0)
		for _, action_id := range action_ids {
			for _, action := range c.emulatedActions[trace_id] {
				if action.ActionId == models.HashType(action_id) {
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
			filtered_txs := make([]*models.Transaction, 0)
			for _, tx := range txs {
				if tx.Hash != models.HashType(tx_hash) {
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

func (c *EmulatedTracesContext) GetTransactions() []*models.Transaction {
	txs := make([]*models.Transaction, 0)
	for _, etxs := range c.emulatedTransactions {
		txs = append(txs, etxs...)
	}
	return txs
}

func (c *EmulatedTracesContext) GetTraces() []*models.Trace {
	traces := make([]*models.Trace, 0)
	for _, trace := range c.emulatedTraces {
		traces = append(traces, trace)
	}
	return traces
}

func (c *EmulatedTracesContext) GetRawActions(supportedActions []string) []*models.RawAction {
	rows := make([]*models.RawAction, 0)
	supportedActionsSet := mapset.NewSet(supportedActions...)
	for _, traceKey := range c.traceKeys {
		if actions, ok := c.emulatedActions[traceKey]; ok {
			sort.Slice(actions, func(i, j int) bool {
				return actions[i].EndLt > actions[j].EndLt
			})
			for _, action := range actions {
				if supportedActionsSet.ContainsAny(action.AncestorType...) {
					continue
				}
				if !supportedActionsSet.ContainsAny(action.Type) {
					continue
				}

				rows = append(rows, action)
			}
		}
	}
	return rows
}

func (c *EmulatedTracesContext) GetMessages(transaction_hashes []string) []*models.Message {
	rows := make([]*models.Message, 0)
	for _, tx_hash := range transaction_hashes {
		for _, msg := range c.emulatedMessages[tx_hash] {
			rows = append(rows, msg)
		}
	}
	return rows
}

func (c *EmulatedTracesContext) FillFromRawData(rawData map[string]map[string]string) error {
	for k, v := range rawData {
		c.emulatedTransactionsRaw[k] = v
	}
	for traceKey := range rawData {
		var message_count uint16 = 0
		var transaction_count uint16 = 0
		var pending_messages uint16 = 0
		trace, err := emulated.ConvertHSet(c.emulatedTransactionsRaw[traceKey], traceKey)
		if err != nil {
			log.Printf("error while converting hset: %s", err.Error())
			continue
		}
		c.traceKeys = append(c.traceKeys, traceKey)
		externalHash := models.HashType(trace.ExternalHash)
		traceModel := models.Trace{
			TraceId:      (*models.HashType)(trace.TraceId),
			ExternalHash: &externalHash,
			TraceMeta: models.TraceMeta{
				TraceState: "pending",
			},
		}

		if trace.Classified {
			traceModel.TraceMeta.ClassificationState = "ok"
		} else {
			traceModel.TraceMeta.ClassificationState = "unclassified"
		}

		var maxLt uint64 = 0
		var maxUtime uint32 = 0
		var maxMcSeqno uint32 = 0

		for _, node := range trace.Nodes {
			var should_save = true
			if c.emulatedOnly && !node.Emulated {
				should_save = false
			}
			transaction, err := node.GetTransaction()
			if err != nil {
				return err
			}

			if node.Key == trace.ExternalHash {
				traceModel.StartLt = uint64(transaction.Lt)
				traceModel.StartUtime = uint32(transaction.Now)
				traceModel.McSeqnoStart = models.HashType(strconv.Itoa(int(transaction.McSeqno)))
			}

			maxLt = max(maxLt, uint64(transaction.Lt))
			maxUtime = max(maxUtime, uint32(transaction.Now))
			maxMcSeqno = max(maxMcSeqno, uint32(transaction.McSeqno))

			if should_save {
				c.emulatedTransactions[traceKey] = append(c.emulatedTransactions[traceKey], &transaction)
				c.txHashTraceExternalHash[string(transaction.Hash)] = trace.ExternalHash
			}
			messages, contents, initStates, err := node.GetMessages()
			if err != nil {
				return err
			}
			for _, msg := range messages {
				if node.Key == traceKey && msg.Source == nil {
					traceModel.ExternalHash = (*models.HashType)(&msg.MsgHash)
				}
				if should_save {
					c.emulatedMessages[string(transaction.Hash)] = append(c.emulatedMessages[string(transaction.Hash)], &msg)
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
		traceModel.EndLt = &maxLt
		traceModel.EndUtime = &maxUtime
		traceModel.McSeqnoEnd = models.HashType(strconv.Itoa(int(maxMcSeqno)))
		traceModel.TraceMeta.Transactions = int64(transaction_count)
		traceModel.TraceMeta.Messages = int64(message_count)
		traceModel.TraceMeta.PendingMessages = int64(pending_messages)
		c.emulatedTraces[traceKey] = &traceModel
		for _, a := range trace.Actions {
			row, err := a.ToRawAction()
			if err != nil {
				return err
			}
			c.emulatedActions[traceKey] = append(c.emulatedActions[traceKey], row)
		}
	}
	sort.Slice(c.traceKeys, func(i, j int) bool {
		trace1, trace1Exists := c.emulatedTraces[c.traceKeys[i]]
		trace2, trace2Exists := c.emulatedTraces[c.traceKeys[j]]
		if trace1Exists && trace2Exists && trace1.EndLt != nil && trace2.EndLt != nil {
			return *trace1.EndLt > *trace2.EndLt
		} else {
			return true
		}
	})
	return nil
}

func (c *EmulatedTracesContext) FilterTransactionsByAccounts(accounts []models.AccountAddress) error {
	for k, txs := range c.emulatedTransactions {
		filtered_txs := make([]*models.Transaction, 0)
		for _, tx := range txs {
			is_requested_account := false

			for _, account := range accounts {
				if tx.Account == account {
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
