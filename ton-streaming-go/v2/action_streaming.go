package v2

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-index-go/index/parse"
	"github.com/toncenter/ton-indexer/ton-streaming-go/observability"
)

func SubscribeToActionHints(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (action hints): %s", channel)

	pool := newKeyedWorkerPool(ctx, "actions", streamingWorkerCount, streamingQueueSizePerWorker,
		func(ctx context.Context, hint actionsHint) {
			ProcessActionHint(ctx, rdb, hint, manager, channel)
		})

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[v2] Error receiving action hint: %v", err)
			continue
		}

		hint, err := decodeActionsHint(msg.Payload)
		if err != nil {
			log.Printf("[v2] Invalid action hint: %v", err)
			continue
		}
		if !hasActionHintSubscribers(manager, hint) {
			continue
		}
		if !pool.Enqueue(ctx, hint.TraceKey.String(), hint.jobKey(), hint.priority(), hint) {
			return
		}
	}
}

func hasActionHintSubscribers(manager *ClientManager, hint actionsHint) bool {
	// Go applies a small transaction-level finality exception after loading the
	// trace. Use the most permissive finality here so routing cannot skip a
	// subscriber which that calculation would accept.
	routingFinality := indexModels.FinalityStateFinalized
	if len(manager.subscribersForTrace(hint.TraceKey, routingFinality)) != 0 {
		return true
	}
	if !hint.ActionsUpdated {
		return false
	}
	return len(manager.subscribersForActionRoutes(hint.ActionTypesAndAccounts, routingFinality)) != 0
}

func ProcessActionHint(ctx context.Context, rdb *redis.Client, hint actionsHint, manager *ClientManager, channel string) {
	// The subscription may have disappeared while this job was waiting.
	if !hasActionHintSubscribers(manager, hint) {
		return
	}

	startedAt := observability.NowUnixNano()
	rawTrace, err := rdb.HGetAll(ctx, hint.TraceKey.String()).Result()
	if err != nil {
		log.Printf("[v2] Error loading action trace %s: %v", hint.TraceKey, err)
		return
	}
	if len(rawTrace) == 0 {
		log.Printf("[v2] Action trace %s is missing from Redis", hint.TraceKey)
		return
	}
	if err := validateTraceHintVersion(rawTrace, hint.UpdateSeq); err != nil {
		if !errors.Is(err, errStaleStreamingHint) {
			log.Printf("[v2] Action hint version mismatch for %s: %v", hint.TraceKey, err)
		}
		return
	}

	// A failed classifier leaves the last good blob in Redis for the pending
	// API, but it is not a result for this update_seq and must not be streamed.
	if !hint.ActionsUpdated {
		delete(rawTrace, "actions")
	}

	stage := NewTraceProcessingStage(startedAt, actionHintSpanName, rawTrace, hint.TraceKey.String(), channel)
	traceRootHash := indexModels.HashType(rawTrace["root_node"])
	stage.Span.AddAttr("ton.trace.external_message_hash", string(traceRootHash))

	rawTraces := map[string]map[string]string{hint.TraceKey.String(): rawTrace}
	emulatedContext := crud.NewEmptyContext(false)
	if err := emulatedContext.FillFromRawData(rawTraces); err != nil {
		log.Printf("[v2] Error filling action trace %s: %v", hint.TraceKey, err)
		stage.EmitOtelError("streaming_api.fill_context_error", err.Error())
		return
	}
	if emulatedContext.GetTraceCount() != 1 {
		err := fmt.Errorf("expected one trace, got %d", emulatedContext.GetTraceCount())
		stage.EmitOtelError("streaming_api.invalid_trace_count", err.Error())
		return
	}

	traceFinality := actionTraceFinality(emulatedContext, stage)
	actions, actionsAddresses := actionsFromContext(emulatedContext)
	stage.Span.AddAttr("ton.actions.count", len(actions))
	stage.Span.AddAttr("ton.actions.has_actions", len(actions) > 0)
	stage.Span.AddAttr("ton.actions.updated", hint.ActionsUpdated)
	stage.Span.AddAttr("ton.trace.finality", traceFinality.String())

	allActionAddresses := flattenActionAddresses(actionsAddresses)
	if hint.ActionsUpdated && len(actions) != 0 {
		var addressBook *indexModels.AddressBook
		var metadata *indexModels.Metadata
		shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadata(
			[]EventType{EventActions}, traceFinality, allActionAddresses)
		if shouldFetchAddressBook || shouldFetchMetadata {
			addressBook, metadata = fetchAddressBookAndMetadata(
				ctx, allActionAddresses, allActionAddresses, shouldFetchAddressBook, shouldFetchMetadata)
		}
		actionTargets := manager.subscribersForAddresses(EventActions, allActionAddresses, traceFinality)
		manager.sendNotification(&ActionsNotification{
			Type:                  EventActions,
			Finality:              traceFinality,
			TraceExternalHashNorm: hint.TraceKey,
			Actions:               actions,
			ActionAddresses:       actionsAddresses,
			AddressBook:           addressBook,
			Metadata:              metadata,
		}, actionTargets)
	}

	traceTargets := manager.subscribersForTrace(hint.TraceKey, traceFinality)
	if len(traceTargets) == 0 {
		stage.Emit()
		return
	}

	txs, err := crud.QueryPendingTransactionsImpl(emulatedContext, nil, indexModels.RequestSettings{}, false)
	if err != nil {
		log.Printf("[v2] Error querying action trace transactions: %v", err)
		stage.EmitOtelError("streaming_api.query_transactions_error", err.Error())
		return
	}

	txOrder := make([]indexModels.HashType, 0, len(txs))
	for index := range txs {
		txOrder = append(txOrder, txs[index].Hash)
	}
	traceRoot, traceTxMap, err := buildTraceFromTransactions(txOrder, txs)
	if err != nil {
		log.Printf("[v2] Error assembling action trace %s: %v", hint.TraceKey, err)
		stage.EmitOtelError("streaming_api.build_trace_error", err.Error())
		return
	}
	if traceRoot == nil {
		stage.EmitOtelError("streaming_api.build_trace_error", "action trace root is nil")
		return
	}

	traceAddresses := traceNotificationAddresses(txs, actionsAddresses)
	var traceAddressBook *indexModels.AddressBook
	var traceMetadata *indexModels.Metadata
	shouldFetchAddressBook, shouldFetchMetadata := manager.shouldFetchAddressBookAndMetadataForTrace(
		traceFinality, hint.TraceKey)
	if shouldFetchAddressBook || shouldFetchMetadata {
		traceAddressBook, traceMetadata = fetchAddressBookAndMetadata(
			ctx, traceAddresses, traceAddresses, shouldFetchAddressBook, shouldFetchMetadata)
	}

	// A non-nil empty slice deliberately serializes as `actions: []`. This is
	// how a trace subscriber learns that classification failed or found nothing.
	manager.sendNotification(&TraceNotification{
		Type:                  EventTrace,
		Finality:              traceFinality,
		TraceExternalHashNorm: hint.TraceKey,
		Trace:                 *traceRoot,
		Transactions:          traceTxMap,
		Actions:               &actions,
		AddressBook:           traceAddressBook,
		Metadata:              traceMetadata,
	}, traceTargets)
	stage.Emit()
}

func actionTraceFinality(emulatedContext *crud.EmulatedTracesContext, stage *TraceProcessingStage) indexModels.FinalityState {
	finality := indexModels.FinalityStateFinalized
	txs := emulatedContext.GetTransactions()
	txFinality := make(map[string]indexModels.FinalityState, len(txs))
	txHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		txHash := string(tx.Hash)
		txHashes = append(txHashes, txHash)
		txFinality[txHash] = tx.Finality
		if stage.RootTxHash == "-" && tx.TraceId != nil {
			stage.SetRootTxHash(*tx.TraceId)
		}
	}

	inOpcodes := make(map[string]string, len(txFinality))
	outMessageCounts := make(map[string]int, len(txFinality))
	if len(txHashes) != 0 {
		for _, message := range emulatedContext.GetMessages(txHashes) {
			txHash := string(message.TxHash)
			if message.Direction == "in" {
				if message.Opcode != nil {
					inOpcodes[txHash] = (*message.Opcode).String()
				}
			} else {
				outMessageCounts[txHash]++
			}
		}
	}

	for txHash, txFinality := range txFinality {
		if outMessageCounts[txHash] == 0 {
			opcode := inOpcodes[txHash]
			if opcode == jettonTransferNotificationOpcode || opcode == nftOwnershipAssignedNotificationOpcode || opcode == excessesOpcode {
				continue
			}
		}
		if txFinality < finality {
			finality = txFinality
		}
	}
	return finality
}

func actionsFromContext(emulatedContext *crud.EmulatedTracesContext) ([]*indexModels.Action, [][]indexModels.AccountAddress) {
	actions := make([]*indexModels.Action, 0)
	actionAddresses := make([][]indexModels.AccountAddress, 0)
	for _, rawAction := range emulatedContext.GetAllActions() {
		addressSet := map[indexModels.AccountAddress]bool{}
		parse.CollectAddressesFromAction(&addressSet, rawAction)

		action, err := parse.ParseRawAction(rawAction)
		if err != nil {
			log.Printf("[v2] Error parsing raw action: %v", err)
			continue
		}
		// Subscriptions are matched against Action.Accounts. Keep the routing
		// addresses and the final delivery target based on the same account set.
		for _, account := range action.Accounts {
			addressSet[account] = true
		}
		addresses := make([]indexModels.AccountAddress, 0, len(addressSet))
		for address := range addressSet {
			addresses = append(addresses, address)
		}
		actions = append(actions, action)
		actionAddresses = append(actionAddresses, addresses)
	}
	return actions, actionAddresses
}

func flattenActionAddresses(actionAddresses [][]indexModels.AccountAddress) []indexModels.AccountAddress {
	var result []indexModels.AccountAddress
	for _, addresses := range actionAddresses {
		result = append(result, addresses...)
	}
	return result
}

func traceNotificationAddresses(txs []indexModels.Transaction, actionAddresses [][]indexModels.AccountAddress) []indexModels.AccountAddress {
	addressSet := map[indexModels.AccountAddress]bool{}
	for index := range txs {
		collectAddressesFromTransaction(addressSet, &txs[index])
	}
	for _, addresses := range actionAddresses {
		for _, address := range addresses {
			addressSet[address] = true
		}
	}

	result := make([]indexModels.AccountAddress, 0, len(addressSet))
	for address := range addressSet {
		result = append(result, address)
	}
	return result
}
