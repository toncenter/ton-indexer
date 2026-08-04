package v2

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/redis/go-redis/v9"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-streaming-go/observability"
)

func SubscribeToTransactionHints(ctx context.Context, rdb *redis.Client, manager *ClientManager, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	log.Printf("[v2] Subscribed to Redis channel (transaction hints): %s", channel)

	pool := newKeyedWorkerPool(ctx, "transactions", streamingWorkerCount, streamingQueueSizePerWorker,
		func(ctx context.Context, hint transactionHint) {
			ProcessTransactionHint(ctx, rdb, hint, manager, channel)
		})

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[v2] Error receiving transaction hint: %v", err)
			continue
		}

		hint, err := decodeTransactionHint(msg.Payload)
		if err != nil {
			log.Printf("[v2] Invalid transaction hint: %v", err)
			continue
		}
		if !hasTransactionSubscribers(manager, hint) {
			continue
		}
		if !pool.Enqueue(ctx, hint.TraceKey.String(), hint.jobKey(), hint.priority(), hint) {
			return
		}
	}
}

func hasTransactionSubscribers(manager *ClientManager, hint transactionHint) bool {
	return len(manager.subscribersForAddresses(EventTransactions, hint.Accounts, hint.TraceFinality)) > 0
}

func ProcessTransactionHint(ctx context.Context, rdb *redis.Client, hint transactionHint, manager *ClientManager, channel string) {
	// Subscriptions may have changed while the hint waited in the queue.
	if !hasTransactionSubscribers(manager, hint) {
		return
	}

	startTimeUnix := observability.NowUnixNano()
	rawTrace, err := rdb.HGetAll(ctx, hint.TraceKey.String()).Result()
	if err != nil {
		log.Printf("[v2] Error loading transaction trace %s: %v", hint.TraceKey, err)
		return
	}
	if err := validateTraceHintVersion(rawTrace, hint.UpdateSeq); err != nil {
		if !errors.Is(err, errStaleStreamingHint) {
			log.Printf("[v2] Transaction hint version mismatch for %s: %v", hint.TraceKey, err)
		}
		return
	}

	stage := NewTraceProcessingStage(startTimeUnix, rawTraceSpanName, rawTrace, hint.TraceKey.String(), channel)
	rawTraces := map[string]map[string]string{
		hint.TraceKey.String(): rawTrace,
	}
	emulatedContext := crud.NewEmptyContext(false)
	if err := emulatedContext.FillFromRawData(rawTraces); err != nil {
		log.Printf("[v2] Error filling transaction trace %s (%s update): %v", hint.TraceKey, hint.UpdateFinality, err)
		stage.EmitOtelError("streaming_api.fill_context_error", err.Error())
		return
	}
	if emulatedContext.GetTraceCount() != 1 {
		err := fmt.Errorf("expected one trace, got %d", emulatedContext.GetTraceCount())
		stage.EmitOtelError("streaming_api.invalid_trace_count", err.Error())
		return
	}

	txs, txIndexes, transactionAccounts, traceFinality := transactionsForHint(emulatedContext, hint, stage)
	if len(txs) == 0 {
		stage.Span.AddAttr("ton.streaming.skipped", true)
		stage.Span.AddAttr("ton.streaming.skip_reason", "no_matching_transactions")
		stage.Emit()
		return
	}
	if traceFinality != hint.TraceFinality {
		err := fmt.Errorf("hint trace_finality is %s, Redis snapshot finality is %s", hint.TraceFinality, traceFinality)
		log.Printf("[v2] Transaction hint finality mismatch for %s: %v", hint.TraceKey, err)
		stage.EmitOtelError("streaming_api.finality_mismatch", err.Error())
		return
	}

	targets := manager.subscribersForAddresses(EventTransactions, transactionAccounts, traceFinality)
	if len(targets) == 0 {
		stage.Span.AddAttr("ton.streaming.skipped", true)
		stage.Span.AddAttr("ton.streaming.skip_reason", "subscription_disappeared")
		stage.Emit()
		return
	}

	allAddresses := attachTransactionMessages(emulatedContext, hint, txs, txIndexes)
	var addressBook *indexModels.AddressBook
	var metadata *indexModels.Metadata
	shouldFetchAddressBook, shouldFetchMetadata := manager.enrichmentNeeds(targets)
	if shouldFetchAddressBook || shouldFetchMetadata {
		addressBook, metadata = fetchAddressBookAndMetadata(ctx, allAddresses, allAddresses, shouldFetchAddressBook, shouldFetchMetadata)
	}

	sortTransactions(txs)
	stage.Span.AddAttr("ton.trace.finality", traceFinality.String())
	stage.Span.AddAttr("ton.transactions.count", len(txs))
	manager.sendNotification(&TransactionsNotification{
		Type:                  EventTransactions,
		Finality:              traceFinality,
		TraceExternalHashNorm: hint.TraceKey,
		UpdateSeq:             hint.UpdateSeq,
		UpdateFinality:        hint.UpdateFinality,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}, targets)
	stage.Emit()
}

func transactionsForHint(
	emulatedContext *crud.EmulatedTracesContext,
	hint transactionHint,
	stage *TraceProcessingStage,
) (
	[]indexModels.Transaction,
	map[indexModels.HashType]int,
	[]indexModels.AccountAddress,
	indexModels.FinalityState,
) {
	txs := make([]indexModels.Transaction, 0)
	txIndexes := make(map[indexModels.HashType]int)
	accounts := make([]indexModels.AccountAddress, 0)
	finality := indexModels.FinalityStateFinalized

	for _, tx := range emulatedContext.GetTransactions() {
		if hint.UpdateFinality == indexModels.FinalityStatePending && tx.Finality != indexModels.FinalityStatePending {
			continue
		}
		txs = append(txs, *tx)
		txIndexes[tx.Hash] = len(txs) - 1
		accounts = append(accounts, tx.Account)
		if tx.Finality < finality {
			finality = tx.Finality
		}
		if stage.RootTxHash == "-" && tx.TraceId != nil {
			stage.SetRootTxHash(*tx.TraceId)
		}
	}
	return txs, txIndexes, accounts, finality
}

func attachTransactionMessages(emulatedContext *crud.EmulatedTracesContext, hint transactionHint, txs []indexModels.Transaction,
	txIndexes map[indexModels.HashType]int) []indexModels.AccountAddress {
	hashes := make([]string, 0, len(txs))
	addresses := make([]indexModels.AccountAddress, 0, len(txs)*3)
	for _, tx := range txs {
		hashes = append(hashes, string(tx.Hash))
		addresses = append(addresses, tx.Account)
	}

	messages := emulatedContext.GetMessages(hashes)
	messagePointers := make([]*indexModels.Message, 0, len(messages))
	for _, msg := range messages {
		txIndex, ok := txIndexes[msg.TxHash]
		if !ok {
			log.Printf("[v2] Message for unknown transaction (%s update), tx hash: %s", hint.UpdateFinality, msg.TxHash)
			continue
		}
		messagePointers = append(messagePointers, msg)
		if msg.Direction == "in" {
			txs[txIndex].InMsg = msg
			if msg.Source != nil {
				addresses = append(addresses, *msg.Source)
			}
		} else {
			txs[txIndex].OutMsgs = append(txs[txIndex].OutMsgs, msg)
			if msg.Destination != nil {
				addresses = append(addresses, *msg.Destination)
			}
		}
	}

	if hint.UpdateFinality == indexModels.FinalityStatePending {
		if err := detect.MarkMessagesByPtr(messagePointers); err != nil {
			log.Printf("[v2] Error marking pending messages for %s: %v", hint.TraceKey, err)
		}
	}
	return addresses
}

func sortTransactions(txs []indexModels.Transaction) {
	for index := range txs {
		sort.SliceStable(txs[index].OutMsgs, func(i, j int) bool {
			if txs[index].OutMsgs[i].CreatedLt == nil {
				return true
			}
			if txs[index].OutMsgs[j].CreatedLt == nil {
				return false
			}
			return *txs[index].OutMsgs[i].CreatedLt < *txs[index].OutMsgs[j].CreatedLt
		})
	}
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Lt > txs[j].Lt
	})
}
