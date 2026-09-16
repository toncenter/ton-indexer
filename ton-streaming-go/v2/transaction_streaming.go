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
	traceContext, err := decodeTraceSnapshot(rawTrace, hint.TraceKey)
	if err != nil {
		stage.EmitOtelError("streaming_api.fill_context_error", err.Error())
		return
	}
	notification, addresses, err := buildTransactionNotification(traceContext, hint, stage)
	if err != nil {
		stage.EmitOtelError("streaming_api.finality_mismatch", err.Error())
		return
	}
	if notification != nil {
		accounts := make([]indexModels.AccountAddress, 0, len(notification.Transactions))
		for _, tx := range notification.Transactions {
			accounts = append(accounts, tx.Account)
		}
		targets := manager.subscribersForAddresses(EventTransactions, accounts, notification.Finality)
		book, metadata := manager.enrichmentNeeds(targets)
		enrichTraceNotification(ctx, notification, addresses, book, metadata)
		manager.sendNotification(notification, targets)
	}
	stage.Emit()
}

// Building a snapshot does not route it or mutate any client state.
func buildTransactionNotification(traceContext *crud.EmulatedTracesContext, hint transactionHint,
	stage *TraceProcessingStage) (*TransactionsNotification, []indexModels.AccountAddress, error) {
	txs, txIndexes, finality := transactionsForHint(traceContext, hint, stage)
	if len(txs) == 0 {
		return nil, nil, nil
	}
	if finality != hint.TraceFinality {
		return nil, nil, fmt.Errorf("hint trace_finality is %s, Redis snapshot finality is %s", hint.TraceFinality, finality)
	}
	addresses := attachTransactionMessages(traceContext, hint, txs, txIndexes)
	sortTransactions(txs)
	stage.Span.AddAttr("ton.trace.finality", finality.String())
	stage.Span.AddAttr("ton.transactions.count", len(txs))
	return &TransactionsNotification{
		version: deliveryVersion{seq: hint.UpdateSeq, partial: hint.UpdateFinality == indexModels.FinalityStatePending},
		Type:    EventTransactions, Finality: finality, TraceExternalHashNorm: hint.TraceKey, Transactions: txs,
	}, addresses, nil
}

func transactionsForHint(
	emulatedContext *crud.EmulatedTracesContext,
	hint transactionHint,
	stage *TraceProcessingStage,
) (
	[]indexModels.Transaction,
	map[indexModels.HashType]int,
	indexModels.FinalityState,
) {
	txs := make([]indexModels.Transaction, 0)
	txIndexes := make(map[indexModels.HashType]int)
	finality := indexModels.FinalityStateFinalized

	for _, tx := range emulatedContext.GetTransactions() {
		if hint.UpdateFinality == indexModels.FinalityStatePending && tx.Finality != indexModels.FinalityStatePending {
			continue
		}
		txs = append(txs, *tx)
		txIndexes[tx.Hash] = len(txs) - 1
		if tx.Finality < finality {
			finality = tx.Finality
		}
		if stage.RootTxHash == "-" && tx.TraceId != nil {
			stage.SetRootTxHash(*tx.TraceId)
		}
	}
	return txs, txIndexes, finality
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
