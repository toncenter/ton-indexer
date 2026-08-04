package v2

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	"github.com/toncenter/ton-indexer/ton-index-go/index/detect"
	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
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
		receivedAt := time.Now()

		hint, err := decodeTransactionHint(msg.Payload)
		if err != nil {
			log.Printf("[v2] Invalid transaction hint: %v", err)
			continue
		}
		hint.timing = hintTiming{receivedAt: receivedAt, workerIndex: pool.workerIndexFor(hint.TraceKey.String())}
		if !hasTransactionSubscribers(manager, hint) {
			continue
		}
		logTraceStage(hint.TraceKey, "stage=redis_hint_received stream=transactions update_seq=%d update_finality=%s "+
			"hint_trace_finality=%s worker=%d decode_ms=%.3f accounts=%d", hint.UpdateSeq, hint.UpdateFinality,
			hint.TraceFinality, hint.timing.workerIndex, durationMilliseconds(time.Since(receivedAt)), len(hint.Accounts))
		enqueueStarted := time.Now()
		logTraceStage(hint.TraceKey, "stage=worker_enqueue_started stream=transactions update_seq=%d update_finality=%s "+
			"worker=%d priority=%s", hint.UpdateSeq, hint.UpdateFinality, hint.timing.workerIndex, hint.priority())
		if !pool.Enqueue(ctx, hint.TraceKey.String(), hint.jobKey(), hint.priority(), hint) {
			logTraceStage(hint.TraceKey, "stage=worker_enqueue_stopped stream=transactions update_seq=%d update_finality=%s "+
				"worker=%d wait_ms=%.3f", hint.UpdateSeq, hint.UpdateFinality, hint.timing.workerIndex,
				durationMilliseconds(time.Since(enqueueStarted)))
			return
		}
		logTraceStage(hint.TraceKey, "stage=worker_enqueue_finished stream=transactions update_seq=%d update_finality=%s "+
			"worker=%d wait_ms=%.3f", hint.UpdateSeq, hint.UpdateFinality, hint.timing.workerIndex,
			durationMilliseconds(time.Since(enqueueStarted)))
	}
}

func hasTransactionSubscribers(manager *ClientManager, hint transactionHint) bool {
	return len(manager.subscribersForAddresses(EventTransactions, hint.Accounts, hint.TraceFinality)) > 0
}

func ProcessTransactionHint(ctx context.Context, rdb *redis.Client, hint transactionHint, manager *ClientManager, channel string) {
	hint.timing.workerStartedAt = time.Now()
	logTraceStage(hint.TraceKey, "stage=worker_started stream=transactions update_seq=%d update_finality=%s worker=%d "+
		"worker_queue_ms=%.3f", hint.UpdateSeq, hint.UpdateFinality, hint.timing.workerIndex,
		durationMilliseconds(durationBetween(hint.timing.receivedAt, hint.timing.workerStartedAt)))
	defer func() {
		logTraceStage(hint.TraceKey, "stage=worker_finished stream=transactions update_seq=%d update_finality=%s total_ms=%.3f",
			hint.UpdateSeq, hint.UpdateFinality, durationMilliseconds(durationBetween(hint.timing.receivedAt, time.Now())))
	}()
	// Subscriptions may have changed while the hint waited in the queue.
	if !hasTransactionSubscribers(manager, hint) {
		logUndeliveredTraceHint("transactions", hint.TraceKey, hint.UpdateSeq, hint.UpdateFinality, hint.timing,
			"subscription_disappeared_before_processing", "")
		return
	}

	startTimeUnix := hint.timing.workerStartedAt.UnixNano()
	redisStartedAt := time.Now()
	logTraceStage(hint.TraceKey, "stage=redis_hgetall_started stream=transactions update_seq=%d update_finality=%s",
		hint.UpdateSeq, hint.UpdateFinality)
	rawTrace, err := rdb.HGetAll(ctx, hint.TraceKey.String()).Result()
	if err != nil {
		logTraceStage(hint.TraceKey, "stage=redis_hgetall_failed stream=transactions update_seq=%d update_finality=%s "+
			"duration_ms=%.3f error=%q", hint.UpdateSeq, hint.UpdateFinality,
			durationMilliseconds(time.Since(redisStartedAt)), err)
		return
	}
	logTraceStage(hint.TraceKey, "stage=redis_hgetall_finished stream=transactions update_seq=%d update_finality=%s "+
		"duration_ms=%.3f fields=%d redis_update_seq=%s", hint.UpdateSeq, hint.UpdateFinality,
		durationMilliseconds(time.Since(redisStartedAt)), len(rawTrace), rawTrace["update_seq"])
	if err := validateTraceHintVersion(rawTrace, hint.UpdateSeq); err != nil {
		if errors.Is(err, errStaleStreamingHint) {
			logUndeliveredTraceHint("transactions", hint.TraceKey, hint.UpdateSeq, hint.UpdateFinality, hint.timing,
				"stale_snapshot", rawTrace["update_seq"])
		} else {
			logTraceStage(hint.TraceKey, "stage=redis_snapshot_invalid stream=transactions update_seq=%d "+
				"update_finality=%s redis_update_seq=%s error=%q", hint.UpdateSeq, hint.UpdateFinality,
				rawTrace["update_seq"], err)
			logUndeliveredTraceHint("transactions", hint.TraceKey, hint.UpdateSeq, hint.UpdateFinality, hint.timing,
				"version_mismatch", rawTrace["update_seq"])
		}
		return
	}

	stage := NewTraceProcessingStage(startTimeUnix, rawTraceSpanName, rawTrace, hint.TraceKey.String(), channel)
	addTraceHintSpanAttributes(stage, hint.timing, hint.UpdateSeq, hint.UpdateFinality, hint.TraceFinality)
	rawTraces := map[string]map[string]string{
		hint.TraceKey.String(): rawTrace,
	}
	emulatedContext := crud.NewEmptyContext(false)
	if err := emulatedContext.FillFromRawData(rawTraces); err != nil {
		logTraceStage(hint.TraceKey, "stage=trace_decode_failed stream=transactions update_seq=%d update_finality=%s "+
			"error=%q", hint.UpdateSeq, hint.UpdateFinality, err)
		stage.EmitOtelError("streaming_api.fill_context_error", err.Error())
		return
	}
	if emulatedContext.GetTraceCount() != 1 {
		err := fmt.Errorf("expected one trace, got %d", emulatedContext.GetTraceCount())
		logTraceStage(hint.TraceKey, "stage=trace_decode_failed stream=transactions update_seq=%d update_finality=%s "+
			"error=%q", hint.UpdateSeq, hint.UpdateFinality, err)
		stage.EmitOtelError("streaming_api.invalid_trace_count", err.Error())
		return
	}

	txs, txIndexes, transactionAccounts, traceFinality := transactionsForHint(emulatedContext, hint, stage)
	if len(txs) == 0 {
		stage.Span.AddAttr("ton.streaming.skipped", true)
		stage.Span.AddAttr("ton.streaming.skip_reason", "no_matching_transactions")
		logUndeliveredTraceHint("transactions", hint.TraceKey, hint.UpdateSeq, hint.UpdateFinality, hint.timing,
			"no_matching_transactions", rawTrace["update_seq"])
		stage.Emit()
		return
	}
	if traceFinality != hint.TraceFinality {
		err := fmt.Errorf("hint trace_finality is %s, Redis snapshot finality is %s", hint.TraceFinality, traceFinality)
		logTraceStage(hint.TraceKey, "stage=trace_finality_mismatch stream=transactions update_seq=%d update_finality=%s "+
			"error=%q", hint.UpdateSeq, hint.UpdateFinality, err)
		logUndeliveredTraceHint("transactions", hint.TraceKey, hint.UpdateSeq, hint.UpdateFinality, hint.timing,
			"finality_mismatch", rawTrace["update_seq"])
		stage.EmitOtelError("streaming_api.finality_mismatch", err.Error())
		return
	}

	targets := manager.subscribersForAddresses(EventTransactions, transactionAccounts, traceFinality)
	if len(targets) == 0 {
		stage.Span.AddAttr("ton.streaming.skipped", true)
		stage.Span.AddAttr("ton.streaming.skip_reason", "subscription_disappeared")
		logUndeliveredTraceHint("transactions", hint.TraceKey, hint.UpdateSeq, hint.UpdateFinality, hint.timing,
			"subscription_disappeared_after_processing", rawTrace["update_seq"])
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
	logTraceStage(hint.TraceKey, "stage=trace_processed stream=transactions update_seq=%d update_finality=%s "+
		"event_finality=%s transactions=%d targets=%d worker_process_ms=%.3f", hint.UpdateSeq, hint.UpdateFinality,
		traceFinality, len(txs), len(targets), durationMilliseconds(time.Since(hint.timing.workerStartedAt)))
	manager.sendHintNotification(&TransactionsNotification{
		Type:                  EventTransactions,
		Finality:              traceFinality,
		TraceExternalHashNorm: hint.TraceKey,
		UpdateSeq:             hint.UpdateSeq,
		UpdateFinality:        hint.UpdateFinality,
		Transactions:          txs,
		AddressBook:           addressBook,
		Metadata:              metadata,
	}, targets, hint.timing)
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
			logTraceStage(hint.TraceKey, "stage=message_attach_skipped stream=transactions update_seq=%d "+
				"update_finality=%s reason=unknown_transaction transaction=%s", hint.UpdateSeq, hint.UpdateFinality, msg.TxHash)
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
			logTraceStage(hint.TraceKey, "stage=message_marking_failed stream=transactions update_seq=%d "+
				"update_finality=%s error=%q", hint.UpdateSeq, hint.UpdateFinality, err)
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
