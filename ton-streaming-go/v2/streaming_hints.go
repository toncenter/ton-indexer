package v2

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/vmihailenco/msgpack/v5"
)

type hintTiming struct {
	receivedAt      time.Time
	workerStartedAt time.Time
	workerIndex     int
}

func logTraceStage(traceKey indexModels.HashType, format string, args ...any) {
	if traceKey == "" {
		return
	}
	values := make([]any, 0, len(args)+1)
	values = append(values, traceKey)
	values = append(values, args...)
	log.Printf("[v2] external_message_hash_norm=%s "+format, values...)
}

func addTraceHintSpanAttributes(stage *TraceProcessingStage, timing hintTiming, updateSeq uint64,
	updateFinality indexModels.FinalityState, traceFinality indexModels.FinalityState) {
	stage.Span.AddAttr("ton.streaming.update_seq", updateSeq)
	stage.Span.AddAttr("ton.streaming.update_finality", updateFinality.String())
	stage.Span.AddAttr("ton.streaming.hint_trace_finality", traceFinality.String())
	stage.Span.AddAttr("ton.streaming.worker.index", timing.workerIndex)
	stage.Span.AddAttr("ton.streaming.worker_queue_ms",
		durationMilliseconds(durationBetween(timing.receivedAt, timing.workerStartedAt)))
}

func logUndeliveredTraceHint(stream string, traceKey indexModels.HashType, updateSeq uint64,
	updateFinality indexModels.FinalityState, timing hintTiming, reason string, redisUpdateSeq string) {
	logTraceStage(traceKey, "stage=hint_not_delivered stream=%s update_seq=%d update_finality=%s reason=%s worker=%d "+
		"worker_queue_ms=%.3f redis_update_seq=%s", stream, updateSeq, updateFinality, reason, timing.workerIndex,
		durationMilliseconds(durationBetween(timing.receivedAt, timing.workerStartedAt)), redisUpdateSeq)
}

type transactionHint struct {
	TraceKey       indexModels.HashType         `msgpack:"trace_key"`
	UpdateSeq      uint64                       `msgpack:"update_seq"`
	UpdateFinality indexModels.FinalityState    `msgpack:"update_finality"`
	TraceFinality  indexModels.FinalityState    `msgpack:"trace_finality"`
	Accounts       []indexModels.AccountAddress `msgpack:"accounts"`
	timing         hintTiming
}

type accountStateHint struct {
	Account  indexModels.AccountAddress `msgpack:"account"`
	Lt       uint64                     `msgpack:"lt"`
	Finality indexModels.FinalityState  `msgpack:"finality"`
}

type actionRoute struct {
	Type     string                       `msgpack:"type"`
	Accounts []indexModels.AccountAddress `msgpack:"accounts"`
}

type actionsHint struct {
	TraceKey               indexModels.HashType      `msgpack:"trace_key"`
	UpdateSeq              uint64                    `msgpack:"update_seq"`
	UpdateFinality         indexModels.FinalityState `msgpack:"update_finality"`
	TraceFinality          indexModels.FinalityState `msgpack:"trace_finality"`
	ActionsUpdated         bool                      `msgpack:"actions_updated"`
	ActionTypesAndAccounts []actionRoute             `msgpack:"action_types_and_accounts"`
	timing                 hintTiming
}

var errStaleStreamingHint = errors.New("stale streaming hint")

func decodeTransactionHint(payload string) (transactionHint, error) {
	var hint transactionHint
	if err := msgpack.Unmarshal([]byte(payload), &hint); err != nil {
		return transactionHint{}, fmt.Errorf("decode msgpack: %w", err)
	}
	if hint.TraceKey == "" {
		return transactionHint{}, fmt.Errorf("missing trace_key")
	}
	if hint.UpdateSeq == 0 {
		return transactionHint{}, fmt.Errorf("update_seq must be positive")
	}
	if hint.UpdateFinality > indexModels.FinalityStateFinalized {
		return transactionHint{}, fmt.Errorf("unknown update_finality %d", hint.UpdateFinality)
	}
	if hint.TraceFinality > indexModels.FinalityStateFinalized {
		return transactionHint{}, fmt.Errorf("unknown trace_finality %d", hint.TraceFinality)
	}
	if len(hint.Accounts) == 0 {
		return transactionHint{}, fmt.Errorf("missing transaction accounts")
	}
	return hint, nil
}

func decodeAccountStateHint(payload string) (accountStateHint, error) {
	var hint accountStateHint
	if err := msgpack.Unmarshal([]byte(payload), &hint); err != nil {
		return accountStateHint{}, fmt.Errorf("decode msgpack: %w", err)
	}
	if hint.Account == "" {
		return accountStateHint{}, fmt.Errorf("missing account")
	}
	if hint.Finality != indexModels.FinalityStateConfirmed &&
		hint.Finality != indexModels.FinalityStateFinalized {
		return accountStateHint{}, fmt.Errorf("unsupported finality %d", hint.Finality)
	}
	return hint, nil
}

func decodeActionsHint(payload string) (actionsHint, error) {
	var hint actionsHint
	if err := msgpack.Unmarshal([]byte(payload), &hint); err != nil {
		return actionsHint{}, fmt.Errorf("decode msgpack: %w", err)
	}
	if hint.TraceKey == "" {
		return actionsHint{}, fmt.Errorf("missing trace_key")
	}
	if hint.UpdateSeq == 0 {
		return actionsHint{}, fmt.Errorf("update_seq must be positive")
	}
	if hint.UpdateFinality > indexModels.FinalityStateFinalized {
		return actionsHint{}, fmt.Errorf("unknown update_finality %d", hint.UpdateFinality)
	}
	if hint.TraceFinality > indexModels.FinalityStateFinalized {
		return actionsHint{}, fmt.Errorf("unknown trace_finality %d", hint.TraceFinality)
	}
	if !hint.ActionsUpdated && len(hint.ActionTypesAndAccounts) != 0 {
		return actionsHint{}, fmt.Errorf("actions_updated=false must not contain action routes")
	}
	for index, route := range hint.ActionTypesAndAccounts {
		if route.Type == "" {
			return actionsHint{}, fmt.Errorf("action route %d has no type", index)
		}
	}
	return hint, nil
}

func validateTraceHintVersion(rawTrace map[string]string, expected uint64) error {
	actual, err := parseRedisVersion(rawTrace["update_seq"], "update_seq")
	if err != nil {
		return err
	}
	return compareStreamingVersion(actual, expected, "trace update_seq")
}

func validateAccountStateHintVersion(rawState map[string]string, expected uint64) error {
	actual, err := parseRedisVersion(rawState["lt"], "account lt")
	if err != nil {
		return err
	}
	return compareStreamingVersion(actual, expected, "account lt")
}

func parseRedisVersion(raw string, field string) (uint64, error) {
	if raw == "" {
		return 0, fmt.Errorf("missing Redis %s", field)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Redis %s %q: %w", field, raw, err)
	}
	return value, nil
}

func compareStreamingVersion(actual uint64, expected uint64, field string) error {
	switch {
	case actual > expected:
		return fmt.Errorf("%w: Redis %s %d is newer than hint %d", errStaleStreamingHint, field, actual, expected)
	case actual < expected:
		return fmt.Errorf("Redis %s %d is older than published hint %d", field, actual, expected)
	default:
		return nil
	}
}

func (hint transactionHint) priority() jobPriority {
	return priorityForUpdateFinality(hint.UpdateFinality)
}

func priorityForUpdateFinality(finality indexModels.FinalityState) jobPriority {
	switch finality {
	case indexModels.FinalityStatePending:
		return normalPriority
	case indexModels.FinalityStateConfirmed:
		return confirmedPriority
	case indexModels.FinalityStateFinalized:
		return finalizedPriority
	default:
		panic("update_finality was not validated")
	}
}

func (hint transactionHint) jobKey() string {
	if hint.UpdateFinality == indexModels.FinalityStatePending {
		return hint.TraceKey.String() + ":pending"
	}
	return hint.TraceKey.String() + ":snapshot"
}

func (hint accountStateHint) priority() jobPriority {
	if hint.Finality == indexModels.FinalityStateFinalized {
		return finalizedPriority
	}
	return confirmedPriority
}

func (hint accountStateHint) jobKey() string {
	return hint.Account.String() + ":" + strconv.FormatUint(hint.Lt, 10)
}

func (hint actionsHint) priority() jobPriority {
	return priorityForUpdateFinality(hint.UpdateFinality)
}

func (hint actionsHint) jobKey() string {
	if hint.UpdateFinality == indexModels.FinalityStatePending {
		return hint.TraceKey.String() + ":actions:pending"
	}
	return hint.TraceKey.String() + ":actions:snapshot"
}
