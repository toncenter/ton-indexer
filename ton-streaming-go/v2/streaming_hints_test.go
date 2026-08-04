package v2

import (
	"errors"
	"testing"
	"time"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/toncenter/ton-indexer/ton-streaming-go/observability"
	"github.com/vmihailenco/msgpack/v5"
)

func TestTraceHintSpanAttributesDescribeWorkerQueue(t *testing.T) {
	receivedAt := time.Now()
	workerStartedAt := receivedAt.Add(12 * time.Millisecond)
	stage := &TraceProcessingStage{Span: &observability.StageSpan{Attributes: make(map[string]any)}}

	addTraceHintSpanAttributes(stage, hintTiming{
		receivedAt:      receivedAt,
		workerStartedAt: workerStartedAt,
		workerIndex:     5,
	}, 42, indexModels.FinalityStateFinalized, indexModels.FinalityStateConfirmed)

	attributes := stage.Span.Attributes
	if attributes["ton.streaming.update_seq"] != uint64(42) ||
		attributes["ton.streaming.update_finality"] != "finalized" ||
		attributes["ton.streaming.hint_trace_finality"] != "confirmed" {
		t.Fatalf("unexpected hint identity attributes: %#v", attributes)
	}
	if attributes["ton.streaming.worker.index"] != 5 || attributes["ton.streaming.worker_queue_ms"] != float64(12) {
		t.Fatalf("unexpected worker attributes: %#v", attributes)
	}
}

func TestDecodeTransactionHint(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]any{
		"trace_key":       "trace",
		"update_seq":      uint64(42),
		"update_finality": uint8(indexModels.FinalityStateFinalized),
		"trace_finality":  uint8(indexModels.FinalityStateConfirmed),
		"accounts":        []string{"0:AAAA", "0:BBBB"},
	})
	if err != nil {
		t.Fatal(err)
	}

	hint, err := decodeTransactionHint(string(payload))
	if err != nil {
		t.Fatal(err)
	}
	if hint.TraceKey != "trace" || hint.UpdateSeq != 42 {
		t.Fatalf("unexpected trace version: %#v", hint)
	}
	if hint.UpdateFinality != indexModels.FinalityStateFinalized {
		t.Fatalf("unexpected update_finality: %s", hint.UpdateFinality)
	}
	if hint.TraceFinality != indexModels.FinalityStateConfirmed {
		t.Fatalf("unexpected trace_finality: %s", hint.TraceFinality)
	}
	if len(hint.Accounts) != 2 || hint.Accounts[0] != "0:AAAA" || hint.Accounts[1] != "0:BBBB" {
		t.Fatalf("unexpected accounts: %v", hint.Accounts)
	}
}

func TestDecodeAccountStateHint(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]any{
		"account":  "0:AAAA",
		"lt":       uint64(123),
		"finality": uint8(indexModels.FinalityStateFinalized),
	})
	if err != nil {
		t.Fatal(err)
	}

	hint, err := decodeAccountStateHint(string(payload))
	if err != nil {
		t.Fatal(err)
	}
	if hint.Account != "0:AAAA" || hint.Lt != 123 || hint.Finality != indexModels.FinalityStateFinalized {
		t.Fatalf("unexpected account state hint: %#v", hint)
	}
}

func TestDecodeActionsHint(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]any{
		"trace_key":       "trace",
		"update_seq":      uint64(42),
		"update_finality": uint8(indexModels.FinalityStateFinalized),
		"trace_finality":  uint8(indexModels.FinalityStateConfirmed),
		"actions_updated": true,
		"action_types_and_accounts": []map[string]any{{
			"type":     "ton_transfer",
			"accounts": []string{"0:AAAA", "0:BBBB"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	hint, err := decodeActionsHint(string(payload))
	if err != nil {
		t.Fatal(err)
	}
	if hint.TraceKey != "trace" || hint.UpdateSeq != 42 ||
		hint.UpdateFinality != indexModels.FinalityStateFinalized ||
		hint.TraceFinality != indexModels.FinalityStateConfirmed {
		t.Fatalf("unexpected action hint version: %#v", hint)
	}
	if !hint.ActionsUpdated || len(hint.ActionTypesAndAccounts) != 1 {
		t.Fatalf("unexpected action status or routes: %#v", hint)
	}
	if route := hint.ActionTypesAndAccounts[0]; route.Type != "ton_transfer" || len(route.Accounts) != 2 {
		t.Fatalf("unexpected action route: %#v", route)
	}
}

func TestDecodeFailedActionsHint(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]any{
		"trace_key":                 "trace",
		"update_seq":                uint64(42),
		"update_finality":           uint8(indexModels.FinalityStateConfirmed),
		"trace_finality":            uint8(indexModels.FinalityStatePending),
		"actions_updated":           false,
		"action_types_and_accounts": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}

	hint, err := decodeActionsHint(string(payload))
	if err != nil {
		t.Fatal(err)
	}
	if hint.ActionsUpdated || len(hint.ActionTypesAndAccounts) != 0 {
		t.Fatalf("failed classification must decode as an empty update: %#v", hint)
	}
}

func TestFailedActionsHintCannotContainRoutes(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]any{
		"trace_key":       "trace",
		"update_seq":      uint64(42),
		"update_finality": uint8(indexModels.FinalityStateConfirmed),
		"trace_finality":  uint8(indexModels.FinalityStatePending),
		"actions_updated": false,
		"action_types_and_accounts": []map[string]any{{
			"type":     "ton_transfer",
			"accounts": []string{"0:AAAA"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := decodeActionsHint(string(payload)); err == nil {
		t.Fatal("failed classification with action routes must be rejected")
	}
}

func TestTraceHintVersionMustMatchRedis(t *testing.T) {
	if err := validateTraceHintVersion(map[string]string{"update_seq": "7"}, 7); err != nil {
		t.Fatalf("matching version rejected: %v", err)
	}

	err := validateTraceHintVersion(map[string]string{"update_seq": "8"}, 7)
	if !errors.Is(err, errStaleStreamingHint) {
		t.Fatalf("newer Redis snapshot must make the hint stale, got: %v", err)
	}

	err = validateTraceHintVersion(map[string]string{"update_seq": "6"}, 7)
	if err == nil || errors.Is(err, errStaleStreamingHint) {
		t.Fatalf("older Redis snapshot must be reported as inconsistent, got: %v", err)
	}
}

func TestAccountStateJobKeyIdentifiesTheStateVersion(t *testing.T) {
	confirmed := accountStateHint{
		Account:  "0:AAAA",
		Lt:       100,
		Finality: indexModels.FinalityStateConfirmed,
	}
	finalized := confirmed
	finalized.Finality = indexModels.FinalityStateFinalized

	if confirmed.jobKey() != finalized.jobKey() {
		t.Fatal("confirmed and finalized forms of the same state must coalesce")
	}

	nextState := confirmed
	nextState.Lt = 101
	if confirmed.jobKey() == nextState.jobKey() {
		t.Fatal("different account states must remain separate")
	}
}

func TestActionsJobKeyKeepsPendingSeparateFromCommittedSnapshot(t *testing.T) {
	pending := actionsHint{TraceKey: "trace", UpdateFinality: indexModels.FinalityStatePending}
	confirmed := actionsHint{TraceKey: "trace", UpdateFinality: indexModels.FinalityStateConfirmed}
	finalized := actionsHint{TraceKey: "trace", UpdateFinality: indexModels.FinalityStateFinalized}

	if pending.jobKey() == confirmed.jobKey() {
		t.Fatal("pending actions must not replace a queued committed snapshot")
	}
	if confirmed.jobKey() != finalized.jobKey() {
		t.Fatal("finalized actions must replace a queued confirmed snapshot")
	}
	if !(pending.priority() < confirmed.priority() && confirmed.priority() < finalized.priority()) {
		t.Fatal("action job priorities must follow finality")
	}
}

func TestActionsPriorityUsesUpdateFinality(t *testing.T) {
	hint := actionsHint{
		TraceKey:       "trace",
		UpdateFinality: indexModels.FinalityStateFinalized,
		TraceFinality:  indexModels.FinalityStatePending,
	}

	if hint.priority() != finalizedPriority {
		t.Fatal("a finalized update must keep finalized priority even when the resulting trace is still pending")
	}
}
