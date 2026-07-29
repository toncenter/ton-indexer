package v2

import (
	"errors"
	"testing"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDecodeTransactionHint(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]any{
		"trace_key":  "trace",
		"update_seq": uint64(42),
		"kind":       uint8(transactionHintFinalized),
		"finality":   uint8(indexModels.FinalityStateConfirmed),
		"accounts":   []string{"0:AAAA", "0:BBBB"},
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
	if hint.Kind != transactionHintFinalized {
		t.Fatalf("unexpected kind: %s", hint.Kind)
	}
	if hint.Finality != indexModels.FinalityStateConfirmed {
		t.Fatalf("unexpected finality: %s", hint.Finality)
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
