package v2

import (
	"errors"
	"fmt"
	"strconv"

	indexModels "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/vmihailenco/msgpack/v5"
)

type transactionHintKind uint8

const (
	transactionHintPending transactionHintKind = iota
	transactionHintConfirmed
	transactionHintFinalized
)

type transactionHint struct {
	TraceKey  indexModels.HashType         `msgpack:"trace_key"`
	UpdateSeq uint64                       `msgpack:"update_seq"`
	Kind      transactionHintKind          `msgpack:"kind"`
	Finality  indexModels.FinalityState    `msgpack:"finality"`
	Accounts  []indexModels.AccountAddress `msgpack:"accounts"`
}

type accountStateHint struct {
	Account  indexModels.AccountAddress `msgpack:"account"`
	Lt       uint64                     `msgpack:"lt"`
	Finality indexModels.FinalityState  `msgpack:"finality"`
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
	if hint.Kind > transactionHintFinalized {
		return transactionHint{}, fmt.Errorf("unknown transaction hint kind %d", hint.Kind)
	}
	if hint.Finality > indexModels.FinalityStateFinalized {
		return transactionHint{}, fmt.Errorf("unknown finality %d", hint.Finality)
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
	switch hint.Kind {
	case transactionHintPending:
		return normalPriority
	case transactionHintConfirmed:
		return confirmedPriority
	case transactionHintFinalized:
		return finalizedPriority
	default:
		panic("transaction hint kind was not validated")
	}
}

func (hint transactionHint) jobKey() string {
	if hint.Kind == transactionHintPending {
		return hint.TraceKey.String() + ":pending"
	}
	return hint.TraceKey.String() + ":snapshot"
}

func (kind transactionHintKind) String() string {
	switch kind {
	case transactionHintPending:
		return "pending"
	case transactionHintConfirmed:
		return "confirmed"
	case transactionHintFinalized:
		return "finalized"
	default:
		return fmt.Sprintf("unknown_%d", kind)
	}
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
