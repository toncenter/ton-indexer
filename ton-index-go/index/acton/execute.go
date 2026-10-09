// Package acton is the catalog-backed ABI layer: candidate selection, TVM stack
// conversion, storage decoding and code identification. It touches neither the
// database nor HTTP, so the handlers and the getter transport share it without
// depending on each other.
package acton

import (
	"context"

	"github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

const MaxBodyBytes = 1 << 20

func Fail(code int, message string) error { return models.IndexError{Code: code, Message: message} }

type Snapshot struct {
	Address             string  `json:"address"`
	CodeHash            *string `json:"code_hash"`
	ImplementationHash  *string `json:"implementation_hash,omitempty"`
	DataHash            *string `json:"data_hash"`
	LastTransactionHash *string `json:"last_transaction_hash"`
	LastTransactionLT   *string `json:"last_transaction_lt"`
	McSeqno             *int32  `json:"mc_seqno,omitempty"`
} // @name ActonSnapshot

// Stack is spelled the way /runGetMethod spells one, so one parser reads both.
// Native is the same stack in the shape the codecs consume, decimal integers
// and Lisp lists as cons pairs; a client reads the typed answer in `decoded`.
// StackError must prevent typed decoding, not hide the exit code or the gas.
type Execution struct {
	Stack      []models.V2StackEntity `json:"stack"`
	GasUsed    int64                  `json:"gas_used"`
	ExitCode   int64                  `json:"exit_code"`
	StackError string                 `json:"stack_error,omitempty"`
	Native     []tolkabi.StackValue   `json:"-"`
} // @name ActonExecution

// GetterExecutor reads pinned account state and runs a getter against it. The
// transport is injected so this package stays free of HTTP and of the upstream.
type GetterExecutor interface {
	Snapshot(context.Context, string, *int32) (*Snapshot, error)
	Run(context.Context, *Snapshot, int64, []tolkabi.StackValue) (*Execution, error)
}
