// Package protocol holds the ton_v1 rules shared by the host and the router.
package protocol

import (
	"fmt"

	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
)

// Provisional ton_v1 constants. They decide answers (out of gas, TOO_LARGE),
// so every backend has the same, and changing one requires a new contract
// version once v1 is published.
const (
	DefaultGetMethodGas = 1_000_000
	MaxGetMethodGas     = 10_000_000
	// Of any BOC in a call: message, ShardAccount, config param, library, stack cell.
	MaxInputBocSize = 1 << 20
	// Of a serialized Call, with all its fields.
	MaxCallRequestSize = 8 << 20
	// Of a serialized CallResult: over it the call is TOO_LARGE.
	MaxCallResultSize = 8 << 20
	// The end of the VM log that is kept.
	MaxVmLogSize = 64 << 10
	// Tuples and lists within each other in a stack: within the recursion
	// limit of protobuf decoders, 100 messages in prost and C++.
	MaxStackDepth = 32
	// Items of a tuple, as in TVM.
	MaxTupleSize = 255
)

// Transport only: the host splits a batch into router requests within these,
// which changes no answer.
const (
	MaxCallsPerRequest  = 1000
	MaxBatchRequestSize = 16 << 20
	MaxRPCMessageSize   = 256 << 20
)

func CallError(code pbtvm.ErrorCode, message string) *pbtvm.CallResult {
	return &pbtvm.CallResult{Outcome: &pbtvm.CallResult_Error{Error: &pbtvm.CallError{Code: code, Message: message}}}
}

func CallTooLarge(size int) *pbtvm.CallResult {
	return CallError(pbtvm.ErrorCode_ERROR_CODE_TOO_LARGE,
		fmt.Sprintf("call of %d bytes is over %d", size, MaxCallRequestSize))
}
