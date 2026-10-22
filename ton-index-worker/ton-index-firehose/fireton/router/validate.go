package router

import (
	"fmt"
	"maps"
	"slices"

	pbstate "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/state/v1"
	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
)

// The rules of requests that need no state, checked before a backend is
// asked: a call that breaks one is answered here, for any block.

type callError struct {
	code    pbtvm.ErrorCode
	message string
}

func invalid(format string, args ...any) *callError {
	return &callError{code: pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST, message: fmt.Sprintf(format, args...)}
}

func tooLarge(what string, size int) *callError {
	return &callError{
		code:    pbtvm.ErrorCode_ERROR_CODE_TOO_LARGE,
		message: fmt.Sprintf("%s of %d bytes is over %d", what, size, protocol.MaxInputBocSize),
	}
}

func (e *callError) result() *pbtvm.CallResult {
	return &pbtvm.CallResult{Outcome: &pbtvm.CallResult_Error{Error: &pbtvm.CallError{Code: e.code, Message: e.message}}}
}

func validateCall(call *pbtvm.Call) *callError {
	switch kind := call.GetKind().(type) {
	case *pbtvm.Call_GetMethod:
		return validateGetMethod(kind.GetMethod)
	case *pbtvm.Call_Emulate:
		return validateEmulate(kind.Emulate)
	case *pbtvm.Call_GetAccount:
		if kind.GetAccount.GetAccount() == nil {
			return invalid("get_account without account")
		}
		return validateAccountRef(kind.GetAccount.GetAccount(), true)
	}
	return invalid("call without kind")
}

func validateGetMethod(call *pbtvm.GetMethod) *callError {
	if call.GetAccount() == nil {
		return invalid("get_method without account")
	}
	if err := validateAccountSource(call.GetAccount(), true); err != nil {
		return err
	}
	switch method := call.GetMethod().(type) {
	case *pbtvm.GetMethod_Name:
		if method.Name == "" {
			return invalid("empty method name")
		}
	case *pbtvm.GetMethod_Id:
	default:
		return invalid("get_method without method")
	}
	for i, entry := range call.GetStack() {
		if err := validateStackEntry(entry, fmt.Sprintf("stack[%d]", i), 0); err != nil {
			return err
		}
	}
	if call.GasLimit != nil && call.GetGasLimit() > protocol.MaxGetMethodGas {
		return invalid("gas_limit %d is over %d", call.GetGasLimit(), protocol.MaxGetMethodGas)
	}
	return validateContext(call.GetContext())
}

func validateEmulate(call *pbtvm.Emulate) *callError {
	if len(call.GetMessageBoc()) == 0 {
		return invalid("emulate without message")
	}
	if len(call.GetMessageBoc()) > protocol.MaxInputBocSize {
		return tooLarge("message", len(call.GetMessageBoc()))
	}
	if call.GetAccount() != nil {
		if err := validateAccountSource(call.GetAccount(), false); err != nil {
			return err
		}
	}
	return validateContext(call.GetContext())
}

// addressRequired is false in Emulate, where no address is the destination.
func validateAccountSource(source *pbtvm.AccountSource, addressRequired bool) *callError {
	switch s := source.GetSource().(type) {
	case *pbtvm.AccountSource_Onchain:
		return validateAccountRef(s.Onchain, addressRequired)
	case *pbtvm.AccountSource_ShardAccountBoc:
		if len(s.ShardAccountBoc) == 0 {
			return invalid("empty shard_account_boc")
		}
		if len(s.ShardAccountBoc) > protocol.MaxInputBocSize {
			return tooLarge("shard_account_boc", len(s.ShardAccountBoc))
		}
		return nil
	}
	return invalid("account without source")
}

func validateAccountRef(ref *pbstate.AccountRef, addressRequired bool) *callError {
	if ref.GetAddress() == nil {
		if addressRequired {
			return invalid("account without address")
		}
	} else if n := len(ref.GetAddress().GetAccountId()); n != 32 {
		return invalid("account_id of %d bytes", n)
	}
	return nil
}

func validateContext(ctx *pbtvm.ContextOverrides) *callError {
	if ctx == nil {
		return nil
	}
	if ctx.Now != nil && ctx.GetNow() == 0 {
		return invalid("now is 0")
	}
	if ctx.Lt != nil && ctx.GetLt() == 0 {
		return invalid("lt is 0")
	}
	if ctx.BlockRandSeed != nil {
		seed := ctx.GetBlockRandSeed()
		if len(seed) != 32 {
			return invalid("block_rand_seed of %d bytes", len(seed))
		}
		zero := true
		for _, b := range seed {
			zero = zero && b == 0
		}
		if zero {
			return invalid("block_rand_seed is zero")
		}
	}
	// A CallError is cached just like a successful result. Always select the
	// same first error, including when different params break different limits.
	for _, param := range slices.Sorted(maps.Keys(ctx.GetConfigParams())) {
		boc := ctx.ConfigParams[param]
		if len(boc) == 0 {
			return invalid("empty config param %d", param)
		}
		if len(boc) > protocol.MaxInputBocSize {
			return tooLarge(fmt.Sprintf("config param %d", param), len(boc))
		}
	}
	for i, boc := range ctx.GetLibraries() {
		if len(boc) == 0 {
			return invalid("empty library %d", i)
		}
		if len(boc) > protocol.MaxInputBocSize {
			return tooLarge(fmt.Sprintf("library %d", i), len(boc))
		}
	}
	return nil
}

// depth is the number of tuples and lists the entry is within.
func validateStackEntry(entry *pbtvm.StackEntry, where string, depth int) *callError {
	switch v := entry.GetValue().(type) {
	case *pbtvm.StackEntry_Null, *pbtvm.StackEntry_Nan:
		return nil
	case *pbtvm.StackEntry_Integer:
		if !minimalInt257(v.Integer) {
			return invalid("%s is not a minimal int257", where)
		}
		return nil
	case *pbtvm.StackEntry_Cell:
		return validateStackBoc(v.Cell, where)
	case *pbtvm.StackEntry_Builder:
		return validateStackBoc(v.Builder, where)
	case *pbtvm.StackEntry_Slice:
		s := v.Slice
		if s.GetStartBit() > s.GetEndBit() || s.GetStartRef() > s.GetEndRef() {
			return invalid("%s has bounds out of order", where)
		}
		return validateStackBoc(s.GetCellBoc(), where)
	case *pbtvm.StackEntry_Tuple:
		if n := len(v.Tuple.GetItems()); n > protocol.MaxTupleSize {
			return invalid("%s is a tuple of %d items, over %d", where, n, protocol.MaxTupleSize)
		}
		return validateItems(v.Tuple.GetItems(), where, depth)
	case *pbtvm.StackEntry_List:
		return validateItems(v.List.GetItems(), where, depth)
	case *pbtvm.StackEntry_Unsupported:
		return invalid("%s is unsupported, which is for results only", where)
	}
	return invalid("%s is empty", where)
}

func validateItems(items []*pbtvm.StackEntry, where string, depth int) *callError {
	if depth == protocol.MaxStackDepth {
		return invalid("%s is nested deeper than %d", where, protocol.MaxStackDepth)
	}
	for i, item := range items {
		if err := validateStackEntry(item, fmt.Sprintf("%s[%d]", where, i), depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateStackBoc(boc []byte, where string) *callError {
	if len(boc) == 0 {
		return invalid("%s has an empty BOC", where)
	}
	if len(boc) > protocol.MaxInputBocSize {
		return tooLarge(where, len(boc))
	}
	return nil
}

// 1 to 33 bytes of two's complement big-endian without a redundant sign byte:
// exactly the range -2^256 to 2^256-1.
func minimalInt257(b []byte) bool {
	if len(b) == 0 || len(b) > 33 {
		return false
	}
	if len(b) == 33 && b[0] != 0x00 && b[0] != 0xff {
		return false
	}
	if len(b) > 1 && ((b[0] == 0x00 && b[1]&0x80 == 0) || (b[0] == 0xff && b[1]&0x80 != 0)) {
		return false
	}
	return true
}
