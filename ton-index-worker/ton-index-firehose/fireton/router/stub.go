package router

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	pbton "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/sf/ton/type/v1"
	pbstate "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/state/v1"
	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"google.golang.org/protobuf/proto"
)

// StubBackend answers without a node: for testing fireton, the router and
// the SDK before ton-state-svc. It knows no accounts, so in its answers:
//   - every account on chain is account_none; it has no transactions, so
//     before_tx and after_tx are INVALID_REQUEST;
//   - a get-method on chain is ACCOUNT_NOT_ACTIVE; on a given ShardAccount it
//     "returns" the seqno of the block, exit code 0;
//   - an emulation on chain is not accepted, exit code 0, as an external
//     message to a nonexistent account; on a given ShardAccount it is
//     EMULATION_FAILED.
//
// Snapshots name the masterchain block itself, with the root hash of the
// request, so a module can check which block it was answered from.
type StubBackend struct {
	// Each block is reached this long after its gen_utime, as by a backend
	// lagging behind the chain: zero answers every block at once.
	Lag time.Duration
}

var accountNoneShardAccountBoc = mustHex(
	"b5ee9c7201010201002e000150" + "00000000000000000000000000000000000000000000000000000000000000000000000000000000" + "01000140")

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func (b *StubBackend) Name() string {
	return "stub"
}

func (b *StubBackend) Reached(block *pbtvm.BlockRequest) bool {
	return !time.Now().Before(time.Unix(int64(block.GetMcGenUtime()), 0).Add(b.Lag))
}

func (b *StubBackend) Execute(_ context.Context, block *pbtvm.BlockRequest, calls []*pbtvm.Call) ([]*pbtvm.CallResult, error) {
	results := make([]*pbtvm.CallResult, len(calls))
	for i, call := range calls {
		results[i] = b.answer(block, call)
	}
	return results, nil
}

func (b *StubBackend) answer(block *pbtvm.BlockRequest, call *pbtvm.Call) *pbtvm.CallResult {
	switch kind := call.GetKind().(type) {
	case *pbtvm.Call_GetAccount:
		snapshot, err := b.snapshot(block, kind.GetAccount.GetAccount())
		if err != nil {
			return err.result()
		}
		return &pbtvm.CallResult{Outcome: &pbtvm.CallResult_Account{Account: snapshot}}
	case *pbtvm.Call_GetMethod:
		if _, ok := kind.GetMethod.GetAccount().GetSource().(*pbtvm.AccountSource_ShardAccountBoc); ok {
			return &pbtvm.CallResult{Outcome: &pbtvm.CallResult_GetMethod{GetMethod: &pbtvm.GetMethodResult{
				Stack: []*pbtvm.StackEntry{{Value: &pbtvm.StackEntry_Integer{Integer: int257(int64(block.GetMcSeqno()))}}},
			}}}
		}
		if _, err := b.snapshot(block, kind.GetMethod.GetAccount().GetOnchain()); err != nil {
			return err.result()
		}
		return (&callError{code: pbtvm.ErrorCode_ERROR_CODE_ACCOUNT_NOT_ACTIVE, message: "stub: the account is none"}).result()
	case *pbtvm.Call_Emulate:
		onchain, ok := kind.Emulate.GetAccount().GetSource().(*pbtvm.AccountSource_Onchain)
		if kind.Emulate.GetAccount() != nil && !ok {
			return (&callError{code: pbtvm.ErrorCode_ERROR_CODE_EMULATION_FAILED, message: "stub: given states are not executed"}).result()
		}
		if ok {
			if _, err := b.snapshot(block, onchain.Onchain); err != nil {
				return err.result()
			}
		}
		return &pbtvm.CallResult{Outcome: &pbtvm.CallResult_Emulate{Emulate: &pbtvm.EmulateResult{
			Outcome: &pbtvm.EmulateResult_ExternalNotAccepted{ExternalNotAccepted: &pbtvm.ExternalNotAccepted{}},
		}}}
	}
	return invalid("call without kind").result()
}

// The snapshot of a nonexistent account at the point of ref.
func (b *StubBackend) snapshot(block *pbtvm.BlockRequest, ref *pbstate.AccountRef) (*pbstate.AccountSnapshot, *callError) {
	seqno, rootHash := block.GetMcSeqno(), block.GetMcRootHash()
	switch point := ref.GetAt().GetPoint().(type) {
	case *pbstate.At_BeforeBlock:
		seqno, rootHash = seqno-1, nil
	case *pbstate.At_BeforeTx:
		return nil, invalid("stub: the account has no transaction with lt %d in block %d", point.BeforeTx, seqno)
	case *pbstate.At_AfterTx:
		return nil, invalid("stub: the account has no transaction with lt %d in block %d", point.AfterTx, seqno)
	}
	return &pbstate.AccountSnapshot{
		Source: proto.Clone(ref).(*pbstate.AccountRef),
		Value: &pbstate.AccountValue{
			Account:         &pbton.Account{Kind: &pbton.Account_AccountNone{AccountNone: &pbton.Account_None{}}},
			ShardAccountBoc: accountNoneShardAccountBoc,
			LastTransHash:   make([]byte, 32),
		},
		Block: &pbton.BlockIdExt{
			ShardId:  &pbton.ShardIdent{WorkchainId: -1},
			SeqNo:    seqno,
			RootHash: rootHash,
		},
		GenUtime: block.GetMcGenUtime(),
	}, nil
}

// Minimal two's complement big-endian of v.
func int257(v int64) []byte {
	b := []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	for len(b) > 1 && ((b[0] == 0x00 && b[1]&0x80 == 0) || (b[0] == 0xff && b[1]&0x80 != 0)) {
		b = b[1:]
	}
	if !minimalInt257(b) {
		panic(fmt.Sprintf("int257 of %d", v))
	}
	return b
}
