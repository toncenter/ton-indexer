package router

import (
	"bytes"
	"context"
	"testing"
	"time"

	pbstate "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/state/v1"
	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var elector = &pbstate.Address{Workchain: -1, AccountId: bytes.Repeat([]byte{0x33}, 32)}

func block(seqno uint32, genUtime time.Time, calls ...*pbtvm.Call) *pbtvm.BlockRequest {
	return &pbtvm.BlockRequest{
		McSeqno:    seqno,
		McRootHash: bytes.Repeat([]byte{0xab}, 32),
		McGenUtime: uint32(genUtime.Unix()),
		Request:    &pbtvm.ExecuteRequest{Calls: calls},
	}
}

func getAccount(address *pbstate.Address, at *pbstate.At) *pbtvm.Call {
	return &pbtvm.Call{Kind: &pbtvm.Call_GetAccount{GetAccount: &pbtvm.GetAccount{
		Account: &pbstate.AccountRef{Address: address, At: at},
	}}}
}

func getMethod(source *pbtvm.AccountSource, stack ...*pbtvm.StackEntry) *pbtvm.Call {
	return &pbtvm.Call{Kind: &pbtvm.Call_GetMethod{GetMethod: &pbtvm.GetMethod{
		Account: source,
		Method:  &pbtvm.GetMethod_Name{Name: "seqno"},
		Stack:   stack,
	}}}
}

func onchain(address *pbstate.Address) *pbtvm.AccountSource {
	return &pbtvm.AccountSource{Source: &pbtvm.AccountSource_Onchain{Onchain: &pbstate.AccountRef{Address: address}}}
}

func errorCode(t *testing.T, result *pbtvm.CallResult) pbtvm.ErrorCode {
	t.Helper()
	if result.GetError() == nil {
		t.Fatalf("expected an error, got %v", result)
	}
	return result.GetError().GetCode()
}

func TestInvalidCallsAreAnsweredWithoutTheBackend(t *testing.T) {
	// The block is not reached, yet invalid calls are answered.
	server := NewServer(&StubBackend{Lag: time.Hour}, zap.NewNop())
	short := &pbstate.Address{Workchain: 0, AccountId: []byte{1}}
	ctx := &pbtvm.ContextOverrides{Now: new(uint32)}
	response, err := server.Execute(context.Background(), block(10, time.Now(),
		&pbtvm.Call{},
		getAccount(short, nil),
		getMethod(nil),
		getMethod(onchain(elector), &pbtvm.StackEntry{Value: &pbtvm.StackEntry_Integer{Integer: []byte{0x00, 0x01}}}),
		&pbtvm.Call{Kind: &pbtvm.Call_Emulate{Emulate: &pbtvm.Emulate{MessageBoc: []byte{1}, Context: ctx}}},
		&pbtvm.Call{Kind: &pbtvm.Call_Emulate{Emulate: &pbtvm.Emulate{MessageBoc: make([]byte, protocol.MaxInputBocSize+1)}}},
	))
	if err != nil {
		t.Fatal(err)
	}
	want := []pbtvm.ErrorCode{
		pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		pbtvm.ErrorCode_ERROR_CODE_TOO_LARGE,
	}
	for i, code := range want {
		if got := errorCode(t, response.Results[i]); got != code {
			t.Errorf("call %d: %v, want %v", i, got, code)
		}
	}
}

func TestUnreachedBlocksAreUnavailable(t *testing.T) {
	server := NewServer(&StubBackend{Lag: time.Hour}, zap.NewNop())
	_, err := server.Execute(context.Background(), block(10, time.Now(), getAccount(elector, nil)))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want Unavailable", err)
	}
	// An old block is reached.
	if _, err := server.Execute(context.Background(), block(10, time.Now().Add(-2*time.Hour), getAccount(elector, nil))); err != nil {
		t.Fatal(err)
	}
}

func TestRequestsOverTheTransportLimitAreRejected(t *testing.T) {
	server := NewServer(&StubBackend{}, zap.NewNop())
	calls := make([]*pbtvm.Call, protocol.MaxCallsPerRequest+1)
	for i := range calls {
		calls[i] = getAccount(elector, nil)
	}
	if _, err := server.Execute(context.Background(), block(10, time.Now(), calls...)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestUnknownFieldsAreNotAnswered(t *testing.T) {
	// A kind of call this router does not know, from a newer SDK: field 99.
	call := &pbtvm.Call{}
	call.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), nil))
	server := NewServer(&StubBackend{}, zap.NewNop())
	if _, err := server.Execute(context.Background(), block(10, time.Now(), getAccount(elector, nil), call)); status.Code(err) != codes.Unimplemented {
		t.Fatalf("got %v, want Unimplemented", err)
	}
}

func TestStubAnswers(t *testing.T) {
	server := NewServer(&StubBackend{}, zap.NewNop())
	given := &pbtvm.AccountSource{Source: &pbtvm.AccountSource_ShardAccountBoc{ShardAccountBoc: []byte{1}}}
	request := block(100, time.Now(),
		getAccount(elector, nil),
		getAccount(elector, &pbstate.At{Point: &pbstate.At_BeforeBlock{BeforeBlock: &emptypb.Empty{}}}),
		getAccount(elector, &pbstate.At{Point: &pbstate.At_BeforeTx{BeforeTx: 5}}),
		getMethod(onchain(elector)),
		getMethod(given),
		&pbtvm.Call{Kind: &pbtvm.Call_Emulate{Emulate: &pbtvm.Emulate{MessageBoc: []byte{1}}}},
	)
	response, err := server.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	after := response.Results[0].GetAccount()
	if after.GetBlock().GetSeqNo() != 100 || !bytes.Equal(after.GetBlock().GetRootHash(), request.McRootHash) ||
		after.GetValue().GetAccount().GetAccountNone() == nil || !bytes.Equal(after.GetValue().GetShardAccountBoc(), accountNoneShardAccountBoc) {
		t.Errorf("after_block: %v", after)
	}
	if before := response.Results[1].GetAccount(); before.GetBlock().GetSeqNo() != 99 {
		t.Errorf("before_block: %v", before)
	}
	if errorCode(t, response.Results[2]) != pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Errorf("before_tx: %v", response.Results[2])
	}
	if errorCode(t, response.Results[3]) != pbtvm.ErrorCode_ERROR_CODE_ACCOUNT_NOT_ACTIVE {
		t.Errorf("get_method on chain: %v", response.Results[3])
	}
	if stack := response.Results[4].GetGetMethod().GetStack(); len(stack) != 1 || !bytes.Equal(stack[0].GetInteger(), []byte{100}) {
		t.Errorf("get_method on a given state: %v", response.Results[4])
	}
	if response.Results[5].GetEmulate().GetExternalNotAccepted() == nil {
		t.Errorf("emulate: %v", response.Results[5])
	}
}

func TestMinimalInt257(t *testing.T) {
	for _, tc := range []struct {
		b    []byte
		want bool
	}{
		{[]byte{0x00}, true},
		{[]byte{0x7f}, true},
		{[]byte{0x00, 0x80}, true},
		{[]byte{0x00, 0x7f}, false},
		{[]byte{0xff, 0x7f}, true},
		{[]byte{0xff, 0x80}, false},
		{append([]byte{0x00, 0x80}, make([]byte, 31)...), true}, // 2^255
		{append([]byte{0x01}, make([]byte, 32)...), false},      // 2^256
		{append([]byte{0xff, 0x7f}, make([]byte, 31)...), true}, // -2^256 + ...
		{nil, false},
	} {
		if got := minimalInt257(tc.b); got != tc.want {
			t.Errorf("%x: %v, want %v", tc.b, got, tc.want)
		}
	}
	if !bytes.Equal(int257(-129), []byte{0xff, 0x7f}) || !bytes.Equal(int257(128), []byte{0x00, 0x80}) {
		t.Error("int257")
	}
}

func TestConfigValidationSelectsTheLowestInvalidParameter(t *testing.T) {
	ctx := &pbtvm.ContextOverrides{ConfigParams: map[int32][]byte{
		-7: nil,
		2:  make([]byte, protocol.MaxInputBocSize+1),
	}}
	for range 1000 {
		err := validateContext(ctx)
		if err == nil || err.code != pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST || err.message != "empty config param -7" {
			t.Fatalf("non-deterministic validation: %v", err)
		}
	}
}

func TestAggregateCallSizeIsLimitedBeforeBackendAccess(t *testing.T) {
	// Each BOC fits, but the entire call does not. The block is not reached,
	// so a missing size check would also incorrectly wait for the backend.
	ctx := &pbtvm.ContextOverrides{}
	for range protocol.MaxCallRequestSize/protocol.MaxInputBocSize + 1 {
		ctx.Libraries = append(ctx.Libraries, make([]byte, protocol.MaxInputBocSize))
	}
	call := &pbtvm.Call{Kind: &pbtvm.Call_Emulate{Emulate: &pbtvm.Emulate{MessageBoc: []byte{1}, Context: ctx}}}
	server := NewServer(&StubBackend{Lag: time.Hour}, zap.NewNop())
	response, err := server.Execute(context.Background(), block(10, time.Now(), call))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(response.Results[0], protocol.CallTooLarge(proto.Size(call))) {
		t.Fatalf("response: %v", response)
	}
}

func TestStackNestingAndTupleSizeAreLimited(t *testing.T) {
	null := &pbtvm.StackEntry{Value: &pbtvm.StackEntry_Null{Null: &emptypb.Empty{}}}
	nested := func(depth int, wrap func(*pbtvm.StackEntry) *pbtvm.StackEntry) *pbtvm.StackEntry {
		entry := null
		for range depth {
			entry = wrap(entry)
		}
		return entry
	}
	tuple := func(item *pbtvm.StackEntry) *pbtvm.StackEntry {
		return &pbtvm.StackEntry{Value: &pbtvm.StackEntry_Tuple{Tuple: &pbtvm.Tuple{Items: []*pbtvm.StackEntry{item}}}}
	}
	list := func(item *pbtvm.StackEntry) *pbtvm.StackEntry {
		return &pbtvm.StackEntry{Value: &pbtvm.StackEntry_List{List: &pbtvm.Tuple{Items: []*pbtvm.StackEntry{null, item}}}}
	}
	wide := &pbtvm.Tuple{}
	for range protocol.MaxTupleSize + 1 {
		wide.Items = append(wide.Items, null)
	}
	long := &pbtvm.Tuple{Items: wide.Items}
	for _, tc := range []struct {
		entry *pbtvm.StackEntry
		valid bool
	}{
		{nested(protocol.MaxStackDepth, tuple), true},
		{nested(protocol.MaxStackDepth+1, tuple), false},
		{nested(protocol.MaxStackDepth, list), true},
		{nested(protocol.MaxStackDepth+1, list), false},
		{&pbtvm.StackEntry{Value: &pbtvm.StackEntry_Tuple{Tuple: wide}}, false},
		{&pbtvm.StackEntry{Value: &pbtvm.StackEntry_List{List: long}}, true},
		{&pbtvm.StackEntry{Value: &pbtvm.StackEntry_List{List: &pbtvm.Tuple{}}}, true},
	} {
		err := validateGetMethod(getMethod(onchain(elector), tc.entry).GetGetMethod())
		if (err == nil) != tc.valid {
			t.Errorf("valid %v: got %v", tc.valid, err)
		}
	}
}
