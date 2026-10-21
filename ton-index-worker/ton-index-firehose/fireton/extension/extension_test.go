package extension

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	pbton "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/sf/ton/type/v1"
	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A router that has not reached the block for its first unavailable calls.
type fakeRouter struct {
	pbtvm.UnimplementedExecutorServer
	unavailable int32
	failWith    codes.Code
	calls       atomic.Int32
	last        atomic.Pointer[pbtvm.BlockRequest]
}

func (r *fakeRouter) Execute(_ context.Context, request *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, error) {
	r.last.Store(request)
	if r.calls.Add(1) <= r.unavailable {
		return nil, status.Error(codes.Unavailable, "not reached")
	}
	if r.failWith != codes.OK {
		return nil, status.Error(r.failWith, "failed")
	}
	results := make([]*pbtvm.CallResult, len(request.GetRequest().GetCalls()))
	for i := range results {
		results[i] = protocol.CallError(pbtvm.ErrorCode_ERROR_CODE_INVALID_REQUEST, "")
	}
	return &pbtvm.ExecuteResponse{Results: results}, nil
}

func serve(t *testing.T, router *fakeRouter) func(context.Context, string, *pbsubstreams.Clock, []byte) ([]byte, error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pbtvm.RegisterExecutorServer(server, router)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	tier1 := New(listener.Addr().String(), zap.NewNop())
	// tier2 is configured only by the params of tier1.
	extensions, err := New("", zap.NewNop()).WASMExtensions(tier1.Params())
	if err != nil {
		t.Fatal(err)
	}
	return extensions[Namespace]["execute"]
}

var clock = &pbsubstreams.Clock{
	Id:        hex.EncodeToString(bytes.Repeat([]byte{0xab}, 32)),
	Number:    100,
	Timestamp: timestamppb.New(time.Unix(1_800_000_000, 0)),
}

func request(t *testing.T) []byte {
	t.Helper()
	in, err := proto.Marshal(&pbtvm.ExecuteRequest{Calls: []*pbtvm.Call{{}, {}}})
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func TestWaitsUntilTheRouterAnswers(t *testing.T) {
	router := &fakeRouter{unavailable: 3}
	execute := serve(t, router)
	out, err := execute(context.Background(), "test", clock, request(t))
	if err != nil {
		t.Fatal(err)
	}
	response := &pbtvm.ExecuteResponse{}
	if err := proto.Unmarshal(out, response); err != nil || len(response.Results) != 2 {
		t.Fatalf("response %v, %v", response, err)
	}
	if router.calls.Load() != 4 {
		t.Errorf("%d calls of the router, want 4", router.calls.Load())
	}
	last := router.last.Load()
	if last.GetMcSeqno() != 100 || last.GetMcGenUtime() != 1_800_000_000 || !bytes.Equal(last.GetMcRootHash(), bytes.Repeat([]byte{0xab}, 32)) {
		t.Errorf("block request %v", last)
	}
}

func TestOtherFailuresAreErrorsAtOnce(t *testing.T) {
	// ResourceExhausted is also what gRPC gives for a message over the limit,
	// DeadlineExceeded comes from no deadline of ours.
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.ResourceExhausted, codes.DeadlineExceeded, codes.Unimplemented} {
		router := &fakeRouter{failWith: code}
		execute := serve(t, router)
		if _, err := execute(context.Background(), "test", clock, request(t)); err == nil {
			t.Fatalf("%v: expected an error", code)
		}
		if router.calls.Load() != 1 {
			t.Errorf("%v: %d calls of the router, want 1", code, router.calls.Load())
		}
	}
}

func TestWaitingEndsWithTheRequest(t *testing.T) {
	execute := serve(t, &fakeRouter{unavailable: 1 << 30})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := execute(ctx, "test", clock, request(t)); err == nil {
		t.Fatal("expected an error")
	}
}

func TestWithoutRouterThereIsNoNamespace(t *testing.T) {
	extensions, err := New("", zap.NewNop()).WASMExtensions(nil)
	if err != nil || len(extensions) != 0 {
		t.Fatalf("%v, %v", extensions, err)
	}
}

func TestUnknownFieldsAreErrors(t *testing.T) {
	// A kind of call from a newer SDK, field 99: not an answer to cache.
	unknown := &pbtvm.Call{}
	unknown.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), nil))
	in, err := proto.Marshal(&pbtvm.ExecuteRequest{Calls: []*pbtvm.Call{{}, unknown}})
	if err != nil {
		t.Fatal(err)
	}
	router := &fakeRouter{}
	if _, err := serve(t, router)(context.Background(), "test", clock, in); err == nil {
		t.Fatal("expected an error")
	}
	if router.calls.Load() != 0 {
		t.Errorf("%d calls of the router, want 0", router.calls.Load())
	}
}

type executorFunc func(context.Context, *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, error)

func (f executorFunc) Execute(ctx context.Context, req *pbtvm.BlockRequest, _ ...grpc.CallOption) (*pbtvm.ExecuteResponse, error) {
	return f(ctx, req)
}

func TestResponseEncodingIsDeterministic(t *testing.T) {
	response := &pbtvm.ExecuteResponse{Results: []*pbtvm.CallResult{{Outcome: &pbtvm.CallResult_Emulate{Emulate: &pbtvm.EmulateResult{
		Outcome: &pbtvm.EmulateResult_Executed{Executed: &pbtvm.ExecutedTransaction{Transaction: &pbton.Transaction{
			TotalFees: &pbton.CurrencyCollection{Other: map[uint32]string{1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7", 8: "8"}},
		}}},
	}}}}}
	c := &client{logger: zap.NewNop(), executor: executorFunc(func(context.Context, *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, error) {
		return response, nil
	})}
	in, _ := proto.Marshal(&pbtvm.ExecuteRequest{Calls: []*pbtvm.Call{{}}})
	want, _ := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	for range 100 {
		got, err := c.executeBlock(context.Background(), "test", clock, in)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("non-canonical response %x, err %v", got, err)
		}
	}
}

// A router that answers every emulation with its now as the exit code, so
// that the results tell which call they are for.
func markerRouter(t *testing.T, requests *[]int) *client {
	return &client{logger: zap.NewNop(), executor: executorFunc(func(_ context.Context, req *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, error) {
		*requests = append(*requests, len(req.Request.Calls))
		if len(req.Request.Calls) > protocol.MaxCallsPerRequest || proto.Size(req.Request) > protocol.MaxBatchRequestSize {
			t.Fatal("transport limit exceeded")
		}
		if req.McSeqno != uint32(clock.Number) || hex.EncodeToString(req.McRootHash) != clock.Id {
			t.Fatal("lost block identity while splitting")
		}
		response := &pbtvm.ExecuteResponse{}
		for _, call := range req.Request.Calls {
			if proto.Size(call) > protocol.MaxCallRequestSize {
				t.Fatal("oversized call sent to the router")
			}
			response.Results = append(response.Results, &pbtvm.CallResult{Outcome: &pbtvm.CallResult_Emulate{Emulate: &pbtvm.EmulateResult{
				Outcome: &pbtvm.EmulateResult_ExternalNotAccepted{ExternalNotAccepted: &pbtvm.ExternalNotAccepted{VmExitCode: int32(call.GetEmulate().GetContext().GetNow())}},
			}}})
		}
		return response, nil
	})}
}

func emulations(n int, boc []byte) *pbtvm.ExecuteRequest {
	request := &pbtvm.ExecuteRequest{}
	for i := 0; i < n; i++ {
		marker := uint32(i + 1)
		request.Calls = append(request.Calls, &pbtvm.Call{Kind: &pbtvm.Call_Emulate{Emulate: &pbtvm.Emulate{
			MessageBoc: boc, Context: &pbtvm.ContextOverrides{Now: &marker},
		}}})
	}
	return request
}

func execute(t *testing.T, c *client, request *pbtvm.ExecuteRequest) []*pbtvm.CallResult {
	t.Helper()
	in, _ := proto.Marshal(request)
	out, err := c.executeBlock(context.Background(), "test", clock, in)
	if err != nil {
		t.Fatal(err)
	}
	response := &pbtvm.ExecuteResponse{}
	if err := proto.Unmarshal(out, response); err != nil || len(response.Results) != len(request.Calls) {
		t.Fatalf("response: %v", err)
	}
	return response.Results
}

func TestSplitsBySizeAndKeepsPerCallErrorsInOrder(t *testing.T) {
	request := emulations(20, make([]byte, protocol.MaxInputBocSize))
	request.Calls[17].GetEmulate().MessageBoc = make([]byte, protocol.MaxCallRequestSize+1)
	var requests []int
	results := execute(t, markerRouter(t, &requests), request)
	if len(requests) < 2 {
		t.Fatal("request was not split")
	}
	for i, result := range results {
		if i == 17 {
			if !proto.Equal(result, protocol.CallTooLarge(proto.Size(request.Calls[i]))) {
				t.Fatalf("oversized call: %v", result)
			}
		} else if result.GetEmulate().GetExternalNotAccepted().GetVmExitCode() != int32(i+1) {
			t.Fatalf("result %d out of order: %v", i, result)
		}
	}
}

func TestSplitsByCount(t *testing.T) {
	request := emulations(2*protocol.MaxCallsPerRequest+1, []byte{1})
	var requests []int
	results := execute(t, markerRouter(t, &requests), request)
	if len(requests) != 3 || requests[0] != protocol.MaxCallsPerRequest || requests[2] != 1 {
		t.Fatalf("requests of %v calls", requests)
	}
	for i, result := range results {
		if result.GetEmulate().GetExternalNotAccepted().GetVmExitCode() != int32(i+1) {
			t.Fatalf("result %d out of order: %v", i, result)
		}
	}
}

func TestTier2JobsShareConnectionsWithinTheExtensioner(t *testing.T) {
	e := New("", zap.NewNop())
	params := New("localhost:10040", zap.NewNop()).Params()
	for range 2 {
		if _, err := e.WASMExtensions(params); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.clients) != 1 {
		t.Fatalf("%d connections for two jobs", len(e.clients))
	}
}
