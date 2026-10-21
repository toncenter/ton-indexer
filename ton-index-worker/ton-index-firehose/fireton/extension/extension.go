// Package extension is the ton_v1 extension of substreams modules: the
// import ton_v1.execute, which sends a batch of calls of a module, with the
// masterchain block the module is processing, to ton-state-router.
//
// Determinism: the router answers a block only from that block. Whatever it
// cannot answer yet (no backend has reached the block, the router is down) is
// retried for as long as the request of the user lives; any other failure is
// an error of the extension, which substreams treats as non-deterministic.
package extension

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/streamingfast/substreams/metering"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/streamingfast/substreams/wasm"
	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	Namespace = "ton_v1"
	// The key of the router address in the params tier1 sends to tier2.
	paramRouter = "ton_v1_router"

	minBackoff   = 100 * time.Millisecond
	maxBackoff   = 2 * time.Second
	logEveryWait = 10 * time.Second
)

// No deadline per attempt: a batch may run long, and running it again would
// only repeat the work. A dead connection is found by keepalive instead; the
// router permits pings this often.
var keepaliveParams = keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}

// Reconnect as often as requests are retried: with the default backoff of up
// to 2 min, a router back after a minute down is found only half a minute later.
var connectParams = grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: minBackoff, Multiplier: 1.6, Jitter: 0.2, MaxDelay: maxBackoff},
	MinConnectTimeout: 5 * time.Second,
}

// Extensioner gives the ton_v1 namespace to substreams. Only tier1 is
// configured with the router: tier2 gets its address from tier1 with every
// request, as params.
type Extensioner struct {
	params map[string]string
	logger *zap.Logger
	// As in fireeth, connections belong to the extensioner and are shared by
	// every tier2 job, which builds the extensions again.
	mu      sync.Mutex
	clients map[string]*client
}

// New returns the extensioner of a process; routerAddr is empty on a tier2
// process or when the extension is off.
func New(routerAddr string, logger *zap.Logger) *Extensioner {
	params := map[string]string{}
	if routerAddr != "" {
		params[paramRouter] = routerAddr
	}
	return &Extensioner{params: params, logger: logger, clients: make(map[string]*client)}
}

func (e *Extensioner) Params() map[string]string {
	return maps.Clone(e.params)
}

// WASMExtensions is called by tier1 once with its own params and by tier2 for
// every request with the params of that request. Without a router address,
// modules importing ton_v1 do not instantiate.
func (e *Extensioner) WASMExtensions(params map[string]string) (map[string]map[string]wasm.WASMExtension, error) {
	addr := params[paramRouter]
	if addr == "" {
		return map[string]map[string]wasm.WASMExtension{}, nil
	}
	c, err := e.routerClient(addr)
	if err != nil {
		return nil, err
	}
	return map[string]map[string]wasm.WASMExtension{
		Namespace: {"execute": c.execute},
	}, nil
}

type client struct {
	addr     string
	executor pbtvm.ExecutorClient
	logger   *zap.Logger
}

func (e *Extensioner) routerClient(addr string) (*client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithConnectParams(connectParams),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxRPCMessageSize), grpc.MaxCallSendMsgSize(protocol.MaxRPCMessageSize)))
	if err != nil {
		return nil, fmt.Errorf("ton_v1 router %q: %w", addr, err)
	}
	c := &client{addr: addr, executor: pbtvm.NewExecutorClient(conn), logger: e.logger.With(zap.String("router", addr))}
	e.clients[addr] = c
	return c, nil
}

func (c *client) execute(ctx context.Context, requestID string, clock *pbsubstreams.Clock, in []byte) ([]byte, error) {
	start := time.Now()
	out, err := c.executeBlock(ctx, requestID, clock, in)
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	executeCalls.WithLabelValues(outcome).Inc()
	executeDuration.Observe(time.Since(start).Seconds())
	return out, err
}

func (c *client) executeBlock(ctx context.Context, requestID string, clock *pbsubstreams.Clock, in []byte) ([]byte, error) {
	request := &pbtvm.ExecuteRequest{}
	if err := proto.Unmarshal(in, request); err != nil {
		return nil, fmt.Errorf("decoding ExecuteRequest: %w", err)
	}
	if len(request.Calls) == 0 {
		return []byte{}, nil
	}
	if at := protocol.UnknownField(request); at != "" {
		return nil, fmt.Errorf("%s has a field unknown to this fireton: the module needs a newer server", at)
	}
	meterCalls(ctx, request.Calls)
	rootHash, err := hex.DecodeString(clock.Id)
	if err != nil || len(rootHash) != 32 {
		return nil, fmt.Errorf("block id %q is not a hex root hash", clock.Id)
	}
	blockRequest := &pbtvm.BlockRequest{
		McSeqno:    uint32(clock.Number),
		McRootHash: rootHash,
		McGenUtime: uint32(clock.Timestamp.GetSeconds()),
	}

	// The batch goes to the router in requests within the transport limits,
	// in order. A call over the limit of one call is answered here: it might
	// not fit in a request at all.
	response := &pbtvm.ExecuteResponse{}
	for start := 0; start < len(request.Calls); {
		if size := proto.Size(request.Calls[start]); size > protocol.MaxCallRequestSize {
			response.Results = append(response.Results, protocol.CallTooLarge(size))
			start++
			continue
		}
		end, requestSize := start, 0
		for end < len(request.Calls) && end-start < protocol.MaxCallsPerRequest {
			size := proto.Size(request.Calls[end])
			framedSize := protowire.SizeTag(1) + protowire.SizeBytes(size)
			if size > protocol.MaxCallRequestSize || requestSize+framedSize > protocol.MaxBatchRequestSize {
				break
			}
			requestSize += framedSize
			end++
		}
		blockRequest.Request = &pbtvm.ExecuteRequest{Calls: request.Calls[start:end]}
		answered, err := c.executeRequest(ctx, requestID, blockRequest)
		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, answered.Results...)
		start = end
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(response)
}

func (c *client) executeRequest(ctx context.Context, requestID string, blockRequest *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, error) {
	backoff := minBackoff
	lastLog := time.Now()
	for {
		response, err := c.executor.Execute(ctx, blockRequest)
		if err == nil {
			if len(response.Results) != len(blockRequest.Request.Calls) {
				return nil, fmt.Errorf("router gave %d results for %d calls", len(response.Results), len(blockRequest.Request.Calls))
			}
			return response, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("block %d: %w", blockRequest.McSeqno, context.Cause(ctx))
		}
		// Unavailable is what the router cannot answer yet (the block is not
		// reached, the router is down or overloaded); anything else it cannot
		// answer at all. ResourceExhausted is not retried: gRPC also gives it
		// for messages over the size limit.
		if status.Code(err) != codes.Unavailable {
			return nil, fmt.Errorf("block %d: router %s: %w", blockRequest.McSeqno, c.addr, err)
		}
		executeRetries.Inc()
		if time.Since(lastLog) >= logEveryWait {
			c.logger.Info("ton_v1 still waiting for the router",
				zap.Uint32("block", blockRequest.McSeqno), zap.String("request_id", requestID), zap.Error(err))
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("block %d: %w", blockRequest.McSeqno, context.Cause(ctx))
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

// As fireeth meters eth_call: a batch counts for as many calls as it holds,
// whatever the requests to the router and their retries.
func meterCalls(ctx context.Context, calls []*pbtvm.Call) {
	counts := map[string]int{}
	for _, call := range calls {
		switch call.GetKind().(type) {
		case *pbtvm.Call_GetMethod:
			counts["ton_v1_get_method"]++
		case *pbtvm.Call_Emulate:
			counts["ton_v1_emulate"]++
		case *pbtvm.Call_GetAccount:
			counts["ton_v1_get_account"]++
		}
	}
	for kind, n := range counts {
		metering.AddExternalCalls(ctx, kind, n)
	}
}
