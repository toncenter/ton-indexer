package router

import (
	"context"
	"fmt"
	"time"

	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ExecutorBackend sends calls to a ton-tvm-executor over gRPC. The executor
// knows which blocks it has: it answers Unavailable for one it has not
// reached, which the router passes on, so Reached is always true here.
type ExecutorBackend struct {
	addr   string
	client pbtvm.ExecutorClient
}

func NewExecutorBackend(addr string) (*ExecutorBackend, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// As fireton to the router: no deadline per request, keepalive to find
		// a dead connection, and reconnecting as often as requests are retried.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxRPCMessageSize), grpc.MaxCallSendMsgSize(protocol.MaxRPCMessageSize)))
	if err != nil {
		return nil, fmt.Errorf("executor %q: %w", addr, err)
	}
	return &ExecutorBackend{addr: addr, client: pbtvm.NewExecutorClient(conn)}, nil
}

func (b *ExecutorBackend) Name() string {
	return "executor " + b.addr
}

func (b *ExecutorBackend) Reached(*pbtvm.BlockRequest) bool {
	return true
}

func (b *ExecutorBackend) Execute(ctx context.Context, block *pbtvm.BlockRequest, calls []*pbtvm.Call) ([]*pbtvm.CallResult, error) {
	response, err := b.client.Execute(ctx, &pbtvm.BlockRequest{
		McSeqno:    block.GetMcSeqno(),
		McRootHash: block.GetMcRootHash(),
		McGenUtime: block.GetMcGenUtime(),
		Request:    &pbtvm.ExecuteRequest{Calls: calls},
	})
	if err != nil {
		return nil, err
	}
	return response.GetResults(), nil
}
