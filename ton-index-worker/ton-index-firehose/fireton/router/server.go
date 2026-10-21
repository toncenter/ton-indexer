// Package router is ton-tvm-router: the ton.tvm.v1 Executor that fireton
// calls, which checks the calls and has a backend answer them on the block.
package router

import (
	"context"
	"strings"
	"time"

	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Backend answers the calls of masterchain blocks it has reached.
type Backend interface {
	Name() string
	// Reached tells whether the backend has the block, which may be from a
	// request carrying a wrong root hash: Execute checks that.
	Reached(block *pbtvm.BlockRequest) bool
	// Execute answers valid calls, one result each. An error is an error of
	// infrastructure: the call is retried, never answered from it.
	Execute(ctx context.Context, block *pbtvm.BlockRequest, calls []*pbtvm.Call) ([]*pbtvm.CallResult, error)
}

type Server struct {
	pbtvm.UnimplementedExecutorServer
	backend Backend
	logger  *zap.Logger
}

func NewServer(backend Backend, logger *zap.Logger) *Server {
	return &Server{backend: backend, logger: logger}
}

func (s *Server) Execute(ctx context.Context, block *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, error) {
	start := time.Now()
	response, outcome, err := s.execute(ctx, block)
	requests.WithLabelValues(outcome).Inc()
	requestDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	if err == nil {
		lastBlock.Set(float64(block.GetMcSeqno()))
		blockLag.Observe(time.Since(time.Unix(int64(block.GetMcGenUtime()), 0)).Seconds())
	}
	return response, err
}

func (s *Server) execute(ctx context.Context, block *pbtvm.BlockRequest) (*pbtvm.ExecuteResponse, string, error) {
	if block.GetMcSeqno() == 0 || len(block.GetMcRootHash()) != 32 || block.GetRequest() == nil {
		return nil, "bad_request", status.Error(codes.InvalidArgument, "a block request needs mc_seqno, a 32-byte mc_root_hash and a request")
	}
	calls := block.GetRequest().GetCalls()
	// fireton splits batches within the transport limits and knows the
	// fields of its requests: anything else is a client to fix, not a call
	// to answer.
	if len(calls) > protocol.MaxCallsPerRequest {
		return nil, "bad_request", status.Errorf(codes.InvalidArgument, "a request of %d calls is over %d", len(calls), protocol.MaxCallsPerRequest)
	}
	if at := protocol.UnknownField(block); at != "" {
		return nil, "bad_request", status.Errorf(codes.Unimplemented, "%s has a field unknown to this router", at)
	}
	results := make([]*pbtvm.CallResult, len(calls))

	var valid []*pbtvm.Call
	var validIndexes []int
	for i, call := range calls {
		if size := proto.Size(call); size > protocol.MaxCallRequestSize {
			results[i] = protocol.CallTooLarge(size)
			continue
		}
		if err := validateCall(call); err != nil {
			results[i] = err.result()
			continue
		}
		valid = append(valid, call)
		validIndexes = append(validIndexes, i)
	}
	if len(valid) > 0 {
		if !s.backend.Reached(block) {
			return nil, "not_reached", status.Errorf(codes.Unavailable, "block %d is not reached by %s", block.GetMcSeqno(), s.backend.Name())
		}
		answered, err := s.backend.Execute(ctx, block, valid)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				st = status.Newf(codes.Unavailable, "backend %s: %v", s.backend.Name(), err)
			}
			// Unavailable is a block not reached yet, or a backend away: the
			// usual at the head, so not worth a warning.
			if st.Code() != codes.Unavailable {
				s.logger.Warn("backend failed", zap.String("backend", s.backend.Name()), zap.Uint32("block", block.GetMcSeqno()), zap.Error(err))
			}
			return nil, "backend_" + strings.ToLower(st.Code().String()), st.Err()
		}
		if len(answered) != len(valid) {
			return nil, "backend_error", status.Errorf(codes.Internal, "backend %s gave %d results for %d calls", s.backend.Name(), len(answered), len(valid))
		}
		for j, i := range validIndexes {
			results[i] = answered[j]
		}
	}
	return s.finish(calls, results), "ok", nil
}

func (s *Server) finish(calls []*pbtvm.Call, results []*pbtvm.CallResult) *pbtvm.ExecuteResponse {
	for i, result := range results {
		callResults.WithLabelValues(callKind(calls[i]), resultLabel(result)).Inc()
	}
	return &pbtvm.ExecuteResponse{Results: results}
}

func callKind(call *pbtvm.Call) string {
	switch call.GetKind().(type) {
	case *pbtvm.Call_GetMethod:
		return "get_method"
	case *pbtvm.Call_Emulate:
		return "emulate"
	case *pbtvm.Call_GetAccount:
		return "get_account"
	}
	return "none"
}

func resultLabel(result *pbtvm.CallResult) string {
	switch outcome := result.GetOutcome().(type) {
	case *pbtvm.CallResult_Error:
		return outcome.Error.GetCode().String()
	case *pbtvm.CallResult_GetMethod:
		return "get_method"
	case *pbtvm.CallResult_Emulate:
		if outcome.Emulate.GetExternalNotAccepted() != nil {
			return "external_not_accepted"
		}
		return "executed"
	case *pbtvm.CallResult_Account:
		return "account"
	}
	return "none"
}
