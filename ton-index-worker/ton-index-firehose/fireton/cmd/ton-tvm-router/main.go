// ton-tvm-router serves ton.tvm.v1 Executor to fireton, with one backend: a
// ton-tvm-executor (--executor-addr), or else the stub (router.StubBackend).
package main

import (
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pbtvm "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/pb/ton/tvm/v1"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/protocol"
	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/router"
)

func main() {
	listenAddr := flag.String("listen-addr", "127.0.0.1:10040", "gRPC address of the Executor service")
	metricsAddr := flag.String("metrics-addr", "127.0.0.1:9140", "HTTP address of /metrics, empty for none")
	executorAddr := flag.String("executor-addr", "", "gRPC address of a ton-tvm-executor; empty for the stub backend")
	stubLag := flag.Duration("stub-lag", 0, "The stub backend reaches each block this long after its gen_utime")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Fatal("listen", zap.Error(err))
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(protocol.MaxRPCMessageSize), grpc.MaxSendMsgSize(protocol.MaxRPCMessageSize),
		// fireton pings every 30 s during a request, which has no deadline;
		// the default policy closes connections pinged more than every 5 min.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second}))
	var backend router.Backend = &router.StubBackend{Lag: *stubLag}
	if *executorAddr != "" {
		executor, err := router.NewExecutorBackend(*executorAddr)
		if err != nil {
			logger.Fatal("executor", zap.Error(err))
		}
		backend = executor
	}
	pbtvm.RegisterExecutorServer(server, router.NewServer(backend, logger))

	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Fatal("metrics", zap.Error(err))
			}
		}()
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		server.GracefulStop()
	}()

	logger.Info("serving", zap.String("listen_addr", *listenAddr), zap.String("backend", backend.Name()), zap.Duration("stub_lag", *stubLag))
	if err := server.Serve(listener); err != nil {
		logger.Fatal("serve", zap.Error(err))
	}
}
