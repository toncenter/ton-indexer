// gramstat-api serves precomputed charts; it never runs aggregation or writes data.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat/api"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:2008", "private HTTP listener behind nginx")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if os.Getenv("GRAMSTAT_CH_USER") == "" || os.Getenv("GRAMSTAT_CH_PASSWORD") == "" {
		log.Error("dedicated ClickHouse user and password are required")
		os.Exit(1)
	}
	ch, err := gramstat.NewClickHouse(os.Getenv("GRAMSTAT_CH_URL"), os.Getenv("GRAMSTAT_CH_USER"), os.Getenv("GRAMSTAT_CH_PASSWORD"))
	if err != nil {
		log.Error("invalid ClickHouse configuration")
		os.Exit(1)
	}
	server := &http.Server{Addr: *listen, Handler: api.New(api.ClickHouseReader{CH: ch}, log),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Info("Gramstat API listening", "address", *listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
