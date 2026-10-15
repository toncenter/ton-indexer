// ton-firehose-reader turns the indexer database into Firehose blocks of type
// sf.ton.type.v1.Block, one per masterchain seqno.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/toncenter/ton-indexer/ton-firehose/reader"
	"github.com/toncenter/ton-indexer/ton-index-go/index/crud"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	// stdout carries the FIRE protocol in live mode, logs go to stderr.
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := newRootCmd(logger).Execute(); err != nil {
		logger.Fatal("reader failed", zap.Error(err))
	}
}

type sourceFlags struct {
	pgDsn                 string
	pgHotDsn              string
	maxConns              int
	hotColdSplitTTL       time.Duration
	kvrocksAddr           string
	kvrocksSentinels      string
	kvrocksSentinelMaster string
	kvrocksUser           string
	kvrocksPassword       string
	kvrocksDb             int
	queryTimeout          time.Duration
	pageSize              int
	firstStreamableBlock  uint64
}

func (f *sourceFlags) register(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()
	flags.StringVar(&f.pgDsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string (the cold database with hot/cold split)")
	flags.StringVar(&f.pgHotDsn, "pg-hot", "", "PostgreSQL connection string of the hot database, enables hot/cold routing")
	flags.IntVar(&f.maxConns, "maxconns", 16, "PostgreSQL max connections per pool")
	flags.DurationVar(&f.hotColdSplitTTL, "hot-cold-split-ttl", 2*time.Second, "Hot/cold split-point cache TTL")
	flags.StringVar(&f.kvrocksAddr, "kvrocks", "", "Kvrocks address or Redis URL, when message contents and account states are in Kvrocks")
	flags.StringVar(&f.kvrocksSentinels, "kvrocks-sentinels", "", "Comma-separated Kvrocks Sentinel addresses")
	flags.StringVar(&f.kvrocksSentinelMaster, "kvrocks-sentinel-master", "", "Kvrocks Sentinel master name")
	flags.StringVar(&f.kvrocksUser, "kvrocks-user", "", "Kvrocks username")
	flags.StringVar(&f.kvrocksPassword, "kvrocks-password", "", "Kvrocks password")
	flags.IntVar(&f.kvrocksDb, "kvrocks-db", 0, "Kvrocks database number")
	flags.DurationVar(&f.queryTimeout, "query-timeout", 30*time.Second, "Timeout of a single database query")
	flags.IntVar(&f.pageSize, "page-size", 1000, "Transactions read per query")
	flags.Uint64Var(&f.firstStreamableBlock, "first-streamable-block", 1, "First masterchain seqno of the chain, same as firecore --common-first-streamable-block")
}

func (f *sourceFlags) open() (*reader.Source, error) {
	cfg := reader.Config{
		PgDsn:                f.pgDsn,
		PgHotDsn:             f.pgHotDsn,
		MaxConns:             f.maxConns,
		HotColdSplitTTL:      f.hotColdSplitTTL,
		QueryTimeout:         f.queryTimeout,
		PageSize:             f.pageSize,
		FirstStreamableBlock: f.firstStreamableBlock,
	}
	if f.kvrocksAddr != "" || f.kvrocksSentinels != "" {
		// Always the master: replica reads would trade completeness for load.
		cfg.Kvrocks = &crud.KvrocksConfig{
			Addr:               f.kvrocksAddr,
			SentinelAddrs:      crud.ParseKvrocksSentinelAddrs(f.kvrocksSentinels),
			SentinelMasterName: f.kvrocksSentinelMaster,
			Username:           f.kvrocksUser,
			Password:           f.kvrocksPassword,
			DB:                 f.kvrocksDb,
		}
	}
	return reader.NewSource(cfg)
}

func newRootCmd(logger *zap.Logger) *cobra.Command {
	src := &sourceFlags{}
	root := &cobra.Command{
		Use:           "ton-firehose-reader",
		Short:         "Firehose reader for TON on top of the indexer database",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	src.register(root)
	root.AddCommand(newPollCmd(src, logger), newRangeCmd(src, logger), newPrintCmd(src))
	return root
}

func newPollCmd(src *sourceFlags, logger *zap.Logger) *cobra.Command {
	cfg := reader.LiveConfig{}
	cmd := &cobra.Command{
		Use:   "poll",
		Short: "Follow finalized masterchain blocks and print the FIRE protocol to stdout (run under firecore reader-node)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			source, err := src.open()
			if err != nil {
				return err
			}
			defer source.Close()
			cfg.FirstStreamableBlock = src.firstStreamableBlock
			return reader.RunLive(source, cfg, logger)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.StateDir, "state-dir", "./poller-state", "Directory of the poller cursor")
	flags.IntVar(&cfg.FetchBatchSize, "fetch-batch-size", 10, "Blocks fetched ahead in parallel while catching up")
	flags.DurationVar(&cfg.PollInterval, "poll-interval", 200*time.Millisecond, "Delay between checks of finalized_mc_seqno at the head")
	flags.DurationVar(&cfg.MaxBlockFetchDuration, "max-block-fetch-duration", 60*time.Second, "Deadline of a single block fetch, retried after")
	return cmd
}

func newRangeCmd(src *sourceFlags, logger *zap.Logger) *cobra.Command {
	cfg := reader.BackfillConfig{}
	cmd := &cobra.Command{
		Use:   "range <start> <stop>",
		Short: "Write merged-blocks bundles of masterchain seqnos [start, stop) to the store",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if cfg.Start, err = strconv.ParseUint(args[0], 10, 64); err != nil {
				return fmt.Errorf("parsing start: %w", err)
			}
			if cfg.Stop, err = strconv.ParseUint(args[1], 10, 64); err != nil {
				return fmt.Errorf("parsing stop: %w", err)
			}
			source, err := src.open()
			if err != nil {
				return err
			}
			defer source.Close()
			cfg.FirstStreamableBlock = src.firstStreamableBlock

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return reader.Backfill(ctx, source, cfg, logger)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.MergedBlocksStoreURL, "merged-blocks-store-url", "", "Merged blocks store, same as firecore --common-merged-blocks-store-url")
	flags.IntVar(&cfg.Concurrency, "concurrency", 8, "Blocks fetched in parallel")
	flags.IntVar(&cfg.FetchRetries, "fetch-retries", 5, "Retries of a failed block fetch")
	_ = cmd.MarkFlagRequired("merged-blocks-store-url")
	return cmd
}

func newPrintCmd(src *sourceFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "print <mc_seqno>",
		Short: "Print the block of a masterchain seqno as JSON, to compare with API v3",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			seqno, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("parsing mc_seqno: %w", err)
			}
			source, err := src.open()
			if err != nil {
				return err
			}
			defer source.Close()
			blk, err := source.TonBlock(seqno)
			if err != nil {
				return err
			}
			out, err := protojson.MarshalOptions{UseProtoNames: true, Multiline: true}.Marshal(blk)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return err
		},
	}
}
