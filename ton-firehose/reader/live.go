package reader

import (
	"context"
	"sync/atomic"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/firehose-core/blockpoller"
	"github.com/streamingfast/firehose-core/rpc"
	"go.uber.org/zap"
)

type LiveConfig struct {
	// Poller cursor, so that a restart resumes after the last emitted block.
	StateDir             string
	FirstStreamableBlock uint64
	// Blocks fetched ahead in parallel while catching up.
	FetchBatchSize        int
	PollInterval          time.Duration
	MaxBlockFetchDuration time.Duration
}

// RunLive prints FIRE INIT and then a FIRE BLOCK line per finalized
// masterchain round to stdout, for firecore's reader-node.
func RunLive(source BlockSource, cfg LiveConfig, logger *zap.Logger) error {
	return newLivePoller(source, cfg, logger).Run(cfg.FirstStreamableBlock, nil, cfg.FetchBatchSize)
}

func newLivePoller(source BlockSource, cfg LiveConfig, logger *zap.Logger) *blockpoller.BlockPoller[BlockSource] {
	clients := rpc.NewClients[BlockSource](cfg.MaxBlockFetchDuration, rpc.NewStickyRollingStrategy[BlockSource](), logger)
	clients.Add(source)

	return blockpoller.New[BlockSource](
		&liveFetcher{pollInterval: cfg.PollInterval},
		blockpoller.NewFireBlockHandler(BlockTypeURL),
		clients,
		blockpoller.WithStoringState[BlockSource](cfg.StateDir),
		blockpoller.WithLogger[BlockSource](logger),
	)
}

type liveFetcher struct {
	pollInterval time.Duration
	head         atomic.Uint64
}

var _ blockpoller.BlockFetcher[BlockSource] = (*liveFetcher)(nil)

func (f *liveFetcher) IsBlockAvailable(num uint64) bool {
	return num <= f.head.Load()
}

// Fetch waits until the round is finalized in the source; the poller retries
// it when the fetch deadline expires first.
func (f *liveFetcher) Fetch(ctx context.Context, source BlockSource, num uint64) (*pbbstream.Block, bool, error) {
	for num > f.head.Load() {
		head, err := source.Head(ctx)
		if err != nil {
			return nil, false, err
		}
		f.head.Store(head)
		if num <= head {
			break
		}
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(f.pollInterval):
		}
	}
	blk, err := source.Block(ctx, num)
	return blk, false, err
}
