package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	firecore "github.com/streamingfast/firehose-core"
	"go.uber.org/zap"
)

type BackfillConfig struct {
	// [Start, Stop) masterchain seqnos. Start is a bundle boundary or the first
	// streamable block, Stop is a bundle boundary, so only whole bundles are written.
	Start, Stop          uint64
	MergedBlocksStoreURL string
	FirstStreamableBlock uint64
	Concurrency          int
	FetchRetries         int
}

// Backfill writes the merged-blocks bundles of a historical range straight to
// the store, bypassing reader-node and merger. Ranges are independent, so
// several processes can fill disjoint ranges in parallel.
func Backfill(ctx context.Context, source BlockSource, cfg BackfillConfig, logger *zap.Logger) error {
	bundleSize := bstream.DefaultMergedBlocksBundleSize
	if cfg.Start%bundleSize != 0 && cfg.Start != cfg.FirstStreamableBlock {
		return fmt.Errorf("start %d is neither a multiple of %d nor the first streamable block", cfg.Start, bundleSize)
	}
	if cfg.Stop%bundleSize != 0 || cfg.Stop <= cfg.Start {
		return fmt.Errorf("stop %d must be a multiple of %d above start", cfg.Stop, bundleSize)
	}
	head, err := source.Head(ctx)
	if err != nil {
		return err
	}
	if cfg.Stop-1 > head {
		return fmt.Errorf("range ends at %d, but the source is finalized up to %d", cfg.Stop-1, head)
	}

	store, err := dstore.NewDBinStore(cfg.MergedBlocksStoreURL)
	if err != nil {
		return fmt.Errorf("opening merged blocks store: %w", err)
	}
	bstream.GetProtocolFirstStreamableBlock = cfg.FirstStreamableBlock
	writer := &firecore.MergedBlocksWriter{
		Store:       store,
		LowBlockNum: firecore.LowBoundary(cfg.Start),
		Logger:      logger,
	}

	var prev *pbbstream.Block
	return fetchInOrder(ctx, cfg.Start, cfg.Stop, cfg.Concurrency,
		func(ctx context.Context, num uint64) (*pbbstream.Block, error) {
			return fetchWithRetries(ctx, source, num, cfg.FetchRetries, logger)
		},
		func(blk *pbbstream.Block) error {
			// Bundles bypass the forkable checks, so verify the chain here.
			if prev != nil && blk.ParentId != prev.Id {
				return fmt.Errorf("block %d parent %s does not match block %d id %s", blk.Number, blk.ParentId, prev.Number, prev.Id)
			}
			prev = blk
			return writer.ProcessBlock(blk, nil)
		})
}

// fetchInOrder fetches [start, stop) with up to concurrency fetches in flight
// and hands the blocks to handle in order.
func fetchInOrder(
	ctx context.Context,
	start, stop uint64,
	concurrency int,
	fetch func(context.Context, uint64) (*pbbstream.Block, error),
	handle func(*pbbstream.Block) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		blk *pbbstream.Block
		err error
	}
	pending := make(chan chan result, max(concurrency, 1))
	go func() {
		defer close(pending)
		for num := start; num < stop; num++ {
			res := make(chan result, 1)
			select {
			case pending <- res:
			case <-ctx.Done():
				return
			}
			go func() {
				blk, err := fetch(ctx, num)
				res <- result{blk, err}
			}()
		}
	}()

	for res := range pending {
		r := <-res
		if r.err != nil {
			return r.err
		}
		if err := handle(r.blk); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func fetchWithRetries(ctx context.Context, source BlockSource, num uint64, retries int, logger *zap.Logger) (*pbbstream.Block, error) {
	delay := time.Second
	for attempt := 0; ; attempt++ {
		blk, err := source.Block(ctx, num)
		if err == nil || attempt >= retries {
			return blk, err
		}
		logger.Warn("fetching block failed, retrying", zap.Uint64("block_num", num), zap.Int("attempt", attempt+1), zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
}
