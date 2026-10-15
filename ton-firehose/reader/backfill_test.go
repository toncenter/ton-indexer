package reader

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeSource serves chained blocks up to head, failing once on failOnce.
type fakeSource struct {
	head     uint64
	mu       sync.Mutex
	failOnce map[uint64]bool
}

func fakeId(num uint64) string { return fmt.Sprintf("%064x", num) }

func (s *fakeSource) Head(context.Context) (uint64, error) { return s.head, nil }

func (s *fakeSource) Block(_ context.Context, num uint64) (*pbbstream.Block, error) {
	s.mu.Lock()
	fail := s.failOnce[num]
	delete(s.failOnce, num)
	s.mu.Unlock()
	if fail {
		return nil, errors.New("transient")
	}
	time.Sleep(time.Duration(rand.IntN(3)) * time.Millisecond)
	return &pbbstream.Block{
		Number: num, Id: fakeId(num), ParentNum: num - 1, ParentId: fakeId(num - 1), LibNum: num,
		Timestamp: timestamppb.New(time.Unix(int64(num), 0)),
		Payload:   &anypb.Any{TypeUrl: BlockTypeURL, Value: []byte{byte(num)}},
	}, nil
}

func TestFetchInOrder(t *testing.T) {
	source := &fakeSource{head: 1000}
	var got []uint64
	err := fetchInOrder(context.Background(), 10, 500, 16,
		func(ctx context.Context, num uint64) (*pbbstream.Block, error) { return source.Block(ctx, num) },
		func(blk *pbbstream.Block) error {
			got = append(got, blk.Number)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for i, num := range got {
		if num != uint64(10+i) {
			t.Fatalf("block %d delivered at position %d", num, i)
		}
	}
	if len(got) != 490 {
		t.Fatalf("delivered %d blocks", len(got))
	}

	boom := errors.New("boom")
	err = fetchInOrder(context.Background(), 0, 1000, 8,
		func(ctx context.Context, num uint64) (*pbbstream.Block, error) {
			if num == 50 {
				return nil, boom
			}
			return source.Block(ctx, num)
		},
		func(*pbbstream.Block) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestBackfillWritesBundles(t *testing.T) {
	storeURL := "file://" + t.TempDir()
	source := &fakeSource{head: 1000, failOnce: map[uint64]bool{150: true}}
	cfg := BackfillConfig{Start: 100, Stop: 300, MergedBlocksStoreURL: storeURL, FirstStreamableBlock: 1, Concurrency: 8, FetchRetries: 1}
	if err := Backfill(context.Background(), source, cfg, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	store, err := dstore.NewDBinStore(storeURL)
	if err != nil {
		t.Fatal(err)
	}
	for _, base := range []uint64{100, 200} {
		r, err := store.OpenObject(context.Background(), fmt.Sprintf("%010d", base))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := bstream.NewDBinBlockReader(r)
		if err != nil {
			t.Fatal(err)
		}
		for want := base; want < base+100; want++ {
			blk, err := reader.Read()
			if err != nil {
				t.Fatalf("bundle %d: reading block %d: %v", base, want, err)
			}
			if blk.Number != want {
				t.Fatalf("bundle %d: got block %d, want %d", base, blk.Number, want)
			}
		}
		r.Close()
	}
}

func TestBackfillRejectsPartialBundles(t *testing.T) {
	source := &fakeSource{head: 1000}
	for _, cfg := range []BackfillConfig{
		{Start: 150, Stop: 300, FirstStreamableBlock: 1},
		{Start: 100, Stop: 250, FirstStreamableBlock: 1},
		{Start: 100, Stop: 1100, FirstStreamableBlock: 1},
	} {
		cfg.MergedBlocksStoreURL = "file://" + t.TempDir()
		if err := Backfill(context.Background(), source, cfg, zap.NewNop()); err == nil {
			t.Errorf("range [%d, %d) is accepted", cfg.Start, cfg.Stop)
		}
	}
}

func TestBackfillDetectsBrokenChain(t *testing.T) {
	source := &brokenChainSource{fakeSource{head: 1000}}
	cfg := BackfillConfig{Start: 100, Stop: 200, MergedBlocksStoreURL: "file://" + t.TempDir(), FirstStreamableBlock: 1, Concurrency: 4}
	if err := Backfill(context.Background(), source, cfg, zap.NewNop()); err == nil {
		t.Error("a parent mismatch is accepted")
	}
}

type brokenChainSource struct{ fakeSource }

func (s *brokenChainSource) Block(ctx context.Context, num uint64) (*pbbstream.Block, error) {
	blk, err := s.fakeSource.Block(ctx, num)
	if err == nil && num == 142 {
		blk.ParentId = fakeId(7)
	}
	return blk, err
}

func TestLiveFetcherWaitsForHead(t *testing.T) {
	source := &fakeSource{head: 5}
	f := &liveFetcher{pollInterval: time.Millisecond}

	blk, _, err := f.Fetch(context.Background(), source, 5)
	if err != nil || blk.Number != 5 {
		t.Fatalf("fetching an available block: %v, %v", blk, err)
	}
	if !f.IsBlockAvailable(5) || f.IsBlockAvailable(6) {
		t.Error("availability does not follow the head")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := f.Fetch(ctx, source, 6); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("fetching above the head: err = %v", err)
	}
}
