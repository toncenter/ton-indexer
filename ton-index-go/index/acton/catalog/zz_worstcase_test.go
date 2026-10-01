package catalog_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	tolkabi "github.com/ton-blockchain/tolk-abi-to-go"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func trivialBinding() *tolkabi.Binding {
	c := tolkabi.StructCodec(nil, nil, 0)
	return tolkabi.NewBinding(tolkabi.TypeInfo{Name: "probe"}, &c, "")
}

func b64(c *cell.Cell) string { return base64.StdEncoding.EncodeToString(c.ToBOC()) }

// skip-DAG: cell[i] refs {i+2, i+1}. Skip-ref first => shallow reaches precede
// deep ones => validateCell re-expands nodes at ever-deeper recorded depths.
// Tree depth == n-1, so n is capped near MaxDepth(=128)+1.
func buildSkipDAG(n int) *cell.Cell { return buildSkipDAGSalt(n, 0) }

// salt makes the chain's cells byte-distinct so the BOC cannot dedup them.
func buildSkipDAGSalt(n, salt int) *cell.Cell {
	cells := make([]*cell.Cell, n)
	for i := n - 1; i >= 0; i-- {
		b := cell.BeginCell()
		b.MustStoreUInt(uint64(salt), 32)
		b.MustStoreUInt(uint64(i), 8)
		if i+2 < n {
			b.MustStoreRef(cells[i+2])
		}
		if i+1 < n {
			b.MustStoreRef(cells[i+1])
		}
		cells[i] = b.EndCell()
	}
	return cells[0]
}

// Fan a shallow 4-ary tree (height h) over `count` independent skip-DAGs of
// length s. Total tree depth = h + s. Shares one skip-DAG cell object across
// all leaves? No: each leaf is a *distinct* skip-DAG so validateCell cannot
// dedup them. Returns root and the real distinct-cell count.
func buildForest(count, s, h int) (*cell.Cell, int) {
	total := 0
	leaves := make([]*cell.Cell, count)
	for i := range leaves {
		leaves[i] = buildSkipDAGSalt(s, i+1)
		total += s
	}
	level := leaves
	for level_depth := 0; level_depth < h; level_depth++ {
		var next []*cell.Cell
		for i := 0; i < len(level); i += 4 {
			b := cell.BeginCell()
			end := i + 4
			if end > len(level) {
				end = len(level)
			}
			for _, c := range level[i:end] {
				b.MustStoreRef(c)
			}
			next = append(next, b.EndCell())
			total++
		}
		level = next
	}
	// collapse whatever remains under a single root (<=4 refs assumed)
	if len(level) == 1 {
		return level[0], total
	}
	rb := cell.BeginCell()
	for _, c := range level {
		rb.MustStoreRef(c)
	}
	total++
	return rb.EndCell(), total
}

func timeDecode(bind *tolkabi.Binding, boc string, reps int) (time.Duration, string) {
	root, err := tolkabi.DecodeBOC(boc)
	if err != nil {
		return 0, "parse: " + err.Error()
	}
	// warm
	_, de := bind.Decode(root)
	best := time.Hour
	for r := 0; r < reps; r++ {
		root, _ = tolkabi.DecodeBOC(boc)
		t0 := time.Now()
		bind.Decode(root)
		if d := time.Since(t0); d < best {
			best = d
		}
	}
	msg := ""
	if de != nil {
		msg = de.Error()
	}
	return best, msg
}

func TestSkipDAGSweep(t *testing.T) {
	bind := trivialBinding()
	for _, n := range []int{100, 120, 124, 126, 127, 128, 129} {
		root := buildSkipDAG(n)
		s := b64(root)
		dt, msg := timeDecode(bind, s, 5)
		fmt.Printf("skipDAG n=%-4d boc=%-5d decode=%-12s err=%q\n", n, len(root.ToBOC()), dt, msg)
	}
}

// 3-ref skip chain: cell[i] refs {i+3, i+2, i+1} (widest-skip first). More
// re-expansion paths per node; depth still == n-1.
func buildSkip3Salt(n, salt int) *cell.Cell {
	cells := make([]*cell.Cell, n)
	for i := n - 1; i >= 0; i-- {
		b := cell.BeginCell()
		b.MustStoreUInt(uint64(salt), 32)
		b.MustStoreUInt(uint64(i), 8)
		for _, d := range []int{3, 2, 1} {
			if i+d < n {
				b.MustStoreRef(cells[i+d])
			}
		}
		cells[i] = b.EndCell()
	}
	return cells[0]
}

func buildForest3(count, s, h int) (*cell.Cell, int) {
	total := 0
	level := make([]*cell.Cell, count)
	for i := range level {
		level[i] = buildSkip3Salt(s, i+1)
		total += s
	}
	for d := 0; d < h; d++ {
		var next []*cell.Cell
		for i := 0; i < len(level); i += 4 {
			b := cell.BeginCell()
			end := i + 4
			if end > len(level) {
				end = len(level)
			}
			for _, c := range level[i:end] {
				b.MustStoreRef(c)
			}
			next = append(next, b.EndCell())
			total++
		}
		level = next
	}
	if len(level) == 1 {
		return level[0], total
	}
	rb := cell.BeginCell()
	for _, c := range level {
		rb.MustStoreRef(c)
	}
	return rb.EndCell(), total + 1
}

func TestForestWorstCase(t *testing.T) {
	bind := trivialBinding()
	// depth = (s-1) + h <= 128; cells ~= count*s <= 4096.
	configs := []struct{ count, s, h int }{
		{31, 124, 3},
		{32, 126, 3},
		{32, 125, 3},
		{30, 126, 3},
		{33, 124, 3},
	}
	for _, cfg := range configs {
		root, total := buildForest(cfg.count, cfg.s, cfg.h)
		s := b64(root)
		if len(s) > 1<<20 {
			fmt.Printf("forest count=%d s=%d h=%d -> b64 %d EXCEEDS 1MiB, skip\n", cfg.count, cfg.s, cfg.h, len(s))
			continue
		}
		dt, msg := timeDecode(bind, s, 5)
		fmt.Printf("forest count=%-3d s=%-4d h=%d cells=%-5d boc=%-7d b64=%-7d decode=%-12s err=%q\n",
			cfg.count, cfg.s, cfg.h, total, len(root.ToBOC()), len(s), dt, msg)
	}
}

// ---- large dictionary -------------------------------------------------------
func buildMaxDict(t *testing.T, entries int) (*tolkabi.Binding, string, int) {
	key := tolkabi.IntegerCodec(256, false, false)
	val := tolkabi.IntegerCodec(64, false, false)
	m := tolkabi.MapCodec(&key, &val, 256)
	bind := tolkabi.NewBinding(tolkabi.TypeInfo{Name: "bigdict"}, &m, "")
	arr := make([]tolkabi.MapEntry, entries)
	for i := 0; i < entries; i++ {
		// high-entropy keys within 2^256 so the patricia tree forks near the top
		k := new(big.Int).SetInt64(int64(i))
		mult, _ := new(big.Int).SetString("9E3779B97F4A7C15", 16)
		k.Mul(k, mult)
		k.Abs(k)
		k.And(k, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)))
		arr[i] = tolkabi.MapEntry{Key: k.String(), Value: fmt.Sprintf("%d", i)}
	}
	root, err := m.Encode(arr)
	if err != nil {
		t.Fatalf("encode dict %d: %v", entries, err)
	}
	return bind, b64(root), len(root.ToBOC())
}

func TestMaxDict(t *testing.T) {
	for _, n := range []int{256, 1000, 1500, 1800, 2000} {
		bind, s, bocLen := buildMaxDict(t, n)
		root, err := tolkabi.DecodeBOC(s)
		if err != nil {
			fmt.Printf("dict n=%d: DecodeBOC err=%v bocLen=%d\n", n, err, bocLen)
			continue
		}
		// warm + best-of-5
		bind.Decode(root)
		best := time.Hour
		var v any
		for r := 0; r < 5; r++ {
			root, _ = tolkabi.DecodeBOC(s)
			t0 := time.Now()
			v, err = bind.Decode(root)
			if d := time.Since(t0); d < best {
				best = d
			}
		}
		jb := []byte("nil")
		if err == nil {
			jb, _ = json.Marshal(v)
		}
		fmt.Printf("dict n=%-5d bocLen=%-7d decode(best5)=%-12s jsonLen=%-7d err=%v\n", n, bocLen, best, len(jb), err)
	}
}
