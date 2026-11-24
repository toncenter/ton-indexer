package dexvolume

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Regression: a nearly empty pool on 2026-03-08 quoted a 30e12 raw
// jetton sale at 7.23M GRAM. Its actual 630.223953 USDT funded the second DEX.
func TestMarch8ExecutionNotMarginalPrice(t *testing.T) {
	g := Graph{Root: "w", Parent: map[string]string{"cmd": "w", "pool": "cmd", "delivery": "pool", "next": "delivery"},
		Start: map[int64][]string{1: {"pool"}, 2: {"next"}}, Receiver: map[string]string{"delivery": "router"}}
	a := sw("stonfi_v2", 1, "pool", leg("A", "30000000000000", "user"), leg(USDT, "630223953", "router"))
	a.Hashes = append(a.Hashes, "delivery")
	b := sw("dedust", 2, "next", a.Out, leg("B", "30014334038459", "user"))
	rs, miss := Resolve([]Swap{b, a}, g)
	if miss != 0 || len(rs) != 1 || rs[0].Dex != "multiple" || rs[0].Via.USDT != "630223953" {
		t.Fatal(rs, miss)
	}
	book, e := readBook(strings.NewReader("USDT\t600\t0.0013373642036286892\t600\t-\nA\t600\t240.97260801768934\t645\t943491767295.9705\ndone-depth\n"), "done-depth")
	if e != nil {
		t.Fatal(e)
	}
	out, e := Summarize(Day{Routes: rs}, Books{0: book}, time.Now())
	if e != nil || out.Value != 1 || out.Breakdown["intermediate"] != 1 || *out.AmountNanoton != "471243324212" {
		t.Fatal(out, e)
	}
	// Without the independently confirmed transfer, the marginal price is not usable.
	rs[0].Via = Flow{}
	if n, _, _ := book.nanograms(rs[0]); n != nil {
		t.Fatalf("unsupported notional %s", n)
	}
}

func TestExecutedCutRequiresAllFundedArms(t *testing.T) {
	g := Graph{Root: "w", Parent: map[string]string{"cmd": "w", "a": "cmd", "b": "cmd"}, Start: map[int64][]string{1: {"a"}, 2: {"b"}}}
	a := sw("stonfi", 1, "a", leg("X", "60", "router"), leg("Y", "45", "user"))
	b := sw("dedust", 2, "b", leg("X", "40", "router"), leg("Y", "35", "user"))
	a.Via.USDT = "60"
	r, m := Resolve([]Swap{a, b}, g)
	if m != 0 || len(r) != 1 || r[0].Via.USDT != "" {
		t.Fatal("partial cut accepted", r)
	}
	b.Via.USDT = "40"
	r, m = Resolve([]Swap{b, a}, g)
	if m != 0 || len(r) != 1 || r[0].Via.USDT != "100" {
		t.Fatal("disjoint arms must sum once", r)
	}
	b.OK = false
	r, m = Resolve([]Swap{a, b}, g)
	if m != 0 || len(r) != 1 || r[0].Via.USDT != "60" {
		t.Fatal("failed arm counted", r)
	}
}

func TestClassifierPeerAmountsMustReconcile(t *testing.T) {
	raw := fmt.Sprintf(`[{"asset_in":"A","amount_in":100,"asset_out":"%s","amount_out":81},{"asset_in":"%s","amount_in":81,"asset_out":null,"amount_out":70},{"asset_in":null,"amount_in":70,"asset_out":"B","amount_out":60}]`, USDT, USDT)
	i, o := leg("A", "100", "w"), leg("B", "60", "w")
	f, e := peerFlow([]byte(raw), i, o)
	if e != nil || f.USDT != "81" || f.Gram != "70" {
		t.Fatal(f, e)
	}
	for _, bad := range []string{strings.Replace(raw, `"amount_in":81`, `"amount_in":80`, 1), strings.Replace(raw, `"amount_out":60`, `"amount_out":61`, 1)} {
		f, e := peerFlow([]byte(bad), i, o)
		if e != nil || f != (Flow{}) {
			t.Fatal("unconfirmed peer flow", f, e)
		}
	}
}

func TestTokenSpotOnlyForSmallValuations(t *testing.T) {
	b, e := readBook(strings.NewReader("X\t600\t1\t605\t500\nY\t600\t1\t606\t1000\ndone-depth\n"), "done-depth")
	if e != nil {
		t.Fatal(e)
	}
	r := Route{In: leg("X", "100", "w"), Out: leg("Y", "99", "w"), End: 670}
	if n, _, _ := b.nanograms(r); n.String() != "99" {
		t.Fatal("prefer received amount if estimating", n)
	}
	r.Out.Amount = "201"
	if n, _, _ := b.nanograms(r); n.String() != "100" {
		t.Fatal("liquid input should still qualify", n)
	}
	r.In.Amount = "101"
	if n, _, _ := b.nanograms(r); n != nil {
		t.Fatal("did not reject oversize spot estimates", n)
	}
}
