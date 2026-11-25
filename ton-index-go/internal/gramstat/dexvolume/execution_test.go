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
	f, e = peerFlow([]byte(strings.Replace(raw, `"amount_in":81`, `"amount_in":80`, 1)), i, o)
	if e != nil || f.USDT != "80" {
		t.Fatal("use actual matched intermediate amount", f, e)
	}
	for _, bad := range []string{strings.Replace(raw, `"amount_in":81`, `"amount_in":64`, 1), strings.Replace(raw, `"amount_out":60`, `"amount_out":61`, 1)} {
		f, e := peerFlow([]byte(bad), i, o)
		if e != nil || f != (Flow{}) {
			t.Fatal("unconfirmed peer flow", f, e)
		}
	}
}

func TestDisjointNativeFundingAtRoot(t *testing.T) {
	g := Graph{Root: "w", Owner: "payer", External: true, Parent: map[string]string{"p": "w", "q": "w"}, Start: map[int64][]string{1: {"p"}, 2: {"q"}}, RootFunds: map[string]string{"p": "562600000000", "q": "437600000000"}}
	a := sw("stonfi", 1, "p", leg(Native, "562500000000", "payer"), leg(USDT, "969874707", "payer"))
	b := sw("dedust", 2, "q", leg(Native, "437500000000", "payer"), leg(USDT, "753244603", "payer"))
	a.Hashes = append(a.Hashes, "w")
	b.Hashes = append(b.Hashes, "w")
	rs, m := Resolve([]Swap{b, a}, g)
	if m != 0 || len(rs) != 1 || rs[0].In.Amount != "1000000000000" || rs[0].Out.Amount != "1723119310" || rs[0].Dex != "multiple" {
		t.Fatal(rs, m)
	}
	reject := func() {
		t.Helper()
		rs, m := Resolve([]Swap{a, b}, g)
		if len(rs) != 0 || m != 1 {
			t.Fatal("unproven native split", rs, m)
		}
	}
	g.RootFunds["p"] = "562499999999"
	reject()
	g.RootFunds["p"] = "562600000000"
	b.Out.Holder = "other"
	reject()
	b.Out.Holder = "payer"
	b.Out.Asset = "OTHER"
	reject()
	b.Out.Asset = USDT
	g.External = false
	reject()
}

func TestDirectPtonMessageLinksOppositeEndpoints(t *testing.T) {
	g := Graph{Root: "root", Parent: map[string]string{"a": "root", "send": "a", "recv": "send"}, Start: map[int64][]string{1: {"a"}, 2: {"recv"}}, Receiver: map[string]string{"send": "proxyA", "recv": "proxyB"}, PtonIn: map[string]bool{"recv": true}}
	a := sw("tonco", 1, "a", leg("UTYA", "111000000000", "user"), leg(Native, "2694750339", "proxyB"))
	a.Hashes = append(a.Hashes, "send")
	b := sw("stonfi_v2", 2, "recv", leg(Native, "2694750339", "proxyA"), leg(USDT, "4746351", "user"))
	b.InDexWallet = "proxyB"
	rs, m := Resolve([]Swap{a, b}, g)
	if len(rs) != 1 || m != 0 || rs[0].Via.Gram != "2694750339" || rs[0].Dex != "multiple" {
		t.Fatal(rs, m)
	}
	reject := func() {
		t.Helper()
		rs, m := Resolve([]Swap{a, b}, g)
		if len(rs) != 0 || m != 1 {
			t.Fatal("unproven cross-holder edge", rs, m)
		}
	}
	delete(g.PtonIn, "recv")
	reject()
	g.PtonIn["recv"] = true
	b.InDexWallet = "other"
	reject()
	b.InDexWallet = "proxyB"
	a.Hashes = []string{"a"}
	reject()
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
