package dexvolume

import (
	"strings"
	"testing"
	"time"
)

func leg(asset, amount, owner string) Leg { return Leg{asset, amount, owner} }
func sw(dex string, at int64, h string, i, o Leg) Swap {
	return Swap{Dex: dex, OK: true, StartLT: at, End: 660, In: i, Out: o, Hashes: []string{h}}
}
func TestRoutes(t *testing.T) {
	g := Graph{Root: "wallet", Owner: "user", Parent: map[string]string{"router": "wallet", "a": "router", "b": "a", "c": "a", "d": "router", "x": "wallet"}, Start: map[int64][]string{1: {"a"}, 2: {"b"}, 3: {"c"}, 4: {"d"}, 5: {"x"}}}
	first := sw("stonfi", 1, "a", leg(Native, "100", "user"), leg("X", "300", "router"))
	g.Wallet = true
	g.Parent["delivery"] = "a"
	g.Parent["b"] = "delivery"
	g.Parent["c"] = "delivery"
	g.Receiver = map[string]string{"delivery": "router"}
	first.Hashes = append(first.Hashes, "delivery")
	second := sw("dedust", 2, "b", leg("X", "300", "router"), leg("Y", "90", "user"))
	assert := func(ss []Swap, n, u int, dex, amount string) {
		t.Helper()
		r, m := Resolve(ss, g)
		if len(r) != n || m != u || n == 1 && (r[0].Dex != dex || r[0].In.Amount != amount) {
			t.Fatalf("routes %+v ambiguous %d", r, m)
		}
	}
	assert([]Swap{second, first}, 1, 0, "multiple", "100")
	delete(g.Receiver, "delivery")
	assert([]Swap{second, first}, 0, 1, "", "")
	g.Receiver["delivery"] = "router"
	second.In.Amount = "299"
	assert([]Swap{first, second}, 0, 1, "", "") // ancestry alone is not funding
	second.In.Amount = "100"
	third := sw("dedust_v2", 3, "c", leg("X", "200", "router"), leg("Y", "130", "user"))
	assert([]Swap{third, first, second}, 1, 0, "multiple", "100")
	left := sw("stonfi_v2", 1, "a", leg(Native, "60", "router"), leg("X", "60", "user"))
	right := sw("dedust", 4, "d", leg(Native, "40", "router"), leg("X", "40", "user"))
	assert([]Swap{left, right}, 1, 0, "multiple", "100")
	right.OK = false
	assert([]Swap{left, right}, 1, 0, "stonfi", "60")
	independent := sw("dedust", 5, "x", leg(Native, "70", "user"), leg("Z", "5", "user"))
	assert([]Swap{first, independent}, 2, 0, "", "")
	g.Wallet = false
	assert([]Swap{first, independent}, 0, 1, "", "")
}
func TestHistoricalValuation(t *testing.T) {
	b, e := readBook(strings.NewReader("USDT\t600\t0.002\t600\nTOKEN\t540\t1.5\t555\nTOKEN\t600\t-\t620\nTOKEN\t660\t99\t675\ndone\n"), "done")
	if e != nil {
		t.Fatal(e)
	}
	r := Route{End: 659, In: leg("TOKEN", "3", "a")}
	if n, old := b.nanograms(r); n == nil || n.String() != "5" || old {
		t.Fatalf("prior minute: %v", n)
	}
	r.End = 670
	if n, _ := b.nanograms(r); n != nil {
		t.Fatal("lookahead or ignored tombstone", n)
	}
	r.In = leg(USDT, "1000000", "a")
	if n, _ := b.nanograms(r); n.String() != "500000000" {
		t.Fatal(n)
	}
	r.Out = leg(Native, "100000000000000000001", "b")
	if n, _ := b.nanograms(r); n.String() != r.Out.Amount {
		t.Fatal("must use exact native boundary")
	}
	d := Day{Routes: []Route{{Dex: "stonfi", Out: r.Out}, {Dex: "tonco"}}, Ambiguous: 1}
	m, e := Summarize(d, Books{0: b}, time.Now())
	if e != nil || *m.AmountNanoton != r.Out.Amount || m.Value != 1 || m.Breakdown["gram:stonfi"] != 100000000000 || m.Breakdown["nano:stonfi"] != 1 || m.Breakdown["unpriced:tonco"] != 1 || m.IsPartial != 1 {
		t.Fatal(m, e)
	}
}

func TestLastKnownOnlyWhenNeitherBoundaryIsFresh(t *testing.T) {
	b, err := readBook(strings.NewReader("A\t60\t1.5\t90\nB\t960\t2\t965\nUSDT\t60\t0.002\t60\nA\t1080\t-\t1085\ndone\n"), "done")
	if err != nil {
		t.Fatal(err)
	}
	r := Route{Dex: "stonfi", In: leg("A", "3", "a"), Out: leg("B", "6", "b"), End: 1030}
	if v, old := b.nanograms(r); old || v.String() != "12" {
		t.Fatal("older input displaced fresh output", v, old)
	}
	r.Out = leg(USDT, "120", "b")
	if v, old := b.nanograms(r); !old || v.String() != "5" {
		t.Fatal("did not carry exact jetton quote", v, old)
	}
	r.End = 1160
	if v, _ := b.nanograms(r); v != nil {
		t.Fatal("revived invalidated quote or carried USDT anchor", v)
	}
	routes := []Route{r, {End: 1160, In: r.In, Out: leg("C", "4", "c")}}
	keys := b.missingOpenings(routes)
	if len(keys) != 1 || !keys["C"] {
		t.Fatal("bad pre-window lookup", keys)
	}
	r.End = 1030
	m, e := Summarize(Day{Routes: []Route{r}}, Books{0: b}, time.Now())
	if e != nil || m.Breakdown["carried"] != 1 || m.Value != 1 || *m.AmountNanoton != "5" {
		t.Fatal(m, e)
	}
	if _, e := readBook(strings.NewReader("done\n"), "done-opening"); e == nil {
		t.Fatal("old reader cannot masquerade as opening history")
	}
}
