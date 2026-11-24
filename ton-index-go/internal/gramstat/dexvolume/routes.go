// Package dexvolume measures recognized user swaps rather than pool turnover.
package dexvolume

import (
	"math/big"
	"slices"
	"sort"
	"strings"
)

const Native = "GRAM"
const USDT = "0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE"

func asset(s string) string {
	switch strings.ToUpper(s) {
	case "", Native, // A present swap transfer with NULL master denotes native GRAM.
		"0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C",
		"0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3":
		return Native
	default:
		return strings.ToUpper(s)
	}
}

func venue(s string) string {
	switch s {
	case "stonfi", "stonfi_v2":
		return "stonfi"
	case "dedust", "dedust_v2":
		return "dedust"
	case "coffee", "tonco":
		return s
	default:
		return "other"
	}
}

type Leg struct {
	Asset, Amount, Holder string
}

func (l Leg) quantity() *big.Int {
	x, ok := new(big.Int).SetString(l.Amount, 10)
	if !ok || x.Sign() <= 0 {
		return nil
	}
	return x
}

type Swap struct {
	Dex       string
	OK        bool
	StartLT   int64
	End       int64
	In, Out   Leg
	Hashes    []string
	OutWallet string
	Via       Flow
}

// Flow contains alternative measurements of the SAME complete exchange.
// Neither field is added to turnover as a separate hop.
type Flow struct{ Gram, USDT string }

func (f Flow) amount(k string) string {
	if k == Native {
		return f.Gram
	}
	return f.USDT
}

type Route struct {
	Dex     string
	End     int64
	In, Out Leg
	Via     Flow
}

// Graph is scoped to one complete trace. Parent is derived from the two
// directions of the same message, never from consecutive transaction LTs.
type Graph struct {
	Root     string
	Owner    string
	Wallet   bool
	Parent   map[string]string
	Receiver map[string]string
	// The incoming message creation LT (or external transaction LT).
	Start map[int64][]string
}

func (g Graph) path(a Swap) []string {
	members := make(map[string]bool, len(a.Hashes))
	for _, h := range a.Hashes {
		members[h] = true
	}
	var entry string
	for _, h := range g.Start[a.StartLT] {
		if members[h] {
			if entry != "" {
				return nil
			}
			entry = h
		}
	}
	var reverse []string
	for h := entry; h != ""; h = g.Parent[h] {
		if len(reverse) > len(g.Parent)+1 {
			return nil
		}
		reverse = append(reverse, h)
		if h == g.Root {
			for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
				reverse[i], reverse[j] = reverse[j], reverse[i]
			}
			return reverse
		}
	}
	return nil
}

func single(s Swap) Route {
	return Route{Dex: venue(s.Dex), End: s.End, In: s.In, Out: s.Out, Via: s.Via}
}

// Add only interchangeable boundaries. Unknown amounts and different holders
// cannot be made interchangeable by assigning the same display symbol.
func combine(ls []Leg) (Leg, bool) {
	if len(ls) == 0 {
		return Leg{}, false
	}
	r := ls[0]
	sum := new(big.Int)
	for _, l := range ls {
		n := l.quantity()
		if l.Asset != r.Asset || l.Holder == "" || l.Holder != r.Holder || n == nil {
			return Leg{}, false
		}
		sum.Add(sum, n)
	}
	r.Amount = sum.String()
	return r, true
}

type positioned struct {
	Swap
	path []string
}

func follows(a, b positioned) bool {
	return len(a.path) < len(b.path) && b.path[len(a.path)-1] == a.path[len(a.path)-1]
}

func branch(v []positioned, g Graph) (Route, bool) {
	if len(v) == 1 {
		return single(v[0].Swap), v[0].OK
	}
	children := make([][]int, len(v))
	var roots []int
	for i, b := range v {
		parent := -1
		for j, a := range v {
			if j == i {
				continue
			}
			if a.path[len(a.path)-1] == b.path[len(b.path)-1] {
				return Route{}, false
			}
			if follows(a, b) && (parent < 0 || len(a.path) > len(v[parent].path)) {
				parent = j
			}
		}
		if parent < 0 {
			roots = append(roots, i)
		} else {
			children[parent] = append(children[parent], i)
		}
	}
	var ins, outs []Leg
	r := Route{}
	for _, i := range roots {
		if v[i].OK {
			ins = append(ins, v[i].In)
		}
	}
	// For parallel arms of one instruction require the same funding holder
	// and asset. A failed independent arm contributes neither input nor output.
	for i, s := range v {
		if !s.OK {
			if len(children[i]) != 0 {
				return Route{}, false
			}
			continue
		}
		d := venue(s.Dex)
		if r.Dex == "" {
			r.Dex = d
		} else if r.Dex != d {
			r.Dex = "multiple"
		}
		if s.End > r.End {
			r.End = s.End
		}
		if len(children[i]) == 0 {
			outs = append(outs, s.Out)
			continue
		}
		ls := make([]Leg, 0, len(children[i]))
		for _, j := range children[i] {
			if !v[j].OK {
				return Route{}, false
			} // returned/retained balance not established
			// An equal amount somewhere below the swap is insufficient:
			// the actual recipient (owner or jetton wallet) must receive a
			// transaction of the first swap on the path funding the next.
			delivered := false
			for _, h := range v[j].path[len(s.path):] {
				to := g.Receiver[h]
				if to != "" && (to == s.Out.Holder || to == s.OutWallet) && slices.Contains(s.Hashes, h) {
					delivered = true
					break
				}
			}
			if !delivered {
				return Route{}, false
			}
			ls = append(ls, v[j].In)
		}
		funded, ok := combine(ls)
		if !ok || funded.Asset != s.Out.Asset || funded.Holder != s.Out.Holder ||
			s.Out.quantity() == nil || s.Out.quantity().Cmp(funded.quantity()) != 0 {
			return Route{}, false
		}
	}
	var ok bool
	r.In, ok = combine(ins)
	if !ok {
		return Route{}, false
	}
	r.Out, _ = combine(outs) // Unlike outputs may still have a single exact input.
	// Funding and conservation of every inter-action edge were checked above.
	// Pick one measurement per successful path. A measurement before a split
	// covers all descendants; below a split ALL successful arms must qualify.
	cut := func(k string) string {
		var visit func(int) *big.Int
		sum := func(indices []int) *big.Int {
			total := new(big.Int)
			for _, i := range indices {
				if !v[i].OK {
					continue
				}
				n := visit(i)
				if n == nil {
					return nil
				}
				total.Add(total, n)
			}
			if total.Sign() == 0 {
				return nil
			}
			return total
		}
		visit = func(i int) *big.Int {
			s := v[i]
			for _, l := range []Leg{s.In, s.Out, {Asset: k, Amount: s.Via.amount(k)}} {
				if l.Asset == k {
					if n := l.quantity(); n != nil {
						return n
					}
				}
			}
			return sum(children[i])
		}
		if n := sum(roots); n != nil {
			return n.String()
		}
		return ""
	}
	r.Via = Flow{Gram: cut(Native), USDT: cut(USDT)}
	return r, true
}

// Resolve returns disjoint, independently priceable routes and the number
// of ambiguous instructions. Never attempt O(n²) work on an unbounded trace.
func Resolve(swaps []Swap, g Graph) ([]Route, int) {
	if len(swaps) == 1 {
		if swaps[0].OK {
			return []Route{single(swaps[0])}, 0
		}
		return nil, 0
	}
	if len(swaps) > 64 {
		return nil, 1
	}
	groups := map[string][]positioned{}
	for _, s := range swaps {
		path := g.path(s)
		if len(path) < 2 {
			return nil, 1
		}
		// Separate outbound messages of the trace initiator are separately
		// funded commands. Splits below one command remain in the same group.
		groups[path[1]] = append(groups[path[1]], positioned{s, path})
	}
	var routes []Route
	missing := 0
	for _, v := range groups {
		if !anySuccess(v) {
			continue
		}
		if r, ok := branch(v, g); ok {
			if len(groups) > 1 && (!g.Wallet || g.Owner == "" || r.In.Holder != g.Owner) {
				// Multiple commands from an unverified common funding account
				// might be arms of one split rather than separate user intents.
				return nil, 1
			}
			routes = append(routes, r)
		} else {
			missing++
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].End < routes[j].End })
	return routes, missing
}

func anySuccess(v []positioned) bool {
	for _, s := range v {
		if s.OK {
			return true
		}
	}
	return false
}
