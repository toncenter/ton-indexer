package dexvolume

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

type quote struct {
	minute, newest int64
	raw            *big.Rat
	liquidity      *big.Rat // contemporaneous quoted liquidity in nanograms
}
type Book map[string][]quote
type Books map[int64]Book // by UTC day of execution, not trace completion

func readBook(r io.Reader, terminator string) (Book, error) {
	sc := bufio.NewScanner(r)
	book := Book{}
	done := false
	for sc.Scan() {
		if done {
			return nil, fmt.Errorf("unexpected trailing price data")
		}
		line := sc.Text()
		if line == terminator {
			done = true
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			return nil, fmt.Errorf("invalid price row")
		}
		at, e := strconv.ParseInt(fields[1], 10, 64)
		newest, e2 := strconv.ParseInt(fields[3], 10, 64)
		rows := book[fields[0]]
		if e != nil || e2 != nil || at%60 != 0 || (len(rows) > 0 && at <= rows[len(rows)-1].minute) {
			return nil, fmt.Errorf("invalid price ordering")
		}
		var raw *big.Rat
		if fields[2] != "-" {
			var ok bool
			raw, ok = new(big.Rat).SetString(fields[2])
			if !ok || raw.Sign() <= 0 {
				return nil, fmt.Errorf("invalid decimal price")
			}
		}
		var liquidity *big.Rat
		if fields[4] != "-" {
			var ok bool
			liquidity, ok = new(big.Rat).SetString(fields[4])
			if !ok || liquidity.Sign() <= 0 {
				return nil, fmt.Errorf("invalid quote liquidity")
			}
		}
		book[fields[0]] = append(rows, quote{at, newest, raw, liquidity})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if !done {
		return nil, fmt.Errorf("incomplete price batch")
	}
	return book, nil
}

func Prices(ctx context.Context, sshConfig string, d Day) (Books, error) {
	days := make(map[int64][]Route)
	for _, r := range d.Routes {
		if (r.In.Asset == Native && r.In.quantity() != nil) || (r.Out.Asset == Native && r.Out.quantity() != nil) {
			continue
		}
		if r.Via.Gram != "" && r.In.Asset != USDT && r.Out.Asset != USDT {
			continue
		}
		days[r.End/86400] = append(days[r.End/86400], r)
	}
	books := Books{}
	for index, routes := range days {
		day := time.Unix(index*86400, 0).UTC()
		needed := map[string]bool{}
		for _, r := range routes {
			if r.Via.USDT != "" {
				needed[USDT] = true
			}
			for _, l := range []Leg{r.In, r.Out} {
				if l.Asset != "" && l.Asset != Native && l.quantity() != nil {
					needed[l.Asset] = true
				}
			}
		}
		book, err := requestBook(ctx, sshConfig, day, needed, false)
		if err != nil {
			return nil, err
		}
		keys := book.missingOpenings(routes)
		if len(keys) > 0 {
			openings, err := requestBook(ctx, sshConfig, day, keys, true)
			if err != nil {
				return nil, err
			}
			for k, row := range openings {
				if !keys[k] || len(row) != 1 || row[0].minute >= day.Unix()-900 {
					return nil, fmt.Errorf("invalid opening quote")
				}
				book[k] = append(row, book[k]...)
			}
		}
		books[index] = book
	}
	return books, nil
}

// Only look up pre-window history for unpriced routes and only when the
// window has no prior observation. An explicit invalid row is authoritative.
func (b Book) missingOpenings(routes []Route) map[string]bool {
	keys := map[string]bool{}
	for _, r := range routes {
		if v, old, _ := b.nanograms(r); v != nil && !old {
			continue
		}
		for _, l := range []Leg{r.In, r.Out} {
			if l.Asset == "" || l.Asset == Native || l.Asset == USDT || l.quantity() == nil {
				continue
			}
			if b.latest(l.Asset, r.End) == nil {
				keys[l.Asset] = true
			}
		}
	}
	return keys
}

func requestBook(ctx context.Context, sshConfig string, day time.Time, needed map[string]bool, opening bool) (Book, error) {
	if len(needed) == 0 {
		return Book{}, nil
	}
	keys := make([]string, 0, len(needed))
	for k := range needed {
		if k == USDT {
			k = "USDT"
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mode, terminator := "window", "done"
	if opening {
		mode, terminator = "opening", "done-opening"
	}
	terminator += "-depth"
	body, err := json.Marshal(map[string]any{"day": day.Format(time.DateOnly), "assets": keys, "mode": mode, "depth": true})
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "/usr/bin/ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-F", sshConfig, "gramstat-prices")
	cmd.Stdin = bytes.NewReader(body)
	var diagnostic bytes.Buffer
	cmd.Stderr = &diagnostic
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	b, err := readBook(out, terminator)
	if err != nil {
		_ = cmd.Process.Kill()
	}
	exit := cmd.Wait()
	if exit != nil {
		return nil, fmt.Errorf("quote reader: %w (%s)", exit, strings.TrimSpace(diagnostic.String()))
	}
	return b, err
}

func (b Book) latest(k string, ts int64) *quote {
	rows := b[k]
	i := sort.Search(len(rows), func(i int) bool { return rows[i].minute >= ts-ts%60 }) - 1
	if i < 0 {
		return nil
	}
	return &rows[i]
}

func (b Book) price(l Leg, ts int64) (*big.Rat, bool) {
	n := l.quantity()
	if n == nil {
		return nil, false
	}
	v := new(big.Rat).SetInt(n)
	if l.Asset == Native {
		return v, false
	}
	k := l.Asset
	if k == USDT {
		k = "USDT"
	}
	q := b.latest(k, ts)
	if q == nil || q.raw == nil || q.newest <= 0 || q.newest >= ts {
		return nil, false
	}
	old := ts-q.newest > 900 || ts-q.minute > 960
	if k == "USDT" {
		if old || q.minute != ts-ts%60-60 {
			return nil, false
		}
		return v.Quo(v, q.raw), false // raw USDT per nanogram
	}
	v.Mul(v, q.raw)
	// Spot is a marginal rate, not a firm quote for an arbitrary quantity.
	// This is a conservative admission test, NOT a simulated execution curve.
	// Accept v <= 20% of quoted liquidity, before rounding.
	if q.liquidity == nil || new(big.Rat).Mul(v, big.NewRat(5, 1)).Cmp(q.liquidity) > 0 {
		return nil, false
	}
	return v, old
}

func (b Book) nanograms(r Route) (*big.Int, bool, bool) {
	// Executed boundaries first; then a verified intermediate cut, once.
	// When neither is observable, prefer a usable estimate of what the user
	// actually received. A fresh input still wins over a carried output.
	for _, internal := range []bool{false, true} {
		for _, asset := range []string{Native, USDT} {
			legs := [2]Leg{r.In, r.Out}
			if internal {
				legs = [2]Leg{{Asset: asset, Amount: r.Via.amount(asset)}, {}}
			}
			for _, l := range legs {
				if l.Asset != asset {
					continue
				}
				if v, _ := b.price(l, r.End); v != nil {
					return roundNano(v), false, internal
				}
			}
		}
	}
	var selected *big.Rat
	var carried bool
	for _, l := range [2]Leg{r.Out, r.In} {
		if l.Asset == Native || l.Asset == USDT {
			continue
		}
		v, old := b.price(l, r.End)
		if v == nil {
			continue
		}
		if !old {
			selected, carried = v, false
			break
		}
		if selected == nil {
			selected, carried = v, true
		}
	}
	if selected == nil {
		return nil, false, false
	}
	return roundNano(selected), carried, false
}

func roundNano(selected *big.Rat) *big.Int {
	// Round only the selected boundary, once, to the nearest nanogram.
	n, rem := new(big.Int), new(big.Int)
	n.QuoRem(selected.Num(), selected.Denom(), rem)
	if rem.Lsh(rem, 1).Cmp(selected.Denom()) >= 0 {
		n.Add(n, big.NewInt(1))
	}
	return n
}
