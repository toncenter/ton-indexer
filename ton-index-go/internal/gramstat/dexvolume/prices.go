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
)

type quote struct {
	minute, newest int64
	raw            *big.Rat
}
type Book map[string][]quote

func readBook(r io.Reader) (Book, error) {
	sc := bufio.NewScanner(r)
	book := Book{}
	done := false
	for sc.Scan() {
		if done {
			return nil, fmt.Errorf("unexpected trailing price data")
		}
		line := sc.Text()
		if line == "done" {
			done = true
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
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
		book[fields[0]] = append(rows, quote{at, newest, raw})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if !done {
		return nil, fmt.Errorf("incomplete price batch")
	}
	return book, nil
}

func Prices(ctx context.Context, sshConfig string, d Day) (Book, error) {
	needed := map[string]bool{}
	for _, r := range d.Routes {
		if (r.In.Asset == Native && r.In.quantity() != nil) || (r.Out.Asset == Native && r.Out.quantity() != nil) {
			continue
		}
		for _, l := range []Leg{r.In, r.Out} {
			if l.quantity() != nil && l.Asset != "" && l.Asset != Native {
				needed[l.Asset] = true
			}
		}
	}
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
	body, err := json.Marshal(map[string]any{"day": d.Date.Format("2006-01-02"), "assets": keys})
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
	b, err := readBook(out)
	if err != nil {
		_ = cmd.Process.Kill()
	}
	exit := cmd.Wait()
	if exit != nil {
		return nil, fmt.Errorf("quote reader: %w (%s)", exit, strings.TrimSpace(diagnostic.String()))
	}
	return b, err
}

func (b Book) price(l Leg, ts int64) *big.Rat {
	n := l.quantity()
	if n == nil {
		return nil
	}
	v := new(big.Rat).SetInt(n)
	if l.Asset == Native {
		return v
	}
	k := l.Asset
	if k == USDT {
		k = "USDT"
	}
	rows := b[k]
	i := sort.Search(len(rows), func(i int) bool { return rows[i].minute >= ts-ts%60 }) - 1
	if i < 0 {
		return nil
	}
	q := rows[i]
	if q.raw == nil || ts-q.newest > 900 || q.newest >= ts || ts-q.minute > 960 {
		return nil
	}
	if k == "USDT" {
		if q.minute != ts-ts%60-60 {
			return nil
		}
		return v.Quo(v, q.raw) // raw USDT per nanogram
	}
	return v.Mul(v, q.raw) // nanograms per raw jetton unit
}

func (b Book) nanograms(r Route) *big.Int {
	// Prefer a directly exchanged GRAM amount, then canonical USDT, then
	// a jetton. For equal ranks use the input; never sum the two valuations.
	rank := func(l Leg) int {
		switch l.Asset {
		case Native:
			return 0
		case USDT:
			return 1
		}
		return 2
	}
	legs := [2]Leg{r.In, r.Out}
	if rank(legs[1]) < rank(legs[0]) {
		legs[0], legs[1] = legs[1], legs[0]
	}
	for _, l := range legs {
		if v := b.price(l, r.End); v != nil {
			// Round a valued route once to the nearest nanogram, ties upwards.
			n, rem := new(big.Int), new(big.Int)
			n.QuoRem(v.Num(), v.Denom(), rem)
			if rem.Lsh(rem, 1).Cmp(v.Denom()) >= 0 {
				n.Add(n, big.NewInt(1))
			}
			return n
		}
	}
	return nil
}
