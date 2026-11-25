package codehashes

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func testHash(prefix byte, tail byte) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	b := make([]byte, 32)
	b[0] = byte(strings.IndexByte(alphabet, prefix)) << 2
	b[31] = tail
	return base64.StdEncoding.EncodeToString(b)
}

type fakePage struct{ cursor, body string }
type fakeConn struct {
	pages     []fakePage
	calls     int
	watermark uint32
	closed    bool
	fail      bool
	regress   bool
}

func (f *fakeConn) page(_ context.Context, _, _ string) (string, string, error) {
	if f.fail {
		return "", "", errors.New("disconnected")
	}
	if f.calls >= len(f.pages) {
		return "", "", errors.New("unexpected page")
	}
	p := f.pages[f.calls]
	f.calls++
	return p.cursor, p.body, nil
}
func (f *fakeConn) progress(context.Context) (uint32, error) {
	if f.regress && f.calls > 0 {
		return f.watermark - 1, nil
	}
	return f.watermark, nil
}
func (f *fakeConn) close() { f.closed = true }

func TestCollectAllPrefixesAndExactCounts(t *testing.T) {
	var conns []*fakeConn
	source := RedisSource{open: func(context.Context) (pageConnection, error) {
		char := Alphabet[len(conns)]
		f := &fakeConn{watermark: 100 + uint32(len(conns)), pages: []fakePage{
			{"9", testHash(char, 0) + ":9007199254740991"},
			{"0", testHash(char, 1) + ":0\n" + testHash(char, 2) + ":2"},
		}}
		conns = append(conns, f)
		return f, nil
	}}
	s, err := source.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	totals, err := s.summary()
	if err != nil || totals.Hashes != 128 || totals.Accounts != 64*(9007199254740991+2) ||
		totals.MCMin != 100 || totals.MCMax != 163 || s.Pages != 128 || s.ZeroHashes != 64 {
		t.Fatal(s, totals, err)
	}
	for _, f := range conns {
		if !f.closed || f.calls != 2 {
			t.Fatal("prefix did not retain and close its own connection")
		}
	}
}

func TestRejectIncompleteOrRestartedScans(t *testing.T) {
	hash := testHash('A', 0)
	for name, f := range map[string]*fakeConn{
		"connection loss": {fail: true},
		"replayed page":   {pages: []fakePage{{"1", hash + ":1"}, {"2", hash + ":1"}}},
		"empty cursor":    {pages: []fakePage{{"", ""}}},
		"no progress":     {pages: []fakePage{{"1", ""}}},
		"wrong prefix":    {pages: []fakePage{{"0", testHash('B', 0) + ":1"}}},
		"malformed hash":  {pages: []fakePage{{"0", strings.Repeat("A", 43) + "?:1"}}},
		"negative count":  {pages: []fakePage{{"0", hash + ":-1"}}},
		"rounded integer": {pages: []fakePage{{"0", hash + ":9007199254740992"}}},
		"numeric junk":    {pages: []fakePage{{"0", hash + ":1e4"}}},
		"rollback":        {regress: true, pages: []fakePage{{"0", hash + ":1"}}},
		"cursor loop": {pages: []fakePage{
			{"1", hash + ":1"}, {"1", testHash('A', 1) + ":1"},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f.watermark = 100
			if _, _, _, err := collectPrefix(context.Background(), f, "A", maxHashes); err == nil {
				t.Fatal("accepted invalid prefix")
			}
		})
	}
	var opened int
	source := RedisSource{open: func(context.Context) (pageConnection, error) {
		i := opened
		opened++
		return &fakeConn{watermark: 100, fail: i == 3, pages: []fakePage{{"0", testHash(Alphabet[i], 0) + ":1"}}}, nil
	}}
	if s, err := source.Collect(context.Background()); err == nil || s != nil || opened != 4 {
		t.Fatal("failed source returned a publishable partial snapshot", s, err, opened)
	}
}

func TestSnapshotCoverageAndOverflow(t *testing.T) {
	s := testSnapshot()
	s.Prefixes[0].Counts[testHash(Alphabet[0], 0)] = ^uint64(0)
	if _, err := s.summary(); err == nil {
		t.Fatal("accepted overflowing total")
	}
	s = testSnapshot()
	s.Prefixes[1].Name = s.Prefixes[0].Name
	if _, err := s.summary(); err == nil {
		t.Fatal("accepted duplicate prefix instead of complete coverage")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (RedisSource{open: func(context.Context) (pageConnection, error) {
		return nil, fmt.Errorf("must not dial")
	}}).Collect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func testSnapshot() *Snapshot {
	s := &Snapshot{StartedAt: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC), Pages: 64}
	s.FinishedAt = s.StartedAt.Add(20 * time.Second)
	for _, c := range Alphabet {
		s.Prefixes = append(s.Prefixes, Prefix{
			Name: string(c), Counts: map[string]uint64{testHash(byte(c), 0): 10}, MCBefore: 100, MCAfter: 101,
		})
	}
	return s
}
