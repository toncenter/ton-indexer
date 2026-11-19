package validators

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func ptr[T any](v T) *T { return &v }
func fixtureRound() Round {
	return Round{Since: 100, Until: 200, Through: 200, Total: 2, Main: 1, TotalWeight: 9007199254741000,
		AnchorMC: 10, AnchorTime: 100, AnchorRoot: strings.Repeat("a", 64), AnchorFile: strings.Repeat("b", 64),
		Members: []Member{{0, strings.Repeat("01", 32), strings.Repeat("11", 32), 9007199254740993}, {1, strings.Repeat("02", 32), strings.Repeat("22", 32), 7}}}
}

func configBOC(t *testing.T, r Round) []byte {
	t.Helper()
	d := cell.NewDict(16)
	for _, m := range r.Members {
		key, _ := hex.DecodeString(m.Pubkey)
		adnl, _ := hex.DecodeString(m.ADNL)
		v, err := tlb.ToCell(tlb.ValidatorAddr{PublicKey: tlb.SigPubKeyED25519{Key: key}, ADNLAddr: adnl, Weight: m.Weight})
		if err != nil {
			t.Fatal(err)
		}
		if err = d.SetIntKey(new(big.Int).SetUint64(uint64(m.Index)), v); err != nil {
			t.Fatal(err)
		}
	}
	c, err := tlb.ToCell(tlb.ValidatorSetExt{UTimeSince: r.Since, UTimeUntil: r.Until, Total: r.Total, Main: r.Main, TotalWeight: r.TotalWeight, List: d})
	if err != nil {
		t.Fatal(err)
	}
	return c.ToBOC()
}

func TestConfig34FullRosterAndExactWeights(t *testing.T) {
	r := fixtureRound()
	boc := configBOC(t, r)
	if hash, err := verifyConfig(boc, r); err != nil || len(hash) != 64 {
		t.Fatalf("verify: %s %v", hash, err)
	}
	r.Members[0].ADNL = strings.Repeat("33", 32)
	if _, err := verifyConfig(boc, r); err == nil {
		t.Fatal("same count, different ADNL accepted")
	}
	r = fixtureRound()
	r.Members[1].Pubkey = r.Members[0].Pubkey
	if err := r.validate(); err == nil {
		t.Fatal("duplicate pubkey accepted")
	}
	r = fixtureRound()
	r.Members[1].Index = 2
	if err := r.validate(); err == nil {
		t.Fatal("missing dictionary index accepted")
	}
	r = fixtureRound()
	r.TotalWeight++
	if err := r.validate(); err == nil {
		t.Fatal("inconsistent weight accepted")
	}
}

func TestGeoUnknownCorrectionAndBoundaries(t *testing.T) {
	r := fixtureRound()
	if err := r.validate(); err != nil {
		t.Fatal(err)
	}
	observations := []observation{
		{Since: 100, Index: 0, GeoMember: GeoMember{Pubkey: r.Members[0].Pubkey, Samples: 2, ObservedAt: ptr(uint32(100)), RecordID: ptr("a"), Country: ptr("nl"), Latitude: ptr(52.0), Longitude: ptr(4.0)}},
		{Since: 100, Index: 1, GeoMember: GeoMember{Pubkey: r.Members[1].Pubkey}},
	}
	calc := func() ([]GeoRound, error) {
		return geoRows([]Round{r}, observations, 210, time.Unix(220, 0), time.Unix(230, 0))
	}
	rows, err := calc()
	if err != nil {
		t.Fatal(err)
	}
	g := rows[0]
	if g.Total != 2 || g.Observed != 1 || g.Located != 1 || g.Unknown != 1 || g.Countries == nil || *g.Countries != 1 || g.Breakdown["NL"] != 1 || g.Members[1].Latitude != nil || g.Status != "partial" {
		t.Fatalf("bad partial snapshot: %+v", g)
	}
	// A later source revision may remove all observations: no stale map entries or zero countries.
	observations[0].GeoMember = GeoMember{Pubkey: r.Members[0].Pubkey}
	rows, err = calc()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Countries != nil || len(rows[0].Breakdown) != 0 || rows[0].Unknown != 2 || rows[0].Status != "missing_observations" {
		t.Fatalf("unknown became zero/stale: %+v", rows[0])
	}
	observations[0].Samples = 1
	observations[0].RecordID = ptr("b")
	observations[0].ObservedAt = ptr(uint32(200))
	if _, err = calc(); err == nil {
		t.Fatal("next-round timestamp accepted")
	}
	observations[0].ObservedAt = ptr(uint32(199))
	observations[0].Latitude = ptr(91.0)
	observations[0].Longitude = ptr(5.0)
	rows, err = calc()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Located != 0 || rows[0].Members[0].Latitude != nil {
		t.Fatal("invalid coordinates published")
	}
	observations[0].Latitude = ptr(52.0)
	observations[0].Country = ptr("NL")
	observations[0].OrderingConflicts = 1
	rows, err = calc()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Members[0].Status != "conflicting_observation" || rows[0].Countries != nil {
		t.Fatal("ambiguous ordering key published")
	}
	observations = append(observations, observations[0])
	if _, err = calc(); err == nil {
		t.Fatal("duplicate response accepted")
	}
}

type fakeStore struct {
	count, geo map[uint32]Coverage
	written    []Round
	geoWritten []GeoRound
	loaded     []uint32
	rounds     []Round
}

func (s *fakeStore) Coverage(_ context.Context, geo bool) (map[uint32]Coverage, error) {
	if geo {
		return s.geo, nil
	}
	return s.count, nil
}
func (s *fakeStore) Rounds(_ context.Context, ids []uint32) ([]Round, error) {
	s.loaded = append(s.loaded, ids...)
	var out []Round
	for _, id := range ids {
		for _, r := range s.rounds {
			if r.Since == id {
				out = append(out, r)
			}
		}
	}
	return out, nil
}
func (s *fakeStore) WriteRounds(_ context.Context, rows []Round) error {
	s.written = append(s.written, rows...)
	return nil
}
func (s *fakeStore) WriteGeo(_ context.Context, rows []GeoRound) error {
	s.geoWritten = append(s.geoWritten, rows...)
	return nil
}

type fakeSource struct {
	rounds []Round
	from   time.Time
}

func (s *fakeSource) Collect(_ context.Context, from, to, refresh time.Time, stored map[uint32]Coverage) ([]Round, error) {
	s.from = from
	return s.rounds, nil
}

type fakeVerify struct{ calls int }

func (v *fakeVerify) Verify(_ context.Context, r Round) (string, error) {
	v.calls++
	return strings.Repeat("c", 64), nil
}

type fakeLocations struct{}

func (fakeLocations) Collect(_ context.Context, r []Round, _ time.Time) ([]GeoRound, error) {
	out := make([]GeoRound, len(r))
	for i, x := range r {
		out[i].Since = x.Since
	}
	return out, nil
}

func TestIndependentRoundsAndVerificationCache(t *testing.T) {
	r := fixtureRound()
	r.validate()
	bad := fixtureRound()
	bad.Since = 201
	bad.Until = 300
	bad.AnchorMC = 0
	s := &fakeStore{count: map[uint32]Coverage{100: {Since: 100, Until: 200, Through: 200, Version: Version, Hash: "verified", Digest: r.RosterDigest, AnchorMC: r.AnchorMC, AnchorRoot: r.AnchorRoot, AnchorFile: r.AnchorFile}}}
	source := &fakeSource{rounds: []Round{r, bad}}
	v := &fakeVerify{}
	h := Handler{Source: source, Verifier: v, Sink: s, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	req := gramstat.Request{AsOf: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), LookbackDays: 3}
	if err := h.Run(context.Background(), req); err == nil {
		t.Fatal("bad anchor did not fail")
	}
	if len(s.written) != 1 || v.calls != 0 || s.written[0].ConfigHash != "verified" || source.from.Unix() != 100 {
		t.Fatal("cache/catch-up/independent valid round failed")
	}
	r.Members[1].ADNL = strings.Repeat("44", 32)
	source.rounds = []Round{r}
	if err := h.Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if v.calls != 1 {
		t.Fatal("changed roster reused old proof")
	}
}

func TestGeoCatchUpRepairsInteriorAndChangedRoster(t *testing.T) {
	s := &fakeStore{count: map[uint32]Coverage{}, geo: map[uint32]Coverage{}}
	for i := uint32(1); i <= 3; i++ {
		r := fixtureRound()
		r.Since = i * 100
		r.Until = (i + 1) * 100
		s.rounds = append(s.rounds, r)
		c := Coverage{Since: r.Since, Until: r.Until, Through: r.Until, Digest: fmt.Sprint(i), Hash: "hash", Version: Version}
		s.count[r.Since] = c
		if i != 2 {
			s.geo[r.Since] = c
		}
	}
	x := s.geo[300]
	x.Digest = "old"
	s.geo[300] = x
	h := GeoHandler{Sink: s, Source: fakeLocations{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := h.Run(context.Background(), gramstat.Request{AsOf: time.Now(), LookbackDays: 3}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.loaded) != "[200 300]" {
		t.Fatalf("wrong catchup: %v", s.loaded)
	}
}
