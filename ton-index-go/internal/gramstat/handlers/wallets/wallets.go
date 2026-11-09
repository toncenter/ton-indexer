// Package wallets prepares exact, versioned wallet series for interactive reads.
package wallets

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed schema.sql
var Schema string

//go:embed first.sql
var firstSQL string

//go:embed lookup.sql
var lookupSQL string

//go:embed lookup_mc.sql
var lookupMCSQL string

//go:embed lookup_daily.sql
var lookupDailySQL string

//go:embed minutes.sql
var minutesSQL string

//go:embed rollup.sql
var rollupSQL string

//go:embed counts.sql
var countsSQL string

//go:embed activations.sql
var activationsSQL string

//go:embed checkpoints.sql
var checkpointsSQL string

//go:embed activity_read.sql
var ActivitySQL string

//go:embed activated_read.sql
var ActivatedSQL string

//go:embed activations_read.sql
var ActivationsSQL string

type Store interface {
	Query(context.Context, string, url.Values) ([]byte, error)
}

type Handler struct {
	CH Store
	// SinkDB defaults to gramstat; isolated benchmark databases use identical SQL.
	SinkDB, SourceID string
	// Requires a source epoch certified for the native delta-last writer.
	FollowNative bool
	Log          *slog.Logger
}

func (*Handler) Name() string { return "wallets" }

type source struct {
	Raw      string `json:"raw_database"`
	Metrics  string `json:"metrics_database"`
	First    uint32 `json:"first_mc"`
	End      uint32 `json:"through_mc"`
	Before   uint32 `json:"complete_before"`
	From     uint32 `json:"complete_from"`
	Verified uint8  `json:"history_verified"`
	Proof    string `json:"proof_id"`
}

func (h *Handler) query(ctx context.Context, q string, p url.Values, out any) error {
	b, err := h.CH.Query(ctx, q, p)
	if err != nil {
		return err
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	if h.SourceID == "" {
		return fmt.Errorf("GRAMSTAT_WALLETS_SOURCE_ID is required")
	}
	sink := h.SinkDB
	if sink == "" {
		sink = "gramstat"
	}
	p := url.Values{"param_sink": {sink}, "param_source": {h.SourceID}, "param_registry": {Registry},
		"param_hashes": {registryParameter()}, "max_threads": {"2"}, "max_execution_time": {"90"},
		"max_memory_usage": {"2147483648"}, "max_bytes_before_external_group_by": {"300000000"}, "optimize_aggregation_in_order": {"1"}}
	var manifest struct {
		Data []source `json:"data"`
	}
	if err := h.query(ctx, `SELECT raw_database,metrics_database,first_mc,through_mc,complete_from,complete_before,history_verified,proof_id
 FROM {sink:Identifier}.wallet_sources WHERE source_id={source:String}
 ORDER BY through_mc DESC,complete_before DESC,history_verified DESC,proof_id DESC LIMIT 1 FORMAT JSON`, p, &manifest); err != nil {
		return err
	}
	if len(manifest.Data) != 1 {
		return fmt.Errorf("wallet source manifest is required")
	}
	s := manifest.Data[0]
	if s.First == 0 || s.End < s.First || s.From >= s.Before || s.Raw == "" || s.Metrics == "" || s.Proof == "" || s.Verified > 1 {
		return fmt.Errorf("invalid wallet source manifest")
	}
	p.Set("param_raw", s.Raw)
	p.Set("param_metrics", s.Metrics)
	set(p, "end", uint64(s.End))
	set(p, "first", uint64(s.First))
	if h.FollowNative {
		if err := h.advanceNative(ctx, p, &s); err != nil {
			return err
		}
	}
	// A manifest certifies deployment/message persistence and the transaction-time
	// horizon; MC/delta continuity is independently checked here. A settling margin
	// alone is not a proof that both sparse event tables have finished writing.
	var proof struct {
		N      uint32 `json:"n"`
		Blocks uint32 `json:"blocks"`
	}
	if err := h.query(ctx, `SELECT
 (SELECT toUInt32(count()) FROM {metrics:Identifier}.account_deltas FINAL WHERE mc_seqno BETWEEN {first:UInt32} AND {end:UInt32}) AS n,
 (SELECT toUInt32(count()) FROM {raw:Identifier}.blocks FINAL WHERE workchain=-1 AND shard=-9223372036854775808 AND seqno BETWEEN {first:UInt32} AND {end:UInt32}) AS blocks
 FORMAT JSONEachRow`, p, &proof); err != nil {
		return err
	}
	if proof.N != s.End-s.First+1 || proof.Blocks != proof.N {
		return fmt.Errorf("incomplete wallet MC/delta prefix")
	}

	var progress struct {
		MC uint32 `json:"mc"`
	}
	if err := h.query(ctx, `SELECT max(through_mc) AS mc FROM {sink:Identifier}.wallet_progress
 WHERE source_id={source:String} AND registry={registry:String} FORMAT JSONEachRow`, p, &progress); err != nil {
		return err
	}
	if progress.MC > s.End {
		return fmt.Errorf("source manifest regressed")
	}
	previous := progress.MC
	if previous == 0 {
		previous = s.First - 1
	}
	if previous < s.End {
		set(p, "previous", uint64(previous))
		if err := h.query(ctx, firstSQL, p, nil); err != nil {
			return fmt.Errorf("prepare first deployments: %w", err)
		}
		// minState is idempotent even when an uncertain insert is retried.
		if err := h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_progress SELECT {source:String},{registry:String},{end:UInt32}`, p, nil); err != nil {
			return err
		}
	}
	from, to, err := window(req, s.Before)
	if err != nil {
		return err
	}
	if from.Unix() < int64(s.From) && !(h.FollowNative && req.From.IsZero()) {
		return fmt.Errorf("requested days precede complete source coverage")
	}
	if h.FollowNative && req.From.IsZero() {
		// Enumerate all missing closed days, not only a moving lookback window.
		// This recovers outages and indexer jumps without silently leaving holes.
		var missing struct {
			Data []struct {
				Day string `json:"day"`
			} `json:"data"`
		}
		completeFrom := (uint64(s.From) + 86399) / 86400 * 86400
		if completeFrom >= uint64(to.Unix()) {
			return nil
		}
		p.Set("param_complete_from", strconv.FormatUint(completeFrom, 10))
		p.Set("param_closed_to", to.Format(time.DateOnly))
		if err := h.query(ctx, `SELECT toString(day) AS day FROM (
 SELECT toDate(toDateTime({complete_from:UInt32},'UTC'))+toUInt32(number) AS day
 FROM numbers(toUInt64(dateDiff('day',toDate(toDateTime({complete_from:UInt32},'UTC')),{closed_to:Date}))))
 WHERE day NOT IN (SELECT day FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1)
 ORDER BY day FORMAT JSON`, p, &missing); err != nil {
			return err
		}
		for _, row := range missing.Data {
			d, err := time.Parse(time.DateOnly, row.Day)
			if err != nil {
				return err
			}
			if err = h.day(ctx, p, s, d); err != nil {
				return fmt.Errorf("wallet day %s: %w", row.Day, err)
			}
		}
		return h.monthly(ctx, p, time.Unix(int64(completeFrom), 0).UTC(), to)
	}
	for d := from; d.Before(to); d = d.AddDate(0, 0, 1) {
		if err := h.day(ctx, p, s, d); err != nil {
			return fmt.Errorf("wallet day %s: %w", d.Format(time.DateOnly), err)
		}
	}
	return h.monthly(ctx, p, from, to)
}

func window(req gramstat.Request, before uint32) (time.Time, time.Time, error) {
	end := time.Unix(int64(before), 0).UTC().Truncate(24 * time.Hour)
	if req.From.IsZero() != req.To.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("both from and to are required")
	}
	if !req.From.IsZero() {
		if !req.From.Equal(req.From.UTC().Truncate(24*time.Hour)) || !req.To.Equal(req.To.UTC().Truncate(24*time.Hour)) || !req.From.Before(req.To) || req.To.After(end) {
			return time.Time{}, time.Time{}, fmt.Errorf("wallet range must contain closed UTC days within the source horizon")
		}
		return req.From, req.To, nil
	}
	days := req.LookbackDays
	if days < 1 {
		days = 1
	}
	return end.AddDate(0, 0, -days), end, nil
}

func set(p url.Values, k string, n uint64) { p.Set("param_"+k, strconv.FormatUint(n, 10)) }

func newID() (string, error) {
	id := [16]byte{}
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]), nil
}

func (h *Handler) ensureLookup(ctx context.Context, p url.Values) error {
	if p.Get("param_lookup") != "" {
		return nil
	}
	var ready struct {
		Data []struct {
			ID string `json:"lookup_id"`
		} `json:"data"`
	}
	if err := h.query(ctx, `SELECT toString(lookup_id) AS lookup_id FROM {sink:Identifier}.wallet_lookup_versions
 WHERE source_id={source:String} AND registry={registry:String} AND through_mc={end:UInt32}
 ORDER BY lookup_id LIMIT 1 FORMAT JSON`, p, &ready); err != nil {
		return err
	}
	if len(ready.Data) > 0 {
		p.Set("param_lookup", ready.Data[0].ID)
		return nil
	}
	id, err := newID()
	if err != nil {
		return err
	}
	p.Set("param_lookup", id)
	for _, q := range []string{lookupSQL, lookupMCSQL, lookupDailySQL,
		`INSERT INTO {sink:Identifier}.wallet_lookup_versions SELECT {source:String},{registry:String},{end:UInt32},{lookup:UUID}`} {
		if err := h.query(ctx, q, p, nil); err != nil {
			p.Del("param_lookup")
			return fmt.Errorf("prepare lookup snapshot: %w", err)
		}
	}
	if h.Log != nil {
		h.Log.Info("wallet lookup prepared", "through_mc", p.Get("param_end"))
	}
	return nil
}

func (h *Handler) activity(ctx context.Context, p url.Values, from, to uint64, lo, hi string) error {
	set(p, "from", from)
	set(p, "to", to)
	p.Set("param_lo", lo)
	p.Set("param_hi", hi)
	err := h.query(ctx, minutesSQL, p, nil)
	if err == nil {
		return nil
	}
	// Failed inserts may have emitted partial states. Retrying smaller aligned
	// windows in the same run remains exact: all downstream counts merge sets.
	if to-from <= 60 || !(strings.Contains(err.Error(), "Code: 241.") || strings.Contains(err.Error(), "Code: 159.")) {
		return err
	}
	middle := from + ((to-from)/120)*60
	if h.Log != nil {
		h.Log.Warn("splitting wallet activity window", "from", from, "to", to)
	}
	for _, span := range [][2]uint64{{from, middle}, {middle, to}} {
		set(p, "from", span[0])
		set(p, "to", span[1])
		var bounds struct {
			N  uint64 `json:"n,string"`
			Lo string `json:"lo"`
			Hi string `json:"hi"`
		}
		if e := h.query(ctx, `SELECT toString(count()) AS n,toString(min(start_lt)) AS lo,toString(max(end_lt)) AS hi
 FROM {raw:Identifier}.blocks WHERE gen_utime >= {from:UInt32} AND gen_utime < {to:UInt32}
 AND mc_block_seqno BETWEEN {first:UInt32} AND {end:UInt32} FORMAT JSONEachRow`, p, &bounds); e != nil {
			return e
		}
		if bounds.N > 0 {
			if e := h.activity(ctx, p, span[0], span[1], bounds.Lo, bounds.Hi); e != nil {
				return e
			}
		}
	}
	return nil
}

func (h *Handler) day(ctx context.Context, p url.Values, s source, d time.Time) error {
	p.Set("param_day", d.Format(time.DateOnly))
	set(p, "verified", uint64(s.Verified))
	var existing struct {
		N uint32 `json:"n"`
	}
	if err := h.query(ctx, `SELECT toUInt32(count()) AS n FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND day=toDate({day:String})
 AND history_verified={verified:UInt8}
 AND ({verified:UInt8}=1 OR through_mc={end:UInt32}) FORMAT JSONEachRow`, p, &existing); err != nil {
		return err
	}
	// A certified closed day cannot change within a source epoch. Initial history
	// repairs or a new registry use a different key. Do not append new copies hourly.
	if existing.N > 0 {
		return nil
	}
	if err := h.ensureLookup(ctx, p); err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	p.Set("param_run", id)
	p.Set("param_day", d.Format(time.DateOnly))
	set(p, "day_from", uint64(d.Unix()))
	set(p, "day_to", uint64(d.AddDate(0, 0, 1).Unix()))
	var chunks struct {
		Data []struct {
			Start uint32 `json:"chunk"`
			Lo    string `json:"lo"`
			Hi    string `json:"hi"`
		} `json:"data"`
	}
	// Blocks bound transaction LTs, across all shards, not just masterchain LTs.
	// The manifest horizon must ensure no future included block has tx.now < to.
	if err := h.query(ctx, `SELECT intDiv(toUInt32(gen_utime),86400)*86400 AS chunk,
 toString(min(start_lt)) AS lo,toString(max(end_lt)) AS hi
 FROM {raw:Identifier}.blocks
 WHERE gen_utime >= {day_from:UInt32} AND gen_utime < {day_to:UInt32}
 AND mc_block_seqno BETWEEN {first:UInt32} AND {end:UInt32}
 GROUP BY chunk ORDER BY chunk FORMAT JSON`, p, &chunks); err != nil {
		return err
	}
	for _, chunk := range chunks.Data {
		if err := h.activity(ctx, p, uint64(chunk.Start), uint64(chunk.Start)+86400, chunk.Lo, chunk.Hi); err != nil {
			return err
		}
	}
	for _, step := range []uint64{3600, 86400} {
		set(p, "step", step)
		if err := h.query(ctx, rollupSQL, p, nil); err != nil {
			return err
		}
	}
	if err := h.query(ctx, countsSQL, p, nil); err != nil {
		return err
	}
	var bounds struct {
		First uint32 `json:"first_mc"`
		Last  uint32 `json:"last_mc"`
	}
	if err := h.query(ctx, `SELECT toUInt32(min(seqno)) AS first_mc,toUInt32(max(seqno)) AS last_mc
 FROM {raw:Identifier}.blocks WHERE workchain=-1 AND shard=-9223372036854775808
 AND gen_utime >= {day_from:UInt32} AND gen_utime < {day_to:UInt32}
 AND seqno BETWEEN {first:UInt32} AND {end:UInt32} FORMAT JSONEachRow`, p, &bounds); err != nil {
		return err
	}
	// A day without MCs can still be a proven network pause, but has no MC points.
	set(p, "first_mc", uint64(bounds.First))
	set(p, "last_mc", uint64(bounds.Last))
	set(p, "opening_before", uint64(bounds.First))
	if bounds.First == 0 {
		var previous struct {
			MC uint32 `json:"mc"`
		}
		if err := h.query(ctx, `SELECT toUInt32(max(seqno)) AS mc FROM {raw:Identifier}.blocks
 WHERE workchain=-1 AND shard=-9223372036854775808 AND gen_utime < {day_from:UInt32}
 AND seqno BETWEEN {first:UInt32} AND {end:UInt32} FORMAT JSONEachRow`, p, &previous); err != nil {
			return err
		}
		set(p, "opening_before", uint64(previous.MC)+1)
	}
	var totals struct {
		Opening     string `json:"opening"`
		Activations string `json:"activations"`
	}
	if err := h.query(ctx, `SELECT
 toString(ifNull((SELECT activated FROM {sink:Identifier}.wallet_first_mc
 WHERE source_id={source:String} AND registry={registry:String} AND lookup_id={lookup:UUID}
 AND mc_seqno < {opening_before:UInt64} ORDER BY mc_seqno DESC LIMIT 1),toUInt64(0))) AS opening,
 (SELECT toString(sum(activations)) FROM {sink:Identifier}.wallet_first_daily
 WHERE source_id={source:String} AND registry={registry:String} AND lookup_id={lookup:UUID}
 AND day=toDate({day:String})) AS activations FORMAT JSONEachRow`, p, &totals); err != nil {
		return err
	}
	p.Set("param_opening", totals.Opening)
	p.Set("param_activations", totals.Activations)
	if bounds.First != 0 {
		if err := h.query(ctx, activationsSQL, p, nil); err != nil {
			return err
		}
		for _, step := range []uint64{3600, 86400} {
			set(p, "step", step)
			if err := h.query(ctx, checkpointsSQL, p, nil); err != nil {
				return err
			}
		}
	}
	set(p, "before", uint64(s.Before))
	set(p, "verified", uint64(s.Verified))
	p.Set("param_proof", s.Proof)
	// Commit last. Readers select one whole run for each day in a single query.
	if err := h.query(ctx, `INSERT INTO {sink:Identifier}.wallet_days
 SELECT {source:String},{registry:String},toDate({day:String}),{run:UUID},
 {end:UInt32},{before:UInt32},{verified:UInt8},{proof:String},{activations:UInt64},
 {opening:UInt64},{first_mc:UInt32},{last_mc:UInt32},now64(6)`, p, nil); err != nil {
		return err
	}
	if h.Log != nil {
		h.Log.Info("wallet day prepared", "day", d.Format(time.DateOnly), "through_mc", s.End, "history_verified", s.Verified)
	}
	return nil
}
