// Package chain prepares daily charts from the ongoing mainnet full-history run.
package chain

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed daily.sql
var dailySQL string

//go:embed hourly.sql
var hourlySQL string

//go:embed inflation.sql
var inflationSQL string

//go:embed schema.sql
var Schema string

// The minimum time of every shard tip referenced by the settled MC is an
// exclusive completeness horizon, including shards lagging behind the MC.
const horizonSQL = `SELECT
 (SELECT toUInt32(count()) FROM {raw:Identifier}.shard_state FINAL WHERE mc_seqno={end:UInt32}) AS expected,
 toUInt32(count()) AS found, toUInt32(min(gen_utime)) AS before,
 toUInt32(countIf(workchain=-1 AND shard=-9223372036854775808 AND seqno={end:UInt32})) AS masterchain
 FROM {raw:Identifier}.blocks FINAL
 WHERE (workchain,shard,seqno) IN (
 SELECT workchain,shard,seqno FROM {raw:Identifier}.shard_state FINAL WHERE mc_seqno={end:UInt32}) FORMAT JSONEachRow`

func completeBefore(data []byte) (uint32, error) {
	var tips struct {
		Expected    uint32 `json:"expected"`
		Found       uint32 `json:"found"`
		Before      uint32 `json:"before"`
		Masterchain uint32 `json:"masterchain"`
	}
	if err := json.Unmarshal(data, &tips); err != nil {
		return 0, err
	}
	if tips.Expected < 2 || tips.Found != tips.Expected || tips.Masterchain != 1 || tips.Before == 0 {
		return 0, fmt.Errorf("incomplete chain shard-tip horizon")
	}
	return tips.Before, nil
}

type Store interface {
	Query(context.Context, string, url.Values) ([]byte, error)
	InsertJSON(context.Context, string, []byte) error
}

type Handler struct {
	CH               Store
	RawDB, MetricsDB string
	Log              *slog.Logger
}

func (*Handler) Name() string { return "chain" }

func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	if h.RawDB == "" || h.MetricsDB == "" {
		return fmt.Errorf("GRAMSTAT_CHAIN_RAW_DB and GRAMSTAT_CHAIN_METRICS_DB are required")
	}
	p := url.Values{"param_raw": {h.RawDB}, "param_metrics": {h.MetricsDB},
		"max_threads": {"2"}, "max_execution_time": {"90"}, "max_memory_usage": {"2147483648"}}
	data, err := h.CH.Query(ctx, `SELECT max(mc_seqno) AS head FROM {metrics:Identifier}.account_deltas FORMAT JSONEachRow`, p)
	if err != nil {
		return err
	}
	var head struct {
		Head uint32 `json:"head"`
	}
	if err = json.Unmarshal(data, &head); err != nil {
		return err
	}
	// Concurrent insert batches can finish out of order. Keep a settling margin,
	// then prove every MC/delta through the chosen endpoint in the aggregate.
	if head.Head < 1003 {
		return fmt.Errorf("full-history source has not reached a settled MC prefix")
	}
	end := head.Head - 1000
	p.Set("param_end", strconv.FormatUint(uint64(end), 10))
	data, err = h.CH.Query(ctx, horizonSQL, p)
	if err != nil {
		return err
	}
	before, err := completeBefore(data)
	if err != nil {
		return err
	}
	p.Set("param_complete_before", strconv.FormatUint(uint64(before), 10))
	data, err = h.CH.Query(ctx, dailySQL, p)
	if err != nil {
		return err
	}
	rows, count, err := publishRows(data, end, req)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	if err = h.CH.InsertJSON(ctx, "gramstat.chain_daily_full", rows); err != nil {
		return err
	}
	// Checkpoints cover the full verified prefix, even for a date-limited daily refresh.
	if _, err = h.CH.Query(ctx, "INSERT INTO gramstat.supply_hourly "+hourlySQL, p); err != nil {
		return err
	}
	if _, err = h.CH.Query(ctx, "INSERT INTO gramstat.inflation_hourly "+inflationSQL, p); err != nil {
		return err
	}
	if h.Log != nil {
		h.Log.Info("chain published", "days", count, "source_mc_to", end)
	}
	return nil
}

// Verify the entire source before narrowing the published range. In particular,
// a requested viewport must never reset the cumulative burn prefix.
func publishRows(data []byte, end uint32, req gramstat.Request) ([]byte, int, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	count := 0
	var last uint32
	for {
		var row map[string]json.RawMessage
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, err
		}
		var proof uint8
		var mc uint32
		var day string
		if err := json.Unmarshal(row["coverage_ok"], &proof); err != nil {
			return nil, 0, err
		}
		if err := json.Unmarshal(row["last_mc"], &mc); err != nil {
			return nil, 0, err
		}
		if err := json.Unmarshal(row["day"], &day); err != nil {
			return nil, 0, err
		}
		if proof != 1 || mc <= last {
			return nil, 0, fmt.Errorf("incomplete or unordered MC/delta prefix on %s", day)
		}
		last = mc
		d, err := time.Parse(time.DateOnly, day)
		if err != nil {
			return nil, 0, err
		}
		if !req.From.IsZero() && (d.Before(req.From) || !d.Before(req.To)) {
			continue
		}
		delete(row, "coverage_ok")
		delete(row, "last_mc")
		if err = enc.Encode(row); err != nil {
			return nil, 0, err
		}
		count++
	}
	if last != end {
		return nil, 0, fmt.Errorf("MC prefix ends at %d, expected %d", last, end)
	}
	return body.Bytes(), count, nil
}
