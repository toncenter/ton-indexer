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
