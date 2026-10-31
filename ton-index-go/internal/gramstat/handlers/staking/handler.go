package staking

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

//go:embed schema.sql
var Schema string

type Source interface {
	Collect(context.Context, time.Time, time.Time) ([]Point, error)
}
type Infrastructure interface {
	Attribute(context.Context, []Point, []Row) error
}
type Sink interface {
	InsertJSON(context.Context, string, []byte) error
}
type Handler struct {
	Source Source
	SS     Infrastructure
	Sink   Sink
	Log    *slog.Logger
}

func (*Handler) Name() string { return "staking" }
func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	// Bound source and SS work independently of the requested backfill length.
	for start := from; start.Before(to); {
		end := start.AddDate(0, 0, 7)
		if end.After(to) {
			end = to
		}
		cutoff := end
		if req.AsOf.Before(cutoff) {
			cutoff = req.AsOf
		}
		points, err := h.Source.Collect(ctx, start, cutoff)
		if err != nil {
			return err
		}
		rows := make([]Row, 0, len(points))
		for _, p := range points {
			r, err := Calculate(p, time.Now())
			if err != nil {
				return err
			}
			rows = append(rows, r)
		}
		if h.SS != nil {
			if err = h.SS.Attribute(ctx, points, rows); err != nil {
				return err
			}
		}
		if len(rows) > 0 {
			var body bytes.Buffer
			enc := json.NewEncoder(&body)
			for _, r := range rows {
				if err = enc.Encode(r); err != nil {
					return err
				}
			}
			if err = h.Sink.InsertJSON(ctx, "gramstat.staking_daily", body.Bytes()); err != nil {
				return err
			}
		}
		h.Log.Info("staking days published", "days", len(rows))
		start = end
	}
	return nil
}
