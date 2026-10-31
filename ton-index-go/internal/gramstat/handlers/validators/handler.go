package validators

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

type Source interface {
	Collect(context.Context, time.Time, time.Time, time.Time, map[uint32]Coverage) ([]Round, error)
}
type Verifier interface {
	Verify(context.Context, Round) (string, error)
}
type Sink interface {
	Coverage(context.Context, bool) (map[uint32]Coverage, error)
	Rounds(context.Context, []uint32) ([]Round, error)
	WriteRounds(context.Context, []Round) error
	WriteGeo(context.Context, []GeoRound) error
}
type Handler struct {
	Source   Source
	Verifier Verifier
	Sink     Sink
	Log      *slog.Logger
}

func (*Handler) Name() string { return "validators" }

func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	stored, err := h.Sink.Coverage(ctx, false)
	if err != nil {
		return err
	}
	refresh := from
	if req.From.IsZero() {
		for _, c := range stored {
			if t := time.Unix(int64(c.Since), 0).UTC(); t.Before(from) {
				from = t
			}
		}
	} else {
		refresh = time.Time{}
	}
	rounds, err := h.Source.Collect(ctx, from, to, refresh, stored)
	if err != nil {
		return err
	}
	var failures []error
	var ready []Round
	for _, r := range rounds {
		if err = r.validate(); err == nil && (r.AnchorMC == 0 || r.AnchorRoot == "" || r.AnchorFile == "") {
			err = fmt.Errorf("activation MC unavailable or not a key block")
		}
		if err == nil {
			old := stored[r.Since]
			if old.Version == Version && old.Hash != "" && old.Digest == r.RosterDigest && old.AnchorMC == r.AnchorMC && old.AnchorRoot == r.AnchorRoot && old.AnchorFile == r.AnchorFile {
				r.ConfigHash = old.Hash
			} else {
				r.ConfigHash, err = h.Verifier.Verify(ctx, r)
			}
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("round %d: %w", r.Since, err))
			continue
		}
		ready = append(ready, r)
	}
	if err = h.Sink.WriteRounds(ctx, ready); err != nil {
		failures = append(failures, err)
	}
	h.Log.Info("validator rounds published", "rounds", len(ready), "failed", len(failures))
	return errors.Join(failures...)
}

type Locations interface {
	Collect(context.Context, []Round, time.Time) ([]GeoRound, error)
}
type GeoHandler struct {
	Source Locations
	Sink   Sink
	Log    *slog.Logger
}

func (*GeoHandler) Name() string { return "validator_geo" }

func (h *GeoHandler) Run(ctx context.Context, req gramstat.Request) error {
	from, to, err := req.DailyWindow()
	if err != nil {
		return err
	}
	rounds, err := h.Sink.Coverage(ctx, false)
	if err != nil {
		return err
	}
	if len(rounds) == 0 {
		return fmt.Errorf("no verified rounds; run validators first")
	}
	stored, err := h.Sink.Coverage(ctx, true)
	if err != nil {
		return err
	}
	var ids []uint32
	for id, r := range rounds {
		if r.Version != Version || int64(r.Since) >= to.Unix() {
			continue
		}
		old, exists := stored[id]
		if req.From.IsZero() {
			if exists && old.Version == Version && old.Digest == r.Digest && old.Hash == r.Hash && old.Through >= r.Until && int64(r.Until) <= from.Unix() {
				continue
			}
		} else if int64(r.Until) <= from.Unix() {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for start := 0; start < len(ids); start += 16 {
		batch, err := h.Sink.Rounds(ctx, ids[start:min(start+16, len(ids))])
		if err != nil {
			return err
		}
		if len(batch) != min(16, len(ids)-start) {
			return fmt.Errorf("verified roster disappeared")
		}
		started := time.Now()
		rows, err := h.Source.Collect(ctx, batch, to)
		if err != nil {
			return err
		}
		if err = h.Sink.WriteGeo(ctx, rows); err != nil {
			return err
		}
		h.Log.Info("validator locations published", "rounds", len(rows), "elapsed", time.Since(started))
	}
	return nil
}
