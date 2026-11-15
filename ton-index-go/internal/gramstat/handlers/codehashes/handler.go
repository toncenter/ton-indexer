package codehashes

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/toncenter/ton-indexer/ton-index-go/internal/gramstat"
)

type Source interface {
	Collect(context.Context) (*Snapshot, error)
}

type Sink interface {
	Publish(context.Context, *Snapshot) error
}

type Handler struct {
	Source Source
	Sink   Sink
	Log    *slog.Logger
}

func (*Handler) Name() string { return "account_code_hashes" }

func (h *Handler) Run(ctx context.Context, req gramstat.Request) error {
	if !req.From.IsZero() || !req.To.IsZero() {
		return fmt.Errorf("account_code_hashes observes latest indexes; historical dates are unsupported")
	}
	snapshot, err := h.Source.Collect(ctx)
	if err != nil {
		return err
	}
	t, err := snapshot.summary()
	if err != nil {
		return err
	}
	if err := h.Sink.Publish(ctx, snapshot); err != nil {
		return err
	}
	if h.Log != nil {
		h.Log.Info("code-hash snapshot published", "hashes", t.Hashes, "indexed_accounts", t.Accounts,
			"collection_elapsed", snapshot.FinishedAt.Sub(snapshot.StartedAt), "pages", snapshot.Pages,
			"zero_hashes", snapshot.ZeroHashes, "source_mc_min", t.MCMin, "source_mc_max", t.MCMax)
	}
	return nil
}
