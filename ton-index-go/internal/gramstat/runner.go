// Package gramstat runs independent analytics handlers over already indexed data.
package gramstat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Request has one stable UTC observation time. Explicit dates use [From, To);
// otherwise each handler chooses its own refresh/catch-up policy.
type Request struct {
	AsOf         time.Time
	From, To     time.Time
	LookbackDays int
}

// Handler owns its sources, calculation and output schema. It need not produce
// daily counters: a future geography handler can publish a complete typed snapshot.
type Handler interface {
	Name() string
	Run(context.Context, Request) error
}

func Run(ctx context.Context, log *slog.Logger, handlers []Handler, names []string, req Request, timeout time.Duration) error {
	registry := make(map[string]Handler, len(handlers))
	for _, h := range handlers {
		if _, exists := registry[h.Name()]; exists {
			return fmt.Errorf("duplicate handler %q", h.Name())
		}
		registry[h.Name()] = h
	}
	seen := map[string]bool{}
	for _, name := range names {
		if registry[name] == nil || seen[name] {
			return fmt.Errorf("unknown or repeated handler %q", name)
		}
		seen[name] = true
	}
	var failures []error
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		started := time.Now()
		hctx, cancel := context.WithTimeout(ctx, timeout)
		err := registry[name].Run(hctx, req)
		cancel()
		if err != nil {
			log.Error("handler failed", "handler", name, "elapsed", time.Since(started), "error", err)
			failures = append(failures, fmt.Errorf("%s: %w", name, err))
		} else {
			log.Info("handler finished", "handler", name, "elapsed", time.Since(started))
		}
	}
	return errors.Join(failures...)
}
