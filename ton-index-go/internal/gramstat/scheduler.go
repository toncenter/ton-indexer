package gramstat

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Schedule orders overlapping publications without coupling failure/retry state.
// After waits for due/running predecessors to finish, not necessarily succeed:
// consumers independently verify the identities of their prepared inputs.
type Schedule struct {
	Name                           string
	Every, Timeout, ReconcileEvery time.Duration
	After                          []string
}

type scheduledState struct {
	Schedule
	handler         Handler
	next, reconcile time.Time
	running         bool
}

func scheduleStates(handlers []Handler, specs []Schedule, concurrency int) ([]*scheduledState, error) {
	if concurrency < 1 || concurrency > 16 || len(specs) == 0 {
		return nil, fmt.Errorf("scheduler requires jobs and concurrency 1..16")
	}
	registry := map[string]Handler{}
	for _, h := range handlers {
		if registry[h.Name()] != nil {
			return nil, fmt.Errorf("duplicate handler %q", h.Name())
		}
		registry[h.Name()] = h
	}
	byName := map[string]*scheduledState{}
	var states []*scheduledState
	for _, spec := range specs {
		if registry[spec.Name] == nil || byName[spec.Name] != nil {
			return nil, fmt.Errorf("unknown or repeated handler %q", spec.Name)
		}
		if spec.Every <= 0 || spec.Timeout <= 0 || spec.ReconcileEvery < spec.Every {
			return nil, fmt.Errorf("invalid schedule for %s", spec.Name)
		}
		s := &scheduledState{Schedule: spec, handler: registry[spec.Name]}
		byName[spec.Name] = s
		states = append(states, s)
	}
	visited, visiting := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if byName[name] == nil {
			return fmt.Errorf("unknown schedule dependency %q", name)
		}
		if visiting[name] {
			return fmt.Errorf("cyclic schedule dependency %q", name)
		}
		if visited[name] {
			return nil
		}
		visiting[name] = true
		for _, dep := range byName[name].After {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[name], visited[name] = false, true
		return nil
	}
	for name := range byName {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return states, nil
}

// Serve runs until cancellation. Cadences are UTC-aligned, missed ticks coalesce,
// and a handler is never launched twice concurrently, even after its deadline.
// Shutdown cancels in-flight I/O and waits for handlers to release their sources.
func Serve(ctx context.Context, log *slog.Logger, handlers []Handler, specs []Schedule, concurrency int, req Request) error {
	states, err := scheduleStates(handlers, specs, concurrency)
	if err != nil {
		return err
	}
	if !req.From.IsZero() || !req.To.IsZero() || req.RecentOnly {
		return fmt.Errorf("daemon does not accept explicit dates or --recent-only")
	}
	byName := map[string]*scheduledState{}
	for _, s := range states {
		byName[s.Name] = s
	}
	type completion struct {
		state             *scheduledState
		started, finished time.Time
		err               error
	}
	done := make(chan completion, len(states))
	running := 0
	cancelled := ctx.Done()
	stopping := false
	log.Info("scheduler started", "jobs", len(states), "concurrency", concurrency)
	for {
		if ctx.Err() != nil {
			stopping = true
			cancelled = nil
		}
		if stopping && running == 0 {
			log.Info("scheduler stopped")
			return nil
		}
		now := time.Now().UTC()
		if !stopping {
			for _, s := range states {
				if running >= concurrency {
					break
				}
				if s.running || s.next.After(now) {
					continue
				}
				blocked := false
				for _, name := range s.After {
					d := byName[name]
					blocked = blocked || d.running || !d.next.After(now)
				}
				if blocked {
					continue
				}
				job := req
				job.AsOf = now
				job.RecentOnly = s.reconcile.After(now)
				mode := "recent"
				if !job.RecentOnly {
					mode = "reconcile"
					s.reconcile = now.Add(s.ReconcileEvery)
				}
				s.running = true
				running++
				log.Info("handler started", "handler", s.Name, "mode", mode, "as_of", now, "interval", s.Every, "timeout", s.Timeout)
				go func(s *scheduledState, job Request) {
					started := time.Now()
					hctx, cancel := context.WithTimeout(ctx, s.Timeout)
					err := s.handler.Run(hctx, job)
					if err == nil {
						err = hctx.Err()
					}
					cancel()
					done <- completion{s, started, time.Now(), err}
				}(s, job)
			}
		}
		// Due jobs blocked on a predecessor/capacity wake on completion, not a
		// zero-duration timer. An overlong run never queues catch-up executions.
		var timer *time.Timer
		var tick <-chan time.Time
		if !stopping && running < concurrency {
			var next time.Time
			for _, s := range states {
				if !s.running && s.next.After(now) && (next.IsZero() || s.next.Before(next)) {
					next = s.next
				}
			}
			if !next.IsZero() {
				timer = time.NewTimer(time.Until(next))
				tick = timer.C
			}
		}
		select {
		case <-cancelled:
			stopping, cancelled = true, nil
		case c := <-done:
			c.state.running = false
			running--
			c.state.next = c.finished.UTC().Truncate(c.state.Every).Add(c.state.Every)
			if c.err != nil {
				log.Error("handler failed", "handler", c.state.Name, "elapsed", c.finished.Sub(c.started), "next_run", c.state.next, "error", c.err)
			} else {
				log.Info("handler finished", "handler", c.state.Name, "elapsed", c.finished.Sub(c.started), "next_run", c.state.next)
			}
		case <-tick:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
