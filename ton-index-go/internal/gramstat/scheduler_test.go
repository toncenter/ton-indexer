package gramstat

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type scheduledTestHandler struct {
	name string
	run  func(context.Context, Request) error
}

func (h scheduledTestHandler) Name() string                               { return h.name }
func (h scheduledTestHandler) Run(ctx context.Context, req Request) error { return h.run(ctx, req) }

func TestSchedulerIsolationDeadlinesDependenciesAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		var active, peak, slowActive atomic.Int32
		type event struct {
			name string
			req  Request
		}
		events := make(chan event, 100)
		makeHandler := func(name string, slow bool) Handler {
			return scheduledTestHandler{name, func(ctx context.Context, req Request) error {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				if slow {
					if slowActive.Add(1) != 1 {
						t.Error("overlapping handler")
					}
					defer slowActive.Add(-1)
				}
				if name == "child" && slowActive.Load() != 0 {
					t.Error("dependency still running")
				}
				events <- event{name, req}
				if slow {
					<-ctx.Done()
					return ctx.Err()
				}
				if name == "child" {
					return errors.New("independent source unavailable")
				}
				return nil
			}}
		}
		hs := []Handler{makeHandler("slow", true), makeHandler("child", false), makeHandler("fast", false)}
		specs := []Schedule{{Name: "slow", Every: time.Second, Timeout: 3 * time.Second, ReconcileEvery: 10 * time.Second},
			{Name: "child", Every: time.Second, Timeout: time.Second, ReconcileEvery: 10 * time.Second, After: []string{"slow"}},
			{Name: "fast", Every: time.Second, Timeout: time.Second, ReconcileEvery: 10 * time.Second}}
		finished := make(chan error, 1)
		go func() { finished <- Serve(ctx, log, hs, specs, 2, Request{LookbackDays: 3}) }()
		synctest.Wait()
		time.Sleep(13 * time.Second)
		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if active.Load() != 0 || peak.Load() != 2 {
			t.Fatal("concurrency/shutdown", active.Load(), peak.Load())
		}
		close(events)
		counts := map[string]int{}
		var previous time.Time
		var recent, reconcile bool
		for e := range events {
			counts[e.name]++
			if e.name == "slow" {
				if !previous.IsZero() && e.req.AsOf.Sub(previous) < 4*time.Second {
					t.Fatal("queued missed ticks", e.req.AsOf, previous)
				}
				previous = e.req.AsOf
				recent = recent || e.req.RecentOnly
				reconcile = reconcile || !e.req.RecentOnly
			}
		}
		if counts["fast"] < 10 || counts["child"] < 3 || counts["slow"] != 4 || !recent || !reconcile {
			t.Fatal(counts, recent, reconcile)
		}
	})
}

func TestSchedulerValidatesWholeGraphBeforePublishing(t *testing.T) {
	h := scheduledTestHandler{"job", func(context.Context, Request) error { t.Fatal("ran invalid schedule"); return nil }}
	good := Schedule{Name: "job", Every: time.Second, Timeout: time.Second, ReconcileEvery: time.Hour}
	for _, specs := range [][]Schedule{nil, {good, good}, {{Name: "missing", Every: time.Second}},
		{{Name: "job", Every: time.Second, Timeout: time.Second, ReconcileEvery: time.Hour, After: []string{"job"}}},
		{{Name: "job", Every: time.Second, Timeout: time.Second, ReconcileEvery: time.Hour, After: []string{"missing"}}}} {
		if err := Serve(context.Background(), slog.Default(), []Handler{h}, specs, 2, Request{}); err == nil {
			t.Fatal("accepted invalid graph", specs)
		}
	}
}
