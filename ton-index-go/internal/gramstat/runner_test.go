package gramstat

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type testHandler struct {
	name string
	run  func(context.Context) error
}

func (h testHandler) Name() string                             { return h.name }
func (h testHandler) Run(ctx context.Context, _ Request) error { return h.run(ctx) }

func TestRunnerIsolatesFailureAndValidatesBeforeRunning(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var ran []string
	hs := []Handler{
		testHandler{"failed", func(context.Context) error { ran = append(ran, "failed"); return errors.New("source unavailable") }},
		testHandler{"good", func(context.Context) error { ran = append(ran, "good"); return nil }},
	}
	if err := Run(context.Background(), log, hs, []string{"failed", "good"}, Request{}, time.Second); err == nil || len(ran) != 2 {
		t.Fatalf("expected isolated failure and both handlers, got %v, %v", err, ran)
	}
	ran = nil
	if err := Run(context.Background(), log, hs, []string{"good", "unknown"}, Request{}, time.Second); err == nil || len(ran) != 0 {
		t.Fatalf("invalid selection must fail before writes: %v, %v", err, ran)
	}
}

func TestUTCWindowAndCatchUp(t *testing.T) {
	date := func(s string) time.Time { d, _ := time.Parse(time.DateOnly, s); return d }
	asof := time.Date(2026, 10, 28, 2, 0, 0, 0, time.FixedZone("east", 3*3600))
	from, to, err := (Request{AsOf: asof, LookbackDays: 3}).DailyWindow()
	if err != nil || !from.Equal(date("2026-10-24")) || to.Hour() != 23 || to.Day() != 27 {
		t.Fatalf("UTC window: %v %v %v", from, to, err)
	}
	stored := map[string][]time.Time{"a": {date("2026-10-21"), date("2026-10-23")}, "b": {date("2026-10-21")}}
	if got := CatchUpStart(date("2026-10-25"), date("2026-10-28"), []string{"a", "b"}, stored); !got.Equal(date("2026-10-22")) {
		t.Fatalf("missed interior day: %v", got)
	}
	if got := CatchUpStart(date("2026-10-25"), date("2026-10-28"), []string{"a", "new"}, stored); !got.Equal(date("2026-10-21")) {
		t.Fatalf("new metric history: %v", got)
	}
	if _, _, err := (Request{AsOf: asof, LookbackDays: 3, From: date("2026-10-21")}).DailyWindow(); err == nil {
		t.Fatal("unpaired explicit dates accepted")
	}
}
