package observability

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestAttributesToKeyValuesPreservesStreamingNumbers(t *testing.T) {
	converted := attributesToKeyValues(map[string]any{
		"ton.streaming.update_seq":      uint64(42),
		"ton.streaming.worker_queue_ms": 12.5,
	})

	values := make(map[string]attribute.Value, len(converted))
	for _, item := range converted {
		values[string(item.Key)] = item.Value
	}

	updateSeq, ok := values["ton.streaming.update_seq"]
	if !ok || updateSeq.Type() != attribute.INT64 || updateSeq.AsInt64() != 42 {
		t.Fatalf("update_seq was not preserved as an integer: %#v", updateSeq)
	}

	workerQueue, ok := values["ton.streaming.worker_queue_ms"]
	if !ok || workerQueue.Type() != attribute.FLOAT64 || workerQueue.AsFloat64() != 12.5 {
		t.Fatalf("worker_queue_ms was not preserved as a float: %#v", workerQueue)
	}
}

func TestParentBasedSamplerInheritsEmulatorDecision(t *testing.T) {
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0))),
	)
	tracer := provider.Tracer("test")

	tests := []struct {
		name        string
		traceFlags  string
		wantSampled bool
	}{
		{name: "sampled by emulator", traceFlags: "01", wantSampled: true},
		{name: "skipped by emulator", traceFlags: "00", wantSampled: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rawTrace := map[string]string{
				otelDataField: fmt.Sprintf(
					`{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-%s"}`,
					test.traceFlags,
				),
			}
			parent, _, missing := extractStageContext(rawTrace)
			if missing {
				t.Fatal("emulator traceparent was not extracted")
			}

			_, span := tracer.Start(parent, "streaming-child")
			defer span.End()
			if got := span.SpanContext().IsSampled(); got != test.wantSampled {
				t.Fatalf("sampled = %v, want %v", got, test.wantSampled)
			}
		})
	}
}

func TestRateLimitedErrorHandlerAggregatesRepeatedErrors(t *testing.T) {
	now := time.Unix(1_000, 0)
	var logs []string
	handler := newRateLimitedErrorHandler(
		otelErrorLogInterval,
		func() time.Time { return now },
		func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) },
	)

	handler.Handle(errors.New("first failure"))
	handler.Handle(errors.New("hidden failure"))
	now = now.Add(otelErrorLogInterval)
	handler.Handle(errors.New("next failure"))

	if len(logs) != 2 {
		t.Fatalf("got %d logs, want 2: %v", len(logs), logs)
	}
	if !strings.Contains(logs[1], "1 OpenTelemetry errors suppressed") {
		t.Fatalf("second log does not report suppressed errors: %q", logs[1])
	}
}
