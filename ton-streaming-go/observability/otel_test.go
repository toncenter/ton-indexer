package observability

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
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
