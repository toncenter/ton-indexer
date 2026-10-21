package extension

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// In the default registry, which the metrics listener of firecore serves.
// substreams itself counts the host invocations per tier in
// substreams_tier{1,2}_wasm_extension_call_counter, and the calls inside
// them by kind in its metering, as external_calls_ton_v1_<kind>.
var (
	executeCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fireton_ton_v1_execute_total",
		Help: "ton_v1.execute host invocations by outcome: ok, or error (non-deterministic)",
	}, []string{"outcome"})

	executeRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fireton_ton_v1_execute_retries_total",
		Help: "Retries of router requests the router could not answer yet (Unavailable)",
	})

	executeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fireton_ton_v1_execute_duration_seconds",
		Help:    "Duration of ton_v1.execute host invocations, waiting for the router included",
		Buckets: prometheus.ExponentialBuckets(0.0005, 4, 10),
	})
)
