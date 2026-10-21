package router

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ton_router_requests_total",
		Help: "Execute requests by outcome: ok, not_reached, bad_request, backend_error",
	}, []string{"outcome"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ton_router_request_duration_seconds",
		Help:    "Duration of Execute requests by outcome",
		Buckets: prometheus.ExponentialBuckets(0.0001, 4, 10),
	}, []string{"outcome"})

	callResults = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ton_router_call_results_total",
		Help: "Results of the calls of answered requests, by call kind and result: the kind of result or the error code",
	}, []string{"kind", "result"})

	lastBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ton_router_last_answered_block",
		Help: "Masterchain seqno of the last answered request",
	})

	blockLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ton_router_answer_lag_seconds",
		Help:    "Time from the gen_utime of a block to an answer for it: the age of the blocks asked about",
		Buckets: []float64{1, 2, 5, 10, 30, 60, 300, 3600, 86400, 30 * 86400, 365 * 86400},
	})
)
