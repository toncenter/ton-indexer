# Initial measurements: 2026-09-15

Host: AMD Ryzen 9 7950X3D (16 cores / 32 logical CPUs), Linux x86-64,
Release build, 8 actor scheduler threads, Redis 8.6.2 on a private Unix socket,
no persistence or subscribers. Pipeline sources based on commit `984d6557`.
Redis write concurrency remained the production default of 64.

These are finite synthetic TON-transfer workloads, not network capacity estimates.
See [README.md](README.md) for boundaries and metric definitions. In particular,
actor busy time includes OS preemption; it is not measured per-actor CPU time.

## Small traces

100 new traces/cohort, 15 transactions/trace, 40 cohorts, 4096 accounts, 32 payload
bytes, completed retention 3 seconds, final settle 1 second. Each trace spans two
finalized blocks. The 10,000 tx/s mixed comparison was repeated sequentially;
ranges below show both runs, not confidence intervals.

| Mode | Offered tx/s | MCH workers | Finalized lag p95 | Max block backlog, including active | Peak TraceProcessor busy cores | Peak Redis CPU cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mixed | 10,000 | 1 | 2,628–2,981 ms | 13–14 | 0.47 | 0.17–0.19 |
| mixed | 10,000 | 4 | 100–114 ms | 1 | 1.00 | 0.30–0.34 |
| finalized | 10,000 | 4 | 99 ms | 1 | 0.61 | 0.26 |
| mixed | 20,000 | 4 | 826 ms | 10 | 1.00 | 0.33 |
| finalized | 20,000 | 4 | 571 ms | 8 | 0.99 | 0.34 |
| finalized | 20,000 | disabled | 109 ms | 2 | 0.97 | 0.28 |

With one classifier worker, lag grew throughout the 10,000 tx/s run. Four workers
removed the accumulated finalized backlog at that rate. At 20,000 tx/s,
TraceProcessor was busy throughout sampled intervals and lag accumulated even in
finalized-only mode. Four classifier workers together occupied about 1.55 busy
cores, so adding classifier workers alone is unlikely to remove this next limit.
Redis was not saturated in these runs.

Disabling MCH in *mixed* mode made this workload slower (p95 2,294 ms at 20,000
tx/s): the last trace received five writes instead of two. This changes how much
pending/confirmed work finishes before supersession, so it cannot be interpreted
as a measurement of classifier cost. Use the finalized-only comparison for that.

## Other paths

- Large traces: 24 cohorts, 2 traces/cohort, **1000 transactions/trace**, 250 ms
  block interval, mixed mode, 4 MCH workers. Each tail update has 500 fragments.
  Offered rate 8,000 tx/s, finalized p95 **206 ms**, no block waiting behind the
  active block. TraceProcessor peak busy time 0.93 cores; longest message 35 ms,
  longest actor execution 83 ms. This path still has noticeable uninterrupted work.
- Promotion with confirmed ready: 12 cohorts, 100 traces/cohort, 15 nodes/trace,
  500 ms interval, 4 workers. **13/13 blocks promoted**, no fallback, p95 84 ms.
  At 10,000 tx/s only 2/41 blocks promoted; the other 39 took ordinary finalization
  because the confirmed snapshots were not all ready.
- Retention: all 15 one-node traces removed after short retention and 2-second
  settle; Redis recorded 15 `UNLINK`s. Cleanup also ran during longer load tests.
- Deliberate overload with a one-second drain deadline exited with status 1 and
  `timed_out=1`, preserving progress, outstanding callbacks and actor statistics.
- All ordinary load cases completed without callback/finalized errors. The last
  retained trace had MCH state `ok` in every MCH-enabled load case. No MCH failure
  warnings appeared. All **110 tests passed**, including the new generated-cell
  fixture test and existing real Redis tests.

## Reproduce

For the principal comparison, run twice with different fresh output directories,
changing `--mch-workers 1` to `4`:

```sh
python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/trace-bench-10k-w1 \
  --blocks 40 --block-ms 150 --traces-per-block 100 --nodes-per-trace 15 \
  --mode mixed --mch-workers 1
```

Use `--block-ms 75` for 20,000 tx/s. Full commands, logs and CSV time series from
these measurements are in `/tmp/ton-trace-bench-v1-*` on the measurement host.
The next diagnostic step is profiling work inside TraceProcessor, especially
assembly, preparation of Redis/action data and promotion; these measurements
identify the actor, not the exact expensive function.
