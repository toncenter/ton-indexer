# MCH disabled: throughput and finalized lag, 2026-09-15

The production `--mch-disable` option leaves `EmuClassifierConfig::prep` null.
The benchmark's `--mch-workers 0` exercises the same TraceProcessor path: no MCH
actors are created. **Zero workers is a benchmark convention**; use
`--mch-disable` on `ton-trace-emulator`, whose worker option clamps to at least 1.

All runs use the ordinary Release benchmark executable, 7 scheduler threads,
private Redis 8.6.2 with no persistence/subscribers, 4096 accounts and 32 payload
bytes on the same Ryzen 9 7950X3D host. All finalized writes succeeded, all callback
error counts were zero, and the sample trace had its full expected node count.
Enabled runs use 8 MCH workers. Disabled runs intentionally have no classified
actions; their empty `mch_classify_state` is expected.

## Small traces: 15 transactions each

80 cohorts, 150 ms block interval. Traces/cohort are 150, 200 or 250 for the rates
below. Every trace has its usual two finalized updates. Scheduled input ends at
12.15 seconds, including the tail-only block.

| Input mode | Offered finalized tx/s | MCH | p95 finalized lag | Last finalized lag | Max backlog including active | Process CPU seconds |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| mixed | 15,000 | on | 326 ms | 299 ms | 3 | 34.43 |
| mixed | 15,000 | off | 1005 ms | 710 ms | 7 | 17.93 |
| finalized only | 20,000 | on | 2892 ms | 2951 ms | 17 | 41.41 |
| finalized only | 20,000 | off | 315 ms | 57 ms | 3 | 13.98 |
| finalized only | 25,000 | off | 1631 ms | 1575 ms | 11 | 17.29 |

Removing classification substantially reduces total CPU, but does not guarantee
better finalized latency in a mixed offered workload. In the 15k mixed comparison,
actual trace writes (`SETEX` for `tr_in_msg`) increased from **27,002 to 51,346**.
The classifier pool previously limited preparation, allowing more pending/confirmed
requests to be superseded before they were processed. With no classifier pool,
`start_next_operations` is no longer limited by idle classifier availability;
more nonfinalized work reaches the writer and consumes TraceProcessor CPU.

The synthetic source continues supplying confirmed updates even with a finalized
backlog. Production TraceScheduler stops starting new confirmed work then, so this
mixed result should not be presented as a universal production regression.

The finalized-only comparison has exactly 32,000 trace writes in both runs, making
it a cleaner measure of removed work. At 20k the disabled run had transient queues
but very little lag remaining at the end. At 25k its queue clearly accumulated.
These finite runs establish an observed range, not a precise sustainable ceiling.

## Long-lived traces: 5000 nodes, fragments of 10

Four simultaneous traces, one cohort, 500 updates per trace, 100 ms interval,
mixed pending/confirmed/finalized input. Offered load is 400 new finalized tx/s;
input ends at 50 seconds. The enabled baseline is the previously recorded run of
the same growing workload. Production code was unchanged between these runs.

| MCH | Fully drained at | Last finalized lag | p95 finalized lag | Max backlog including active | Process CPU seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| on | 124.030 s | 74.030 s | 69.495 s | 202 | 449.00 |
| off | 53.424 s | 3.424 s | 2.387 s | 25 | 37.87 |

Both runs performed **4004 trace writes**; the sample had version 1001 and all
5000 nodes. Here the amount of nonfinalized work was the same. Disabling MCH cut
total process CPU about 11.9-fold and total drain completion time about 2.3-fold.
It still did not fully sustain the offered 400 tx/s through the largest snapshots.

Median finalized block service time with MCH off, grouped by finalized progress
per trace: 11.8 ms at 1–1000 nodes, 29.7 ms at 1001–2000, 57.4 ms at 2001–3000,
95.3 ms at 3001–4000, and 130.0 ms at 4001–5000. The last range exceeds the 100 ms
arrival interval. TraceProcessor was approximately fully busy then; MCH no longer
appeared in actor statistics. Actor busy time is elapsed message time, not literal
CPU; process CPU above uses `CLOCK_PROCESS_CPUTIME_ID`.

**The 5000-node limit is benchmark-only. `--mch-disable` does not remove production's
1000-node cache limit**, which is checked before the classifier-enabled branch.

## Remaining work

TraceProcessor still assembles/copies the snapshot, calculates node/index deltas,
serializes raw transactions and accounts, manages queues/retention and writes Redis.
It currently even calls `TraceAssembler::build_full_trace` **before** checking that
the classifier pool is empty, then uses the view to populate a dummy completion.
Avoiding that unused classifier-view construction is a possible separate small
optimization, but was not applied in these measurements.

## Reproduce

```sh
python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/small-finalized-no-mch \
  --blocks 80 --traces-per-block 200 --nodes-per-trace 15 --block-ms 150 \
  --mode finalized --threads 7 --mch-workers 0

python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/growing-no-mch \
  --blocks 1 --traces-per-block 4 --nodes-per-trace 5000 --fragment-txs 10 \
  --block-ms 100 --mode mixed --threads 7 --mch-workers 0 --drain-seconds 60
```

Raw reports and time series on the measurement host are in
`/tmp/ton-mch-disable-*/`. The earlier large enabled baseline is
`/tmp/ton-growing-5000-by10/`. No production implementation was modified for this
comparison.
