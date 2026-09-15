# Trace pipeline benchmark

An opt-in executable feeding generated `TraceUpdate`s into the **real**
`TraceProcessor`, persistent MCH workers, Redis materializer/transport and retention
cleanup. Redis write concurrency remains 64. The benchmark alone compiles with a
5000-node trace limit (`TON_TRACE_BENCH_MAX_CACHED_NODES`); production retains its
1000-node limit. Results above 1000 nodes describe this experimental limit, not
current production behavior. The compiled limit is included in every summary.

It excludes TON DB reads, block parsing, TVM execution, interface detection and
MCH tier-2 DB lookups. The source models a single finalized block committing at a
time, but does not instantiate `TraceScheduler`. Nonfinalized updates are offered
even during finalized processing to stress admission, priority and supersession;
upstream Scheduler admission would reduce that load in production.

## Run

From the repository root, using an existing Release build:

```sh
cmake --build ton-index-worker/build --parallel 4 --target bench-ton-trace-pipeline
python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/trace-bench-w8 \
  --blocks 40 --block-ms 250 --traces-per-block 100 --nodes-per-trace 15 \
  --mode mixed --mch-workers 8
```

The Python runner needs only Python's standard library, `redis-server` and
`redis-cli`. It starts an empty temporary Redis on a private Unix socket, disables
persistence, and stops only its own processes. It never connects to an existing
Redis or runs `FLUSHDB`. Each run requires a new output directory. Direct execution
requires `--redis` and `--output-dir`; use a disposable Redis for that too.

## Input

The source pre-generates a finite dataset outside the timed phase. By default every cohort
contains distinct balanced binary traces. The head update contains the entire
graph: internal transactions are committed, leaves are still emulated. The next
block commits those leaves as separate fragments of the same update. Thus a
1000-node trace has a second update of 500 fragments. One-node traces need only
one block. A run of N cohorts has N+1 finalized blocks (N for one-node traces).

With `--fragment-txs 5` or `10`, each trace instead grows by one connected fragment
per block. Fragments are short chains arranged in a binary tree, so total depth
does not become thousands of nodes. Only the current fragment's transactions
enter the snapshot; future fragments are represented by outgoing message hashes.
The final fragment may be smaller. Pending includes the first fragment only;
confirmed precedes each finalized fragment by one interval in mixed/promotion mode.
There are `ceil(nodes / fragment-txs)` updates per trace and
`cohorts + updates_per_trace - 1` finalized blocks in total.

For four long-lived traces, each receiving five transactions per 100 ms:

```sh
python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/growing-5000-by5 \
  --blocks 1 --traces-per-block 4 --nodes-per-trace 5000 --fragment-txs 5 \
  --block-ms 100 --mode mixed --threads 7 --drain-seconds 180 --timeout 600
```

Here `--blocks 1` creates **one cohort**, which receives 1000 sequential block
updates over 100 seconds of offered input. Offered rate is 200 new finalized
transactions/s, regardless of the growing cached graph size. Every accepted
update still classifies the full accumulated graph. Increasing the cohort count
introduces new traces every block, so overlapping cohorts increase the peak rate.
Mean and steady/peak offered transaction rates are reported separately.

Cells contain parsable ordinary successful TON-transfer transactions, messages
with configurable bodies, and active account states. These are synthetic records,
not TVM-validated executions. MCH uses its actual converter, matcher engine and
action serializer, but **TON transfers do not represent DEX/NFT classification
cost or production interface lookups**. Redis has no stream subscribers and uses
local memory with persistence disabled, so its result is not a production capacity
estimate either.

Modes:

- `finalized`: head and tail finalized updates only.
- `mixed` (default): pending and confirmed arrive one block interval before their
  finalized counterpart; finalized always uses ordinary insertion. Obsolete
  queued work is removed by the real TraceProcessor.
- `promotion`: same arrivals; promote a block's confirmed snapshots if all are
  available when its finalized processing starts. Otherwise use ordinary finalized
  updates. A failed promotion also falls back. Counts show how often each path ran.

The block clock never waits for completion. Its original deadlines remain fixed
if the source actor runs late. Finalized blocks wait for the previous block's
completion, preserving the production commit barrier. Lag includes this wait;
the source's own lateness is reported separately. Input stops after the configured
number of blocks, followed by bounded draining and an idle retention period.

| Option | Default | Meaning |
| --- | ---: | --- |
| `--blocks` | 20 | Trace cohorts |
| `--block-ms` | 250 | Fixed block arrival interval |
| `--traces-per-block` | 100 | New traces per cohort |
| `--nodes-per-trace` | 15 | Transactions per trace, 1–5000 in the benchmark |
| `--fragment-txs` | 0 | 0: original two-update scenario; positive: new transactions per connected fragment |
| `--accounts` | 4096 | Shared account pool; fewer accounts increase index contention |
| `--payload-bytes` | 32 | Comment bytes per message, in addition to opcode/id |
| `--threads` | 8 | Actor scheduler CPU threads |
| `--mch-workers` | 8 | Persistent classifier workers; 0 disables MCH for comparison |
| `--retention-ms` | 3000 | Completed trace retention; pending/open defaults unchanged |
| `--settle-ms` | 1000 | Idle time after callbacks drain; increase to exercise all cleanup |
| `--drain-seconds` | 30 | Maximum extra wait after the last scheduled arrival |
| `--seed` | 1 | Deterministic trace/account identity namespace |
| `--no-measurements` | off | Disable per-update Measurement instrumentation |
| `--timeout` | 300 | Runner wall-clock timeout, including generation |
| `--binary` | build target | Runner override for another build |

Input is capped at one million distinct transactions and an estimated 1 GB of
cell payload. Actual memory is larger because phase variants, graphs and caches
also occupy space. `peak_rss_kib_including_generation` explicitly includes the
dataset; it is not just the production actor's cache.

## Results and comparisons

`summary.json` contains options/command, host information, finalized lag
percentiles, backlog, callback errors, promotion/fallback counts, process CPU,
actor busy time and Redis CPU/memory/command counters. Additional files:

- `blocks.csv`: planned arrival, actual processing start and completion, lag,
  source lateness, success and expected maximum trace size for every completed
  finalized block. Confirmed may already have introduced later nodes in mixed mode.
- `samples.csv`: one-second progress, outstanding callbacks and process CPU.
- `actors.csv`: actor busy cores per sampling interval, plus lifetime maxima of
  message duration, actor execution duration and mailbox wait.
- `redis-samples.json`: Redis CPU/memory over time (includes input generation).
- `application-stats.txt`: the existing pipeline's counters and histograms.
- `benchmark.log`: existing queue and MCH statistics (every ten seconds), warnings
  and the first few callback errors; `redis.log`: private server log.

`max_block_backlog` includes the active block: 1 means there was no block waiting
behind it. Callback success can mean a superseded update was skipped, so callback
counts are **not** Redis-write throughput. Nonfinalized admission rejections are
reported separately and may be expected in overload. Failed finalized blocks or a
drain timeout return a nonzero exit status; failed blocks are excluded from lag
percentiles. On timeout inspect outstanding work, not just completed-block lag.

Actor busy time uses the same RDTSC instrumentation as production actor stats.
It measures elapsed time inside actor messages, including OS preemption, **not
literal CPU time**. 1 busy core for TraceProcessor means its single actor is busy
throughout the interval; worker stats aggregate all workers. True process CPU uses
`CLOCK_PROCESS_CPUTIME_ID`; Redis CPU comes from `INFO`. Short pauses, profiling
overhead and other processes can affect results. Use CSV time series as well as
peaks, and repeat runs with no concurrent builds.

The runner also reads the last trace's `mch_classify_state`, root and version,
and counts its stored transaction fields (`retained_nodes`, `expected_nodes`,
`complete`). Check completeness to detect dropped, expired or truncated traces.
`ok`/`unknown` indicate completed classification; `convert_failed`/`fallback` or
MCH warnings require investigation before treating a run as representative.
These fields are empty if that trace has already expired during draining/settling.

Suggested comparisons, changing one parameter at a time:

1. `mixed`, workers 1, 4 and 8, same input. Does finalized lag stop accumulating?
2. Repeat with smaller `--block-ms` until lag grows over the run. This tests
   sustainable arrival rate, not just how fast a finite backlog drains.
3. `finalized` and `promotion` at the same rate. Promotion has to report successful
   promoted blocks for this comparison to measure that path.
4. Few large traces: `--traces-per-block 2 --nodes-per-trace 1000`.
5. In `finalized` mode, `--mch-workers 0` to compare without classification.
   In `mixed`, disabling MCH also changes how much nonfinalized work gets processed
   before supersession; it is not a clean subtraction of classifier cost.
6. Longer retention for large live caches; short retention plus
   `--settle-ms 5000` for cleanup. Redis `UNLINK`/`ZREM` counters verify cleanup ran.

To diagnose the first bottleneck, look for growing finalized lag together with a
busy TraceProcessor, a saturated classifier pool, or high Redis CPU and write
latency. This benchmark identifies candidates and relative regressions; validating
production headroom needs a representative transaction mix and production-like
Redis deployment.

Recorded measurements: [initial comparison](RESULTS.md),
[eight workers and CPU profile](WORKERS8_RESULTS.md), and
[long-lived traces with small fragments](GROWING_RESULTS.md), and
[costs inside MCH classification](MCH_RESULTS.md), and
[MCH enabled versus disabled](MCH_DISABLED_RESULTS.md).
