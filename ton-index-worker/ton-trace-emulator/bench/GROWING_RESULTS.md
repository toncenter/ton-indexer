# Long-lived traces with small fragments, 2026-09-15

Four simultaneous traces, one cohort, one connected fragment per trace per block.
Every trace starts small and gradually reaches 1000 or 5000 transactions. Fragments
contain 5 or 10 transactions and form a binary tree of short chains; future
transactions are not preloaded into the snapshot. See `generate_growing` in
`SyntheticTrace.h`.

Settings: 8 MCH workers, 7 actor scheduler threads, Release build, 100 ms block
interval, 4096 accounts, 32 payload bytes. Same AMD Ryzen 9 7950X3D host and private
Redis 8.6.2 as earlier tests. Redis has no persistence or subscribers. These are
synthetic successful TON transfers, without TON DB/TVM/tier-2 lookups.

**The benchmark's node limit is 5000; production still limits traces to 1000.**
Only the benchmark target defines `TON_TRACE_BENCH_MAX_CACHED_NODES=5000`.
Results for 5000 nodes describe the experimental larger cache, not current
production behavior (which would discard the oversized trace).

## Main runs: mixed pending/confirmed/finalized input

Pending supplies the first fragment; every confirmed fragment precedes its
finalized version by one scheduled block interval. Input is open-loop; finalized
commit remains one block at a time. Durations below exclude the final idle settle.

| Transactions per trace | New transactions per fragment | Offered new finalized tx/s | Updates per trace | Input duration | Fully drained at | Last finalized block lag | p95 finalized lag |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1000 | 5 | 200 | 200 | 20 s | 20.071 s | 71 ms | 67 ms |
| 1000 | 10 | 400 | 100 | 10 s | 10.066 s | 66 ms | 63 ms |
| 5000 | 5 | 200 | 1000 | 100 s | 251.048 s | 151.046 s | 141.734 s |
| 5000 | 10 | 400 | 500 | 50 s | 124.030 s | 74.030 s | 69.495 s |

All runs completed without callback or classification errors. The last trace in
Redis had exactly 1000/5000 stored transaction fields and MCH state `ok`.
The large runs reached backlogs of 407 and 202 blocks, including the active block.
The small runs never had a block waiting behind the active block.

For the large traces, median finalized **block service time** (processing start
through completion, including any active confirmed operation/MCH/Redis waits) was:

| Finalized transactions accumulated per trace | Fragment 5 | Fragment 10 |
| --- | ---: | ---: |
| 1–1000 | 35.5 ms | 35.6 ms |
| 1001–2000 | 147.9 ms | 147.2 ms |
| 2001–3000 | 255.0 ms | 247.9 ms |
| 3001–4000 | 349.0 ms | 333.1 ms |
| 4001–5000 | 415.5 ms | 416.0 ms |

Each block still adds only 20 or 40 distinct finalized transactions across all
four traces. Work per update depends strongly on the accumulated graph size.
The 20-block rolling median service time first exceeded the 100 ms arrival
interval at about 1150 finalized nodes/trace in the fragment-10 run. Confirmed
updates can already have added later nodes to the cached graph; the x-axis here
is finalized progress, not an independent measurement of exact cached size.

## Where time goes

In the 5000-node runs, the MCH pool reached approximately 3.35–3.40 busy cores,
TraceProcessor 0.77, RedisMaterializer 0.06–0.07; Redis server CPU peaked at
0.05–0.08 cores. Actor values are elapsed message execution time, including OS
preemption, rather than literal per-actor CPU. At most four classifications can
run concurrently for these four traces, because operations on one trace remain
serial even with eight available classifier workers.

The existing classification histogram recorded **4004 classifications** for
fragment 10 and **8004** for fragment 5. The last trace's version was 1001/2001:
pending plus all confirmed and finalized updates were actually written. In this
scenario the priority queues did not eliminate that repeated work. Classification
p95 was about 206/215 ms, compared with the 100 ms input interval; this timer
starts at classifier dispatch and includes conversion/classification/response
latency. It is not a CPU timer.

The full accumulated graph is passed to MCH again on each update, and full action
data is prepared again afterward. Halving fragment size doubles the number of
updates for the same final graph, explaining the approximately doubled total
processing time. The earlier 14–15k tx/s result for short traces cannot be applied
to these long-lived traces.

## Finalized-only control and CPU profile

Removing pending/confirmed input while keeping four traces, 5000 nodes, fragments
of 10 and the same 400 tx/s reduced total processing time to **78.968 seconds**.
The final block still lagged by **28.968 seconds** (p95 24.450 seconds, maximum
backlog 116 blocks). Redis retained all 5000 nodes; all writes and classifications
succeeded. The repeated full-graph work remains expensive even without confirmed.

An independent diagnostic build profiled the same finalized-only scenario with
`CLOCK_THREAD_CPUTIME_ID`, subtracting nested scopes for exclusive CPU. Timers
apply to synchronous TraceProcessor/TraceAssembler/TraceState work and exclude
waiting for MCH and Redis. Capacity timings above use the ordinary executable.
The instrumented call tree recorded 37.412 seconds of CPU; approximate shares:

| Non-overlapping work inside TraceProcessor | Share of measured CPU |
| --- | ---: |
| Prepare full action state and Redis write plan (`prepare_trace_materialization`) | 43.2% |
| Build the full trace view for MCH | 13.6% |
| Recompute node/index delta over the accumulated graph (`graph.make_delta`) | 13.5% |
| Copy the current snapshot | 9.0% |
| Remaining update, callback, destruction and lifecycle work | 20.7% |

The action/Redis row includes action-state preparation (19.4% of total CPU),
other materialization/copy/destruction work in its parent (10.4%), and full
streaming transaction/action hints (about 11.9%). These are nested breakdowns,
not additional CPU to add to the table.

Parsing new transactions/messages takes only **0.58%** and node MsgPack packing
**0.30%** of TraceProcessor CPU in this workload. A tiny incoming patch avoids
reparsing old nodes inside TraceProcessor, but snapshot copying, full delta/index
comparison, full-view construction and full action-state preparation still
scale with accumulated size. MCH separately reconverts/reclassifies the full view.
This differs substantially from the earlier profile with large incoming updates.

Raw control data: `/tmp/ton-growing-finalized-5000-by10/`.
CPU scopes and diagnostic results:
`/tmp/ton-growing-profile-finalized-5000-by10/cpu-profile.csv`.
The diagnostic build script and instrumented source copies are in
`/tmp/ton-trace-workers8-profile/`; `experiments.json` for these runs is saved
separately as `/tmp/ton-growing-experiments.json`.

## Reproduce

```sh
cmake --build ton-index-worker/build --parallel 4 --target bench-ton-trace-pipeline
python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/growing-5000-by5 \
  --blocks 1 --traces-per-block 4 --nodes-per-trace 5000 --fragment-txs 5 \
  --block-ms 100 --mode mixed --threads 7 --mch-workers 8 \
  --drain-seconds 180 --timeout 600
```

Change `--fragment-txs` to 10 or `--nodes-per-trace` to 1000 for the other cases.
`--blocks 1` means one cohort, not one finalized block: the cohort receives
`ceil(nodes/fragment-txs)` sequential block updates.

Raw summaries, block/actor CSVs, Redis samples and logs on this host:
`/tmp/ton-growing-{1000,5000}-by{5,10}/`. The normal two-update mode and growing
promotion mode also passed smoke runs. All **111 tests passed**, including
incremental assembly beyond 5000 nodes with a partial last fragment and the real
Redis tests. Both production and benchmark executables built successfully.
