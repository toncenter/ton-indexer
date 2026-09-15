# Eight MCH workers: capacity and TraceProcessor CPU, 2026-09-15

The default is now 8 in `mch::EmuClassifierConfig`; the emulator and benchmark
take their default from that configuration. CLI help also says 8. Both executables
built successfully and all 110 TraceProcessor/TraceState tests passed, including
real Redis tests. Load runs omitted `--mch-workers`, exercising the new default.

## Capacity

AMD Ryzen 9 7950X3D, Release build, **7 scheduler threads** (the emulator's default),
Redis 8.6.2 on a private Unix socket, no persistence/subscribers. Each trace has
15 transactions, 4096 shared accounts, 32 payload bytes. Block interval is fixed
at 150 ms; new traces per cohort increase with offered load. Completed retention
is 3 seconds and final settle is 1 second. This differs from the first report's
8 scheduler threads and shorter runs, so it is not a direct workers-4 vs workers-8
comparison.

`mixed` mode supplies pending and confirmed updates in addition to finalized.
Production TraceProcessor admission, supersession and cleanup all run. The source
continues supplying nonfinalized work while finalized is committing; production
TraceScheduler would reduce that input. TON DB, TVM and MCH tier-2 lookups remain
outside this benchmark. These figures describe this synthetic transfer workload.

| Offered finalized tx/s | Main input duration | Finalized lag p95 | Maximum block backlog, including active | Observation |
| --- | ---: | ---: | ---: | --- |
| 10,000 | 12 s | 139 ms | 1 | No waiting block |
| 12,500 | 12 s | 199 ms | 2 | Brief waits, no substantial accumulation |
| 14,000 | 24 s, twice | 223–280 ms | 2–3 | Borderline; bounded fluctuations in these runs |
| 15,000 | 24 s, twice | 568–1,193 ms | 4–8 | Persistent accumulation in both runs |
| 16,000 | 24 s | 2,046 ms | 14 | Clear accumulation |
| 17,500 | 12 s | 2,127 ms | 13 | Clear accumulation |
| 20,000 | 12 s | 4,315 ms | 23 | Clear accumulation; nonfinalized admission starts rejecting work |

The observed transition is approximately **14–15 thousand finalized tx/s** with
this mixed input. At 14,000, average lag in the early/late quarter was
143–148 / 180–206 ms. At 15,000, it was 172–202 / 535–1,116 ms; the last block
finished 566–1,231 ms behind its deadline. Linear lag growth was 19–53 ms per
second at 15,000, versus about 2 ms per second at 14,000. Calculations exclude the
first ten blocks and the final tail-only block. A finite run cannot establish an
exact permanent capacity; 14,000 already has little headroom.

A control run with **only finalized** updates at 17,500 tx/s also accumulated lag:
p95 1,063 ms, maximum backlog 7 blocks, about 94 ms of additional lag per second.
Nonfinalized work worsens the result, but it is not the sole cause of saturation.

All finalized writes succeeded. At 20,000 there were 3,442 **nonfinalized**
admission rejections at the existing backlog limit of 10,000; lower-rate capacity
runs had none. At overload TraceProcessor occupied approximately one busy core,
all eight classifiers together about 1.5 busy cores, and Redis peaked at roughly
0.4–0.45 CPU cores. Actor busy time includes OS preemption; Redis CPU uses `INFO`.

## CPU inside TraceProcessor

An independent diagnostic executable instruments copies of TraceProcessor,
TraceAssembler and TraceState with `CLOCK_THREAD_CPUTIME_ID` scopes. Each scope
records inclusive CPU, exclusive CPU (nested scopes subtracted), call count and
maximum inclusive CPU. Timers cover synchronous work; they do not count time
waiting for MCH/Redis. Instrumentation is enabled only during the timed workload,
and reports are written after scheduler shutdown. Production sources have no
profiling hooks. Capacity numbers above use the ordinary executable.

The mixed profile offered 17,500 tx/s for 80 cohorts and measured 17.10 seconds of
CPU in the instrumented TraceProcessor call tree. Percentages below are approximate:
3.29 million scope calls add overhead and change how much nonfinalized work finishes
before being superseded. Do not use this diagnostic run's latency as a capacity
measurement.

| Non-overlapping major stage | Measured CPU | Share |
| --- | ---: | ---: |
| `TraceAssembler::apply_update`, including its helpers | 10.263 s | 60.0% |
| `prepare_trace_materialization`, including Redis/account/action helpers | 3.387 s | 19.8% |
| `TraceAssembler::build_full_trace` for MCH | 0.288 s | 1.7% |
| Remaining queue, callback, lifecycle and cleanup work | 3.166 s | 18.5% |

Breakdown **inside the 60% apply_update share**, expressed as percentages of the
same total measured TraceProcessor CPU (do not add these to the major-stage table):

- Parse transaction and messages: **18.9%**, 337,296 calls.
- Serialize Redis node to MsgPack: **10.8%**, 337,296 calls.
- Serialize transaction cell to BOC: **5.8%**, 337,296 calls.
- Initial snapshot copy: **2.0%**, 30,178 copies.
- Remaining 22.5%: node preparation/fingerprints/strings, traversal, graph changes,
  delta/index generation, metadata and temporary object destruction.

Within Redis preparation, account parsing/packing takes about 7.0% of total CPU,
action-state preparation 4.0%, publications about 3.6%, and the remainder is plan
construction and other materialization work. These are CPU costs before sending
the write, not Redis server latency.

Finalized supersession scanning itself takes about 1.35% exclusive CPU;
`schedule_trace` 0.65%; `update_lifecycle` 0.36%; the alarm body 0.10% exclusive.
The cache here contains thousands of traces, so this does not rule out costly
scans with hundreds of thousands of retained traces.

Large traces confirm the shape: 24 cohorts, 2 traces/cohort, 1000 nodes/trace,
250 ms interval, with a 500-fragment tail update. Of 5.53 seconds measured CPU,
apply_update takes 65.8%, Redis preparation 19.9%, snapshot copy only 2.7%.
The largest apply_update call takes 29.1 ms of actual thread CPU.

The next optimization candidate is repeated transaction conversion in
`TraceAssembler.cpp::prepare_state_node`. Cache reuse currently requires a full
fingerprint match; that fingerprint includes finality and block metadata.
Consequently, changing finality can reparse/repack a transaction and regenerate
its BOC even when the transaction cell is unchanged. Reusing immutable transaction
data is worth investigating before another graph-merge redesign. This is a
code-based inference supported by the profile, not a measured speedup of a fix.

## Reproduce and raw data

```sh
python3 ton-index-worker/ton-trace-emulator/bench/run.py \
  --output-dir /tmp/workers8-15000 \
  --blocks 160 --block-ms 150 --traces-per-block 150 --nodes-per-trace 15 \
  --mode mixed --threads 7 --mch-workers 8
```

Change `--traces-per-block` to 140 for 14,000 or 160 for 16,000 tx/s.

On the measurement host, each run has command/summary JSON, block and actor CSVs,
Redis samples and application logs:

- `/tmp/ton-trace-w8-rate-*`: initial 12-second sweep.
- `/tmp/ton-trace-w8-long-*`, `/tmp/ton-trace-w8-repeat-*`: 24-second runs.
- `/tmp/ton-trace-w8-finalized-17500`: finalized-only control.
- `/tmp/ton-trace-w8-profile-mixed/cpu-profile.csv`: detailed regular-trace CPU.
- `/tmp/ton-trace-w8-profile-large/cpu-profile.csv`: detailed large-trace CPU.
- `/tmp/ton-trace-workers8-profile/`: diagnostic sources, timer header, exact
  compiler/linker commands, build script and an aggregate `experiments.json`.
  `python3 /tmp/ton-trace-workers8-profile/build_profile.py --build` regenerates
  the diagnostic binary from the current source; replacement anchors are checked.

See [README.md](README.md) for workload semantics and [RESULTS.md](RESULTS.md) for
the earlier workers-1/workers-4 measurements.
