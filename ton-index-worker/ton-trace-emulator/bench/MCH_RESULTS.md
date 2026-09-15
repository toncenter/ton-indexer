# Inside MCH classification, 2026-09-15

This isolates the real `EmuClassifierActor` on immutable snapshots of the growing
trace fixture, rather than inferring matcher cost from the whole actor's busy time.
Snapshots contain 1000, 2500 or 5000 finalized transactions from a 5000-node trace
with connected 10-transaction fragments. The snapshot is assembled before timing.
One classifier worker runs three warm-ups and then 20 repetitions; there is no
Redis, TON DB, TVM or tier-2 lookup in the measured call. Every result is `ok`,
with respectively 999, 2499 and 4999 actions.

The existing `classify_us` metric covers **conversion, classification, action
construction and output serialization**. It is not the time of a graph walk.
It also ends before the function's final local-object destruction.

## Measured costs

The diagnostic build uses CPU scopes in temporary source copies. A first version
timed individual nodes/matcher attempts, but that distorted cheap operations
(approximately 238 ms versus an uninstrumented 182 ms for 5000 nodes). The final
version times whole phases and whole matcher passes only: approximately 181 ms
versus 182 ms for the ordinary binary in that comparison. Use the latter profile
for timing attribution; the former remains useful for call counts.

At 5000 nodes, mean thread CPU per complete actor call, including local-object
destruction, was approximately 186 ms:

| Non-overlapping work | CPU per call |
| --- | ---: |
| BOC deserialization and parsing all schema transactions/messages | 44.9 ms |
| Internal Trace objects, event tree and initial blocks | 27.2 ms |
| All 13 eligible matcher passes | 41.6 ms |
| Building the vector of output Action objects | 37.6 ms |
| MsgPack output, routes and account/action index entries | 7.0 ms |
| Remaining work, including row selection/IDs, postprocessing and temporary-object destruction | 28.1 ms |

The matcher total is highly uneven: **auction_bid takes 35.6 ms; the other twelve
passes together take 6.0 ms**. Building the event tree itself takes about 3.7 ms.
Thus a plain graph traversal is not what accounts for the whole 180–200 ms.

The initial detailed profile counted 65,013 `try_build` calls per classification:
13 passes over 5000 nodes plus the wrapper. 4999 attempts reach `run_two_phase`.
These counts do not depend on the timer overhead. The plain TON transfers pass
the cheap `auction_bid_candidate` guards, so `auction_bid_data` queries
`nft_auction` and `nft_item`. Both return null for this fixture. No database I/O
occurs, but constructing lookup keys/tables, running collection/resolution/final
passes and constructing temporary Values still costs CPU. Most other protocol
matchers are excluded by the opcode/btype inventory before a traversal starts.

Observed existing `classify_us` means with coarse profiling:

| Snapshot nodes | Mean call duration |
| ---: | ---: |
| 1000 | 27.8 ms |
| 2500 | 78.0 ms |
| 5000 | 181.0 ms |

These three points do not prove a general asymptotic bound for arbitrary matcher
patterns. They do not show an obvious quadratic explosion within one call on this
fixture. Conversion and many passes are linear; sorting/indexing and allocation
add costs. Repeated full classification of a growing trace is a separate issue:
for final size N and k new transactions/update, even one linear pass per update
visits approximately N²/(2k) nodes over the trace lifetime. Confirmed plus finalized
repeats that work again. At N=5000, k=5, that is about 2.5 million node visits for
finalized updates of one trace alone.

## A concrete allocation cost

In this build `sizeof(Value) == 104` and `sizeof(Action) == 5520` bytes. Action
contains 51 Value fields, most empty for a TON transfer. About 5000 Action objects
therefore occupy roughly 27.6 MB before separately allocated strings/containers.
The output vector is populated with `push_back` without reserving the known row
count. Its growth relocates large Action objects; their move constructor is
noexcept, so these are moves rather than an assumed deep-copy fallback.

A temporary variant adding only
`rows.reserve(rows.size() + core.size())` before row construction reduced that
phase from **34.4 ms to 14.5 ms** in a paired 30-repetition comparison. The whole
existing call metric averaged 171.4 ms without it and 158.7 ms with it. Host/run
variation affects totals; this is not a measured end-to-end throughput gain.
All results retained state `ok` and 4999 actions. The production implementation
was not changed as part of this investigation; broader correctness checks would
belong to implementing an optimization.

The expensive work is thus repeated data conversion, allocation and selected
per-node rules, rather than merely visiting 5000 graph vertices. There are
implementation-level optimization candidates before a full incremental matcher
redesign: output allocation, immutable parsed-data reuse and lookup-path overhead.

## Sources and artifacts

- `src/emu/EmuClassifierActor.cpp`: whole-call metric and pipeline boundaries.
- `src/emu/EmuTraceConvert.cpp`: deserialize/parse every transaction BOC.
- `src/ClassifyCore.cpp`: per-matcher traversal, schema adaptation, output loop.
- `src/host/HostNftSale.cpp`: auction candidate guards and interface lookups.
- `src/ActionBuild.h`, `src/Value.h`: output object representation.

Paths above are relative to `ton-mch-engine`. On the measurement host:
`/tmp/ton-mch-stage-profile/` contains the first detailed profile, uninstrumented
control and probe source; `/tmp/ton-mch-stage-thin/` contains the coarse profile,
build scripts/commands, size probe, reserve-only source variant, CSV timings and
JSON run summaries. Each profile's `build.py` compiles copies under `/tmp` and
links the same production libraries. No production C++ source was edited.
