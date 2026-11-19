# Inter-block parsing caches

The indexer's shared scanner library keeps bounded caches for methods, elections,
and parent accounts/cells.
They work across parallel MCs and across the ClickHouse/PostgreSQL pipelines.
The keys are content identities, not the most recently processed MC. Retries and
out-of-order blocks therefore cannot reuse a different state's value.

## CellDB RocksDB block cache

Both indexer binaries accept `--celldb-cache-size <bytes>` to set the shared
scanner's **CellDB-only** block-cache budget, for example
`--celldb-cache-size 1073741824` for 1 GiB. This applies independently of
`--gramstat`. Omission or `0` preserves upstream defaults; it does not disable
caching. Values are unsigned decimal bytes (suffixes such as `1G` are rejected).

With the pinned TON/RocksDB revision and the scanner's default V1 CellDB options,
the implicit cache is **32 MiB AutoHyperClockCache**. A positive explicit budget
uses TON's `RocksDb::create_cache`, currently **LRU**. Therefore a default-versus-
explicit comparison changes both capacity and policy; use an explicit 32-MiB
control when measuring the effect of capacity. Existing partitioned SST filters
and indexes can use the block cache even when top-level metadata caching is off.

This is separate from the parsed-account/parent caches below, ZFS ARC, archive
database caches and allocator overhead, and is not a process RSS limit. For
large historical reads, inspect `<--working-dir>/secondary_logs/celldb_stats.txt`
(interval filter/index/data hits, misses and bytes inserted), physical-device IO,
and memory pressure. Compare identical MC ranges and logical outputs; a change
in transactions per MC makes raw MC/s comparisons misleading.

A bounded read-only mainnet comparison on 2026-11-08 used MC
**37300000..37300511**, a 128-MC warmup, four workers/four in-flight MCs, and
default/32-MiB-LRU/1-GiB-LRU arms repeated in reverse order. Fetching, parsing,
getters, account deltas and deployments processed **862 blocks / 46,962 tx** per
arm. The two-run aggregate rates were **7.22 / 5.93 / 10.45 MC/s**, respectively.
The 1-GiB cache reduced read syscall count by **59.8%** and userspace read volume
by **57.2%** versus default; all six content digests agreed (block IDs/metrics,
tx/state hashes, balances, interface kinds/methods, deployment IDs and deltas).
This probe excludes sink/trace/action processing and cross-MC parent caching,
does not compare every getter result field, and shares the host with production.
Its **1.45x** speedup is not a measurement of full-indexer throughput.

## Account states from block Merkle updates

The shared parser first reads account roots and their new descendants from the
block's existing `state_update`. `StateUpdateReader` reconstructs the new side
using the pinned node's Merkle-depth rules, validates the update, and checks its
root hash/depth against the persisted post-state. Old pruned subtrees become lazy
references into the same historical CellDB reader. Genuine exotic cells inside
code/data retain their Merkle semantics. Roots absent from the update use CellDB.
This applies to both ClickHouse and PostgreSQL, independently of `--gramstat`.

The temporary index is **per shard block**, is skipped when there are no changed
accounts, and is released immediately after parsing those accounts. It is not
stored in `ParsedBlock` or the insert queue. Only the extracted account code/data
graphs survive into the detector pipeline; previous-state/parent lookups retain
the original CellDB reader. No queue or dispatch dictionary is read from CellDB
to construct the overlay: only the cells already present in the block are walked.

Construction admits at most **65,536 new cells**, **262,145 traversal visits** and
**1,024 recursive edges** per shard block. An oversized, invalid or unsupported
update falls back to the ordinary persisted-state reader, without publishing a
partial index or dropping accounts. These are transient node/work bounds, not
byte-accurate RSS limits. There is no shared mutable overlay across blocks.

Statistics: `indexer.state_update.hit|miss` count account-root reads served from
the overlay or delegated to CellDB; `cells` counts admitted new cells;
`oversized|error` count bypassed updates. `build.micros` measures construction
elapsed time, including failed attempts. Lazy old-descendant reads are not
included in the root hit/miss counters.

A read-only prototype A/B/B/A on 2026-11-08 tested three 128-MC windows beginning
at **40,670,000 / 40,700,000 / 40,730,000**, with 32-MC warmups, four workers and
the same 1-GiB CellDB cache. The windows contain **387,323 transactions**;
**99.42%** of their 275,352 account roots were available in the updates. Mean
baseline/candidate seconds were **78.14/25.29**, **57.17/21.53**, **124.59/36.72**:
**3.11x** aggregate speedup, **76.84% fewer read calls**, **67.11% fewer returned
read bytes**, and **59.69% less CPU**. Reconstruction is included in timing.
All output digests matched, including every interface field; a separate 12-MC
check compared **215,107 reconstructed cell records** with CellDB. This prototype
kept a per-MC index until getters completed; production narrows its lifetime to
per-block account parsing. The figures exclude sinks/trace/action processing and
do not establish full-indexer throughput at production concurrency.

CTest `gramstat_state_update` covers lazy fallback and error propagation, index
lifetime, missing/mismatched/malformed inputs, size limits, mixed bulk reads,
unchanged roots, shared subgraphs, library references and nested Merkle proofs.

### Full-writer verification status (2026-11-08)

The first complete ClickHouse canary at **40730000..40730127** wrote **197,670
transactions / 2,290 blocks** with both executables. Its literal, all-column
`FINAL` comparison **failed**; the read-only digest match above must not be
reported as equality of all stored tables. The scoped reader-regression checks
below subsequently passed with the explicitly identified existing defects.

The initial differences are:

- 128 MC `blocks` rows differ only in the order of `shards`; sorted tuples agree.
  The scanner collects shard results in asynchronous completion order.
- 763 transaction rows differ only in `account_state_timestamp` and/or
  `account_state_last_trans_lt`. All have the same `account_none` post-state hash.
  The existing writer joins account metadata by hash across an entire insert
  batch, although `account_none` has the same hash for different addresses.
  An independent check against each account's final transaction and its shard
  block already finds **695 incorrect metadata rows in the old executable**
  (802 in the candidate); every incorrect pair matches other accounts instead.
  Faster processing changes timed batching, so this bug can change stored values
  without any change to parsed account cells.
- One `nft_items` and one `nft_transfers` `FINAL` row select different identities
  under equal replacement keys/versions. Existing keys omit the item address and
  transaction hash respectively; different NFTs/events can collide.

All remaining tables, including account deltas, deployments and
`latest_account_states` (with code/data BoCs), matched in this range. Several
validator tables were empty, so their equality does not establish coverage.
Repeating the **unchanged old executable** reproduced differences in these same
four tables: 128 reordered shard lists and 660 shared-`account_none` metadata
rows, plus the two NFT replacement choices. The early **3400000..3400015** range
matched except for 16 shard-list permutations (419 blocks / 80 transactions).
At **103190374..103190389**, across v17 activation, **every FINAL table matched**
(34 blocks / 416 transactions, including 2 validator cycles and 756 members).

Disabling background merges alone does not preserve incoming NFT rows:
ClickHouse's default `optimize_on_insert=1` can replace equal keys inside each
incoming part. The regression check therefore captures native-writer INSERT
rows through materialized views into plain `MergeTree`, before source-engine
replacement. NFT events must match with every field and multiplicity; latest
NFT rows must match by their actual address with every field and version.

The operator selected a **reader-only release**, leaving these existing writer
and schema defects as a separate task. A valid reader-regression result is not
literal equality of all FINAL tables and does not certify or repair those old
defects. Shard permutations and only the two metadata fields on the known
`account_none` hash are explicit exceptions, backed by the unchanged-binary A/A;
other stored fields, including monetary values and NULLs, remain exact checks.
The original failed comparison is retained separately from subsequent evidence.

The pre-engine capture completed with **all 1,960 NFT item rows and all 439 NFT
transfer rows exactly equal**, including every field and multiplicity. Latest
full rows for all **1,266 actual NFT addresses** also matched without version
ties. The legacy replacement keys reduced those to 621 items / 436 transfers;
every originally differing FINAL row was present in both input captures.

A longer **40730000..40734095** canary ran the complete candidate with **96 CPU
workers, 4 IO workers, 128 active MC tasks and 20 insert actors**, writing all
**4,096 MC blocks/deltas and 5,495,143 transactions**. It exited successfully with
no pipeline failures; sampled reader counts were **3,806,140 hits / 20,736 misses**,
zero oversized/error bypasses. Peak RSS was **15.53 GiB**, with no memory-high,
memory-max or OOM events. The 192.45-second wall duration includes startup and
runs alongside production; it is a bounded concurrency/memory check, not a
long-term memory plateau or a production-throughput speedup measurement.

These specific legacy ClickHouse defects do not feed the current Gramstat chain
metrics (blocks/account deltas), the actions aggregator's NFT transfer counts
(PostgreSQL `public.actions`), or the separately collected NFT ownership/existence
history (`nft_item_states`, keyed by actual account and MC). That history matched
in the canaries. Direct counts from legacy CH `nft_items`/`nft_transfers` can still
undercount because replacement loses distinct identities; they are not an
interchangeable source for those statistics.

## Contract methods

`parse_contract_methods` caches successful method lists by the **original account
code hash**, including library-reference cells. All three users share this cache:
`BlockInterfacesDetector`, `AccountStateHistory`, and token getter error
classification in `Tokens.cpp`.

On a miss, the parser resolves library references in the supplied historical
config and parses the method dictionary. On a hit it does not resolve the
library again. This is **code metadata**, not proof that the code was executable
in that config: once known, methods remain known even if an earlier/later config
lacks the library. The uncached reference function still requires resolution.
The database continues to use the original code hash as well.

Only complete successful results are retained, including an empty list. Missing
libraries, read failures, malformed dictionaries, and unsupported dispatchers
are not retained. In particular, contracts embedding a changing counter in code
without the expected SETCP0/DICTPUSHCONST dispatcher do not fill the cache with
negative entries. Errors never replace a previous successful database row.

The limit is **128 MiB of accounted retained memory**, with no separate entry
count cap. Accounting includes vector capacities, keys, list/hash nodes,
shared-value/control-block and allocator allowances, plus the hash table's
actual bucket count (also after evictions). Empty vectors therefore consume
budget too. This is not a process RSS limit; active callers can temporarily
hold values after eviction, and allocator bookkeeping/fragmentation varies.

## Elector elections

`elector::parse_election` caches the **parsed** `participant_list_extended`
result, including the successful absence of an active election. Its key is:

```
Elector code hash + TVM global version + optional cur_elect cell hash
```

This subtree key is enabled only for an **exact selector and getter closure**:
an ordinary 80-bit/one-reference code cell containing SETCP0 / 19 DICTPUSHCONST /
DICTIGETJMPZ / THROWARG 11, with no extra instructions or continuation; method
86698's entire cell graph must hash to
`4YQSc9XqovH2ggyt2+7Gd6h6ecwftLK8/2vXIdL/RHA=`.
This matches the original audited deployed code
`Yhf4csmfr8uHDywRo2L1kzm+lQlfcNALnP8vbc1p090=` and historical versions
`ru0Xh4xjMGnjqPUkty8uJC1IoKWFCo0fbEUI/o/EgVc=` (MC 3400000) and
`5IiS+ovkOVSikj1mj/no1okxyC2NyAvhyISLiuj+Nmo=` (MC 3422397), whose other methods differ.
The complete dispatcher/getter
closure reads only c4's first dictionary reference and that election's contents.
It does not read NOW, the address, config, randomness or libraries. The fixture
and audit description are in `tondb-scanner/test/fixtures/`.

Changes to credits, past elections or accumulated rewards do not invalidate
this value. New participants, increased stakes, changed ADNL/max-factor,
election scalar fields/flags, and removal of the election do change its key.
Reading the `cur_elect` reference hash does not walk its participants.

**Other selectors/getter closures bypass the cache.** Merely including a new code
hash in a key would not prove independence of time/config. Library-reference code
also bypasses it: config can remove a library without changing the reference hash.
Unreadable keys fall back to the original VM path rather than being cached as absence.
Admission decisions are cached by full code hash in a separate **64-entry/64-KiB**
metadata cache (`indexer.cache.elector_code`); inspection errors are not cached.

The cache retains at most **16 states and 8 MiB**, counting participant vector
capacity, strings, integer allocations and container overhead. It stores no
TVM stack, lazy cell graph or CellDB reader. Each caller receives a mutable copy
with its own `source_mc_seqno` on both the election and every participant.
Existing snapshot triggers and database observations are preserved.

## Concurrency and statistics

Both caches coalesce simultaneous requests for the same key. The first caller
computes synchronously outside the cache lock; followers wait on that key's
condition variable, releasing the lock. Other keys proceed independently.
These loaders perform synchronous VM/cell work and do not await actor messages.
Success and error paths both release followers and remove the in-flight entry;
future calls can retry an error. In-flight entries are transient and bounded by
the number of synchronous callers, separately from retained cache values.

`<--working-dir>/stats.txt` is atomically replaced about every 60 seconds. Cache
prefixes are `indexer.cache.contract_methods` and `indexer.cache.elector_election`:

- `hit`, `miss` (leader), `coalesced` (follower), `eviction`, `compute`, `error`,
  `bypass`, `oversized`: `COUNT` over the reporting interval.
- `compute.micros`, `wait.micros`: interval `SUM` of elapsed time, including cell
  reads/waits, not sampled CPU time. The compute timer excludes cache lookup and
  per-caller copying. Follower wait time helps detect scheduler-worker occupancy.
- `entries`, `bytes`, `inflight`: current `GAUGE` values.

Elector diagnostic counters in the same report:

- `indexer.elector.compare.error|changed|unchanged`: transaction comparisons.
  An error still conservatively treats the transaction's data as changed.
- `indexer.elector.election.trigger`: MC processing attempts marking the active
  election dirty, including attempts without an available Elector account.
- `indexer.elector.participant_getter`: actual getter attempts, including empty
  elections, failures and unknown-code bypasses; cache hits do not increment it.

These are attempts, not deduplicated MC counts. Preserve report snapshots when
measuring a whole run; the file itself keeps only the latest interval. Per-key
logs/high-cardinality metric labels are not required.

## Verification

`test-memoized-parsers` / CTest `gramstat_caches` exercises LRU byte/entry bounds,
empty results, unsupported code churn, concurrent single-flight success and
failure, unrelated-key progress, library disappearance/appearance, read-error
retries, and cached/uncached parity on both actual Elector bytecodes. It also tests
in-round changes, unrelated storage/time changes, source MC rebinding, VM/code
changes, historical ordering and the missing-intermediate-state trigger counters.
The test asserts both full-code hashes and the identical getter closure, then
runs the complete election-cache suite on each fixture, including 380-member
concurrent requests, input perturbations and read-failure retries.
Admission tests also change an unrelated method (cache still applies), replace
the getter with a NOW-dependent body under the exact selector (bypass), prepend
selector instructions (bypass), and remove a code library after a successful call
(the next call fails normally instead of reusing stale success).

A read-only NodeDB probe on 2026-11-07 verified exact cached/uncached field parity
at mainnet MC **3400000, 3407832, 3422397, 3437018, 10000000 and 100334250**,
covering all three observed code hashes, TVM versions 2/15, absent elections and
112/113/114/376 participants. Each snapshot computed once and then hit the cache
with a changed timestamp/source MC. This is a targeted correctness check, not a
full-history data audit or a throughput benchmark.

For an end-to-end performance comparison, use the same MC range, resources and
writer configuration, with fresh destination databases or explicit forced replay.
Compare logical database results as well as throughput, read volume, cache counts
and a new profile. The previous CPU profile alone does not measure the new speedup.

## Detector routing and shared parent lookups

The method cache also records whether it recognized an **exhaustive dispatcher**:
SETCP0 / DICTPUSHCONST / DICTIGETJMP[Z] / unconditional THROW 11 or THROWARG 11,
with no trailing bits or references. A parsed dictionary with a different tail
remains useful metadata but cannot prove that other methods are absent.
The legacy non-Z dispatcher discards a missing key, so its THROWARG can exit 2
on an empty getter stack; an absent method still cannot return successfully.

`InterfacesDetector` consults this shared metadata before creating detector
actors. For an exhaustive dispatcher it skips probes whose primary getter is
absent. The auction probe allows either `get_auction_data_v4` or `get_sale_data`;
optional getters do not disqualify a candidate. Vesting, NominatorPool and DeDust
retain their own cheap exact-code checks, including direct storage parsing.
Unknown selectors and inspection failures run the full detector set.

A getter being present is only a candidate filter. The selected detector still
executes it against the current address/data/config/time, validates its return
types and verifies its parent contract. An exported getter throwing exit code 11
remains a failed/unknown probe. Library metadata retains the semantics above:
known missing methods can be excluded, while a declared method whose library is
currently unavailable is attempted and remains unknown.

Each `BlockInterfaceProcessor` also creates one `AccountLookupContext`, shared by
all of that MC's jetton-wallet, NFT-item, multisig-order and DeDust detectors.
Successful `lookup_account` results are cached by full workchain/address, so
multiple children referencing the same master/collection perform one account
dictionary lookup and unpack. Parent getter executions remain per-child, with
their original arguments and current config.

The context owns immutable shard-state roots and is released when its detector
group finishes. It has an LRU cap of **1,024 accounts / 4 MiB of accounted decoded
headers and scalar values**. Code/data are lazy references into that block's
snapshot, not serialized account BOCs; this is not a bound on the complete
reachable cell graph or process RSS. Concurrent requests for the same account
coalesce; errors and absent accounts are not retained. Supplying a context with
different shard-root identities falls back to an uncached lookup instead of
returning an account from another snapshot.

Additional interval statistics in `stats.txt`:

- `indexer.detector.started|skipped`: number of detector actors started/avoided.
- `indexer.detector.filter_fallback`: account-level filter attempts falling back
  because method inspection failed or the selector was not exhaustive.
- `indexer.lookup_account.cache.hit|miss|coalesced|bypass`: lookup caller outcomes.
- `indexer.lookup_account.cache.eviction`: evictions, reported when each context
  is destroyed (at the end of its detector group).
- `indexer.lookup_account.error`: actual failed dictionary lookup/unpack attempts.
- `indexer.lookup_account.micros`: elapsed-time histogram of actual lookups,
  including bypasses and failures, excluding hits/coalesced followers.

`gramstat_native` compares filtered and full detector results, including a custom
selector that implements an NFT getter outside its dictionary. It also checks
that two wallets sharing a master in one MC execute only one parent lookup.
`gramstat_caches` covers lookup concurrency, transient read retries, context
release, byte/entry eviction and out-of-order/different snapshot identities.

## Inter-block parent states and warmed cells

The indexing scheduler owns a **512 MiB** parent-cache budget (about **20 MiB** was
observed in the 30-minute profile of the seven-day pilot indexer run), shared by
all its parallel MC detector groups. It updates cached parents immediately after the
existing ordered TraceAssembler stage. There is no new actor barrier or waiting
for another MC: incompatible snapshots use the existing per-MC lookup path.

Every unique transaction account is checked, including accounts whose final-state
read failed. Existing entries are updated from already parsed final account
states. The state must match the last transaction's LT and post-state hash;
missing/incomplete states, deletion, freezing and uninitialized states invalidate
the entry. Updates perform no CellDB reads and do not promote cache popularity.
Only a detector's demand lookup admits a new parent.

Each address retains one latest state and its `valid_from` MC. The cache also
tracks the continuously observed MC head and verifies predecessor block hashes.
Gaps or conflicting identities clear parent entries and start a new epoch;
older views cannot publish into that epoch. A detector can use an entry only
inside its confirmed validity interval, and holds its own immutable value while
other MCs update the cache. Old MCs requiring an overwritten version fall back.
The account timestamp is rebound to the requested shard snapshot, while getters
always use the current MC's time, config and libraries.

Cold lookups can finish after later MCs have already been observed. A fixed
128-MC journal of 8192-bit address Bloom filters checks all intervening changes
before admission. False positives only deny admission; outside the journal the
result is returned to its caller but not retained. This also prevents a stale
load from inserting a parent that was absent from the cache when it changed.
The journal is bounded and included in the 512 MiB budget.

### Memory and lazy loading

The persistent cache stores detached account headers/root identities and **flat
immutable cell images by content hash**. Images contain the cell's bits and child
hash/depth identities, never recursive `Cell` references or CellDB readers.
Consequently a later getter cannot grow an unaccounted retained graph. Each newly
read cell is a separately charged entry, including its child identities and cache
container overhead; shared cell content is stored once in this cache.

After fixed overhead and the journal, 1/16 of the budget is for account metadata
(approximately 32 MiB at the default), and 15/16 for warmed cells (approximately
480 MiB). Both reuse `MemoCache` with segmented LRU: first-use entries enter the
probation segment; repeated reads promote them to a protected segment capped at
80% of that cache's byte budget. Updates preserve the segment/recency. A scan of
one-off parents/cells therefore preferentially evicts probation entries.

Each caller receives lazy cell references using **its own** CellDB reader.
Reading a reference gets the image from the shared cell cache, or loads exactly
that cell on a miss, and reconstructs a VM cell with lazy child references. The
caller's materialized graph reuses loaded cells during execution, as ordinary
ExtCells do. Unvisited subtrees are never prefetched. Library-reference code
remains a reference and resolves against the caller's current libraries.

The budget covers retained cache data, not process RSS: active detector groups
still hold their ordinary per-MC snapshots, materialized VM cells and in-flight
loads. Neither detached parent entries nor warmed cell images pin an old reader.

Metrics are appended to the scheduler's minute report:

- `indexer.cache.parents.*`: hits/misses/evictions and retained metadata bytes;
- `.update`, `.invalidate`, `.reset`, `.version_bypass`, `.stale_publish`:
  interval counts; `.verified_mc`: current confirmed head;
- `.total_bytes`: retained metadata, cell images, journal and fixed overhead;
- `indexer.cache.parent_cells.*`: warmed-cell hits/misses/coalescing, read errors,
  evictions and bytes (the generic cache's count, excluding its fixed object).

`gramstat_caches` covers updates, old/future MCs, gaps/forks/retries, late-load
publication, missing latest states, SLRU scan resistance, byte eviction, lazy
subtree loading, reader release and NOW/library changes. `gramstat_native` runs
two jetton wallets against one parent over three MCs and checks equivalent
detector results, one parent lookup, and no repeated CellDB reads after warmup.
