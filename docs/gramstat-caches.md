# Inter-block parsing caches

The indexer's shared scanner library keeps bounded caches for methods, elections,
and parent accounts/cells.
They work across parallel MCs and across the ClickHouse/PostgreSQL pipelines.
The keys are content identities, not the most recently processed MC. Retries and
out-of-order blocks therefore cannot reuse a different state's value.

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

This subtree key is enabled only for the audited deployed Elector code
`Yhf4csmfr8uHDywRo2L1kzm+lQlfcNALnP8vbc1p090=`. The complete dispatcher/getter
closure reads only c4's first dictionary reference and that election's contents.
It does not read NOW, the address, config, randomness or libraries. The fixture
and audit description are in `tondb-scanner/test/fixtures/`.

Changes to credits, past elections or accumulated rewards do not invalidate
this value. New participants, increased stakes, changed ADNL/max-factor,
election scalar fields/flags, and removal of the election do change its key.
Reading the `cur_elect` reference hash does not walk its participants.

**Unknown code bypasses the cache.** Merely including a new code hash in a key
would not prove that a new getter is independent of time/config. Unreadable keys
also fall back to the original VM path rather than being cached as absence.

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
retries, and cached/uncached parity on the actual Elector bytecode. It also tests
in-round changes, unrelated storage/time changes, source MC rebinding, VM/code
changes, historical ordering and the missing-intermediate-state trigger counters.

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
