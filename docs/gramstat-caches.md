# Inter-block parsing caches

The indexer's shared scanner library keeps two inter-block, bounded LRU caches.
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
SETCP0 / DICTPUSHCONST / DICTIGETJMPZ / unconditional THROW 11 or THROWARG 11,
with no trailing bits or references. A parsed dictionary with a different tail
remains useful metadata but cannot prove that other methods are absent.

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
