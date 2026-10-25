# Gramstat: total supply

`ton-index-clickhouse --gramstat` calculates one total-supply snapshot per
indexed masterchain block and writes `gramstat.total_supply` on the same
ClickHouse server as the ordinary indexer tables. This is an opt-in stage of
the existing `IndexScheduler` pipeline.

## Calculation

The adapter in `index-engine/src/TotalSupply.cpp` calls TON's
`ton::validator::calculate_global_balance_at_anchor()` (included in the TON
revision pinned by `v1.3`). Its inputs are the MC post-state/block root and
exactly the top shard states referenced by that MC, taken from the existing
`ParsedBlock::mc_block_`. Intermediate first-included shard blocks are not
additional snapshot states.

The native method returns:

```text
total = accounts + dispatch_queue + unprocessed_messages + unrecovered_validator_fees
```

All amounts are exact integer nanoTON. The method accounts for messages already
processed by another shard but still present in an outgoing queue. Historical
MC states needed to interpret `processed_upto`, and seqno-zero shard tips omitted
by the normal scanner, are loaded through `DbScanner::fetch_state_by_id` on the
same secondary RootDb. This state-only lookup reuses the node's state/cell cache.
Auxiliary MC states are also deduplicated within each native calculation.

The native `burned` field is diagnostic and is not subtracted again from `total`.
Missing states, malformed inputs and failed calculations cause the MC to retry;
they never produce a zero or partial supply row. The calculation has a
`total_supply` scheduler span. Measure its cost on sequential blocks: the
stateless native API may walk message queues on each snapshot.

## Storage

| Column | Type | Meaning |
| --- | --- | --- |
| `mc_seqno` | `UInt32` | Finalized MC snapshot identifier |
| `gen_utime` | `DateTime('UTC')` | MC state/block generation time |
| `total` | `Decimal(38, 0)` | Total supply, nanoTON |
| `accounts` | `Decimal(38, 0)` | Account balances |
| `dispatch_queue` | `Decimal(38, 0)` | Funds in the dispatch queue |
| `unprocessed_messages` | `Decimal(38, 0)` | Funds in unprocessed outgoing messages |
| `unrecovered_validator_fees` | `Decimal(38, 0)` | Unrecovered validator fees |

The table uses `ReplacingMergeTree`, monthly `toYYYYMM(gen_utime)` partitions,
and `ORDER BY (gen_utime, mc_seqno)`. Identical retries converge to one logical
row. Until merges run, physical duplicates can exist; use `FINAL` for a
deduplicated raw listing/count. The `argMax` chart queries below are unaffected
by identical duplicates and do not require `FINAL`.

One `gramstat` database belongs to one network and its canonical `blocks`
dataset. Hashes remain in the ordinary `blocks` table; join on `seqno` with
`workchain = -1` when they are needed. Time is stored locally to support
time-range scans without that join.

## Insertion and restart

Supply is calculated before the MC is queued for insertion. The insert actor
writes ordinary tables, including `blocks`, then `gramstat.total_supply`, and
only then acknowledges the batch. ClickHouse inserts across tables are not
atomic: while a batch is in flight, a block can briefly exist without supply.

With `--gramstat`, both bounded-replay skip detection and normal resume require
the MC in **both** tables. A crash after inserting `blocks` but before inserting
supply therefore retries the missing snapshot. Retrying may also rewrite the
ordinary `ReplacingMergeTree` rows. Errors from the supply insert fail the batch.

For adding historical supply to an already indexed range, run with `--gramstat
--from N --to M`; both bounds are inclusive. An existing `blocks` row alone does
not skip a missing supply calculation. Use the same range and flag on restart.

## Build and run

Build with the repository's normal CMake configuration and pinned submodules:

```sh
cmake --build build --target ton-index-clickhouse test-gramstat -j 4
ctest --test-dir build -R '^gramstat_' --output-on-failure
```

`gramstat_native` tests exact monetary conversion, the decimal limits, missing
input handling and ClickHouse column types. If `clickhouse` and Python are
available when configuring CMake, `gramstat_sql` also tests the production DDL,
hourly/daily points, equal-second ordering, duplicate replays and restart repair
using a fresh, isolated `clickhouse local` directory.

Example bounded run against a test ClickHouse dataset:

```sh
build/ton-index-worker/ton-index-clickhouse/ton-index-clickhouse \
  --db /path/to/node-db \
  --working-dir /path/to/gramstat-work \
  --ch clickhouse://127.0.0.1:9000/indexer_test \
  --gramstat --from "$FROM_SEQNO" --to "$TO_SEQNO" \
  --max-active-tasks 2 --threads 4
```

The configured ClickHouse user needs permission to create/use `gramstat` and
insert/select its table, in addition to the ordinary indexer permissions.

## Hourly and daily charts

```sql
SELECT
    toStartOfInterval(gen_utime, INTERVAL 1 HOUR, 'UTC') AS bucket,
    argMax(total, tuple(gen_utime, mc_seqno)) AS total
FROM gramstat.total_supply
WHERE gen_utime >= toDateTime('2026-09-01 00:00:00', 'UTC')
  AND gen_utime <  toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY bucket
ORDER BY bucket;
```

Use `INTERVAL 1 DAY` for daily points. To retrieve a consistent set of all
components, use `argMax(tuple(total, accounts, dispatch_queue,
unprocessed_messages, unrecovered_validator_fees), tuple(gen_utime, mc_seqno))`.

The bucket labels its start; the value is the last available snapshot inside
it. Supply is a stock: do not sum snapshots across time. The current bucket is
provisional. `argMax` alone does not establish continuous indexing coverage;
closed buckets are final only once indexing has completed their range. Missing
buckets stay absent rather than becoming zeros.

Materialized hourly/daily aggregates can be added and backfilled from this
table later, without changes to the indexer or the raw snapshot format.
