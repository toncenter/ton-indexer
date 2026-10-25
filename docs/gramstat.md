# Gramstat: total supply

Both `ton-index-clickhouse` and `ton-index-postgres` support `--gramstat`.
The shared `collect_gramstat` switch enables the Gramstat stage in the existing
`IndexScheduler` pipeline. Total supply is its first metric: one snapshot per
indexed masterchain block, stored in `gramstat.total_supply`.

## Calculation

`index-engine/src/TotalSupply.cpp` calls TON's
`calculate_global_balance_at_anchor()` using the MC post-state and its top shard
states from `ParsedBlock::mc_block_`. Auxiliary historical MC states needed for
`processed_upto`, and seqno-zero shard tips, are loaded through the same
`DbScanner` and secondary RootDb.

All amounts are exact integer nanoTON:

```text
total = accounts + dispatch_queue + unprocessed_messages + unrecovered_validator_fees
```

The native method excludes already-processed messages from outgoing queue
balances. Its diagnostic `burned` field is not subtracted again. Missing inputs
or calculation failures cause a retry rather than a partial or zero snapshot.

## Storage

`gramstat` is a database in ClickHouse and a schema in PostgreSQL. Each belongs
to one network and its ordinary `blocks` dataset. Block hashes remain in `blocks`.

| Column | ClickHouse type | PostgreSQL type | Meaning |
| --- | --- | --- | --- |
| `mc_seqno` | `UInt32` | `bigint` | Masterchain block sequence number |
| `gen_utime` | `DateTime('UTC')` | `bigint` | Block generation time; Unix seconds in PostgreSQL |
| `total` | `Decimal(38,0)` | `numeric(38,0)` | Total supply |
| `accounts` | `Decimal(38,0)` | `numeric(38,0)` | Account balances |
| `dispatch_queue` | `Decimal(38,0)` | `numeric(38,0)` | Funds in the dispatch queue |
| `unprocessed_messages` | `Decimal(38,0)` | `numeric(38,0)` | Funds in unprocessed outgoing messages |
| `unrecovered_validator_fees` | `Decimal(38,0)` | `numeric(38,0)` | Unrecovered validator fees |

**ClickHouse:** `ReplacingMergeTree`, monthly `toYYYYMM(gen_utime)` partitions,
`ORDER BY (gen_utime, mc_seqno)`. Identical retries converge to one logical row;
use `FINAL` for deduplicated raw listings/counts before background merges.
The chart queries below tolerate identical duplicates without `FINAL`.

**PostgreSQL:** primary key on `mc_seqno`, index on `(gen_utime, mc_seqno)`.
Snapshots use batched `INSERT ... ON CONFLICT DO NOTHING` and commit in the same
transaction as blocks, including when account-state writes go to Kvrocks.

## Running and backfill

Add `--gramstat` to the normal indexer command with its `--ch` or `--pg`
connection. The flag creates the optional Gramstat database/schema and table;
the configured role needs the corresponding permissions. PostgreSQL still
requires the ordinary indexer migrations. Configure the flag consistently on
PostgreSQL leader and standby workers.

Use `--gramstat --from N --to M` for historical backfill; both bounds are
inclusive. Bounded replay skips an MC only when both its block and supply row
exist. ClickHouse normal resume also checks both tables because their inserts
are not atomic. PostgreSQL normal resume uses `_ton_indexer_progress`; new
blocks and supply are committed atomically, while pre-existing history needs
an explicit bounded backfill. Standbys require both rows before acknowledging
a leader's batch.

## Hourly and daily charts

ClickHouse:

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

PostgreSQL:

```sql
SELECT DISTINCT ON (bucket)
    date_trunc('hour', to_timestamp(gen_utime), 'UTC') AS bucket,
    total
FROM gramstat.total_supply
WHERE gen_utime >= 1788220800  -- 2026-09-01 00:00:00 UTC
  AND gen_utime <  1790812800  -- 2026-10-01 00:00:00 UTC
ORDER BY bucket, gen_utime DESC, mc_seqno DESC;
```

For daily points, use `INTERVAL 1 DAY` or `'day'`. Each point is the last available
snapshot in its UTC interval, with `mc_seqno` breaking equal-second ties. Supply
snapshots are not summed across time. Current or incompletely indexed intervals
are provisional; missing intervals remain absent. Materialized aggregates can
be added and backfilled later without changing the indexer.

## Checks

```sh
cmake --build build --target ton-index-clickhouse ton-index-postgres test-gramstat test-gramstat-postgres -j 4
ctest --test-dir build -R '^gramstat_' --output-on-failure
```

Tests cover exact monetary conversion, missing inputs, duplicate replays,
backfill selection and chart queries. With the respective database tools
installed, they use isolated `clickhouse local` storage and a temporary
Unix-socket-only PostgreSQL cluster to verify commit/rollback behavior.
