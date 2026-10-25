`--gramstat` enables Gramstat in `ton-index-clickhouse` and `ton-index-postgres` (`collect_gramstat` internally). The shared `IndexScheduler` calls TON's `calculate_global_balance_at_anchor()` on the loaded MC and shard states. Each MC produces one snapshot in `gramstat.total_supply`. Amounts are exact integer nanoTON; `total` is the sum of the four components.

| Column | ClickHouse type | PostgreSQL type | Meaning |
| --- | --- | --- | --- |
| `mc_seqno` | `UInt32` | `bigint` | Masterchain block sequence number |
| `gen_utime` | `DateTime('UTC')` | `bigint` | Block generation time; Unix seconds in PostgreSQL |
| `total` | `Decimal(38,0)` | `numeric(38,0)` | Total supply |
| `accounts` | `Decimal(38,0)` | `numeric(38,0)` | Account balances |
| `dispatch_queue` | `Decimal(38,0)` | `numeric(38,0)` | Funds in the dispatch queue |
| `unprocessed_messages` | `Decimal(38,0)` | `numeric(38,0)` | Funds in unprocessed outgoing messages |
| `unrecovered_validator_fees` | `Decimal(38,0)` | `numeric(38,0)` | Unrecovered validator fees |

In ClickHouse, `gramstat` is a database; the table uses monthly `ReplacingMergeTree` partitions ordered by `(gen_utime, mc_seqno)`. In PostgreSQL, `gramstat` is a schema; snapshots commit atomically with blocks, with `mc_seqno` as the primary key. Historical backfill uses `--from N --to M`. Hourly/daily charts select the last snapshot per UTC interval using `argMax` in ClickHouse or `DISTINCT ON` in PostgreSQL, breaking equal-second ties by `mc_seqno`.
