# Gramstat: total supply, accounts, deployments and block flows

Both `ton-index-clickhouse` and `ton-index-postgres` support `--gramstat`.
The shared `collect_gramstat` switch enables the Gramstat stage in the existing
`IndexScheduler` pipeline. It collects total supply in `gramstat.total_supply`
and account-status changes in `gramstat.account_deltas`, one row per indexed
masterchain block in each table. `gramstat.deployments` stores individual
successful deployment events for all contract types.

The separate [Go aggregator](gramstat-aggregate.md) refreshes daily jetton/NFT
senders, receivers and transfer counts from indexed PostgreSQL actions into ClickHouse
`gramstat.daily_metrics`. It runs hourly and supports independent future handlers.

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
connection. ClickHouse creates the optional Gramstat database and tables when
the flag is enabled. PostgreSQL migration **1.3.2** creates the Gramstat tables;
startup with the flag also ensures they exist using the same DDL. The configured
role needs the corresponding permissions. Configure the flag consistently on
PostgreSQL leader and standby workers.

Use `--gramstat --from N --to M` for historical backfill; both bounds are
inclusive. Bounded replay skips an MC only when its block, supply and account
deltas all exist. ClickHouse normal resume also checks all three tables because
their inserts are not atomic. PostgreSQL normal resume uses
`_ton_indexer_progress`; new blocks and metrics are committed atomically, while
pre-existing history needs an explicit bounded backfill. Standbys require all
three rows before acknowledging a leader's batch. For independent PostgreSQL
backfill use `--no-leader --pg-no-copy`. Missing metrics are repaired without
`--force`; use `--force` to recompute already-present account deltas.

**Deployments:** ClickHouse writes deployments synchronously **before account
deltas**, making deltas the completion marker even for MCs with zero deployments.
PostgreSQL commits all metrics together. There is no separate progress table.
History with deltas written by a pre-deployments binary requires an explicit
`--gramstat --from N --to M --force` backfill; missing deployments cannot be
detected from the absence of events. Upgrade all writers before relying on this
ordering. Retrying the same events is idempotent; if a future change removes
previously selected events, rebuild the affected data rather than relying on
upsert/`ReplacingMergeTree` to delete stale rows.

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

## Historical Accounts

Accounts counts existing accounts across all workchains, including MC:
`uninit + active + frozen`; `nonexist` is excluded. These are account statuses,
not distinct addresses ever seen or a count of user wallets.

`index-engine/src/AccountDeltas.cpp` reads the already-parsed transactions in
`ParsedBlock::blocks_`: the MC itself plus every shard block first included by
that MC. Current shard tips are not counted again. For each status `s`:

```text
delta_s(mc) = SUM([tx.end_status = s] - [tx.orig_status = s])
```

Every transaction participates, including aborted transactions. Multiple changes
of one account cancel naturally; their order does not affect the sum. The
calculator needs O(transactions) time, O(1) extra memory and no extra state reads.
Missing/mismatched MC input or unknown statuses fail the Gramstat stage and retry.

| Column | ClickHouse type | PostgreSQL type | Meaning |
| --- | --- | --- | --- |
| `mc_seqno` | `UInt32` | `bigint PRIMARY KEY` | Including MC sequence number |
| `gen_utime` | `DateTime('UTC')` | `bigint NOT NULL` | MC time, Unix seconds in PostgreSQL |
| `delta_uninit` | `Int64` | `bigint NOT NULL` | Net change of uninitialized accounts |
| `delta_active` | `Int64` | `bigint NOT NULL` | Net change of active accounts |
| `delta_frozen` | `Int64` | `bigint NOT NULL` | Net change of frozen accounts |

Zero deltas are stored too: a zero row means the MC was calculated; an absent
row means unknown. `total` is derived from the three components. Hashes remain
in `blocks`. Unlike block fees, the day is determined by the **including MC's
time**, not each shard block's own time.

ClickHouse uses monthly `ReplacingMergeTree` partitions and
`ORDER BY (gen_utime, mc_seqno)`, as for Supply. The canonical MC time is stable
across replays. **Use `FINAL` before any SUM** so unmerged retries do not inflate
counts. PostgreSQL uses an index on `(gen_utime, mc_seqno)` and batched
`ON CONFLICT (mc_seqno) DO UPDATE` replacing the time and three deltas, never
adding them to the old row. It commits with blocks and Supply, including in
Kvrocks mode.

### Daily Accounts queries

The accepted baseline is zero before MC 1. Absolute counts therefore require a
complete prefix from MC 1. A replay of a later interval alone gives changes over
that interval, not absolute Accounts. Initial zerostate accounts are neglected.

For a closed UTC interval `[from_day, to_day)`, choose `end_mc` as the last
canonical MC strictly before `to_day`. Verify the boundary against the indexed
chain, not just the maximum row currently available in `account_deltas`. The
queries below check that **every MC from 1 through end_mc is present** and return
no points if that prefix has a gap. A missing delta must not be filled with zero.
Current/incompletely indexed days remain provisional.

Daily deltas are summed first, then accumulated over the whole prefix; the
display-range filter is applied **after** accumulation. This preserves accounts
created before `from_day`. Each point is the count after the last MC before the
end of that UTC day. All equal-second MCs participate.

ClickHouse (typed parameters `end_mc`, `from_day`, `to_day`):

<!-- accounts-clickhouse -->
```sql
WITH prefix AS (
    SELECT * FROM gramstat.account_deltas FINAL
    WHERE mc_seqno BETWEEN 1 AND {end_mc:UInt32}
), daily AS (
    SELECT toDate(gen_utime, 'UTC') AS day,
           sum(delta_uninit) AS du, sum(delta_active) AS da, sum(delta_frozen) AS df
    FROM prefix GROUP BY day
), cumulative AS (
    SELECT day,
           sum(du) OVER w AS uninit,
           sum(da) OVER w AS active,
           sum(df) OVER w AS frozen
    FROM daily
    WINDOW w AS (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT day, uninit, active, frozen, uninit + active + frozen AS total
FROM cumulative
WHERE day >= {from_day:Date} AND day < {to_day:Date}
  AND {end_mc:UInt32} > 0
  AND (SELECT count() FROM prefix) = {end_mc:UInt32}
ORDER BY day;
```

PostgreSQL (bind `$1 = end_mc`, `$2 = from_day`, `$3 = to_day`):

<!-- accounts-postgres -->
```sql
WITH prefix AS (
    SELECT * FROM gramstat.account_deltas
    WHERE mc_seqno BETWEEN 1 AND $1
), daily AS (
    SELECT (to_timestamp(gen_utime) AT TIME ZONE 'UTC')::date AS day,
           sum(delta_uninit) AS du, sum(delta_active) AS da, sum(delta_frozen) AS df
    FROM prefix GROUP BY day
), cumulative AS (
    SELECT day,
           sum(du) OVER w AS uninit,
           sum(da) OVER w AS active,
           sum(df) OVER w AS frozen
    FROM daily
    WINDOW w AS (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT day, uninit, active, frozen, uninit + active + frozen AS total
FROM cumulative
WHERE day >= $2::date AND day < $3::date
  AND $1 > 0
  AND (SELECT count(*) FROM prefix) = $1
ORDER BY day;
```

These queries scan compact per-MC deltas instead of transaction history. A daily
cache can later reduce prefix-scan cost; rebuilding it must use deduplicated MC
rows as above. No running global counter is required in the indexer.

## Deployments and wallet charts

`index-engine/src/Deployments.cpp` collects transactions with
`orig_status IN ('nonexist', 'uninit')`, `end_status = 'active'`, and `aborted = false`.
Internal and external incoming messages both qualify. Repeated deployments are
separate events; unfreezes (`frozen -> active`) are excluded. The indexer does not
look up prior activations or filter wallet types.

| Column | ClickHouse | PostgreSQL | Meaning |
| --- | --- | --- | --- |
| `mc_seqno` | `UInt32` | `bigint` | Including MC |
| `gen_utime` | `DateTime('UTC')` | `bigint` | **Transaction** time (`tx.now`), not MC time |
| `account` | `String` | `tonaddr` | Raw address including workchain |
| `lt` | `UInt64` | `bigint` | Transaction LT |
| `tx_hash` | `FixedString(44)` | `tonhash` | Transaction hash, base64 |
| `init_code_hash` | `Nullable(FixedString(44))` | nullable `tonhash` | Code-cell hash from incoming StateInit, base64 |
| `out_msgs_count` | `UInt32` | `integer` | All actually emitted messages, including bounced |

StateInit is already available in `tx.in_msg`; collection needs no additional
CellDB/Kvrocks reads or emulation and handles intermediate transactions in a block.
It classifies the **deployment code**, not the post-code after possible `SETCODE`.
NULL code means a valid StateInit without a code reference. Missing/malformed
StateInit or an unsupported success descriptor fails the Gramstat stage instead
of silently dropping an event.

ClickHouse: monthly `ReplacingMergeTree`, `ORDER BY (account, lt, tx_hash)`;
canonical transaction time keeps retries in the same partition. Use `FINAL`
before counting raw events. PostgreSQL: PK `(account, lt, tx_hash)`, batched upsert,
indexes on `(init_code_hash, account, lt)`, `gen_utime`, and `mc_seqno`.

### First wallet activations per day

Pass the chosen wallet code-hash registry as `wallet_hashes` (base64 strings).
Select the first **qualifying wallet** deployment per address by LT, then apply
the display interval `[from, to)` (Unix seconds). Earlier non-wallet deployments
do not disqualify the address. Repeat wallet deployments do not create new first
activations; outgoing messages are not required. These queries need complete
deployment history before `to`, not just the displayed period. They do not
themselves verify coverage. Grouping by day returns days with qualifying events.

ClickHouse:

<!-- deployments-clickhouse -->
```sql
WITH first_wallet AS (
    SELECT account, argMin(gen_utime, tuple(lt, tx_hash)) AS first_time
    FROM gramstat.deployments FINAL
    WHERE gen_utime < toDateTime({to:UInt32}, 'UTC')
      AND init_code_hash IN {wallet_hashes:Array(String)}
    GROUP BY account
)
SELECT toDate(first_time, 'UTC') AS day, count() AS activations
FROM first_wallet
WHERE first_time >= toDateTime({from:UInt32}, 'UTC')
GROUP BY day ORDER BY day;
```

PostgreSQL (`$1 = wallet_hashes`, `$2 = from`, `$3 = to`):

<!-- deployments-postgres -->
```sql
WITH first_wallet AS (
    SELECT DISTINCT ON (account) account, gen_utime
    FROM gramstat.deployments
    WHERE gen_utime < $3 AND init_code_hash = ANY($1::tonhash[])
    ORDER BY account, lt, tx_hash
)
SELECT (to_timestamp(gen_utime) AT TIME ZONE 'UTC')::date AS day,
       count(*) AS activations
FROM first_wallet
WHERE gen_utime >= $2
GROUP BY day ORDER BY day;
```

### Active wallets: daily / rolling 30 days

An address qualifies once it has a wallet deployment at or before the sending
transaction's LT. It stays eligible after destruction/redeployment or `SETCODE`;
this is activity of **addresses previously deployed as wallets**, not a claim
about execution-time code. Historical sends from destroyed wallets remain counted.
Use actual non-bounced outgoing messages (also external-out); the incoming message
type is unrestricted. `deployments.out_msgs_count` covers only the deployment
transaction and is not a substitute for subsequent messages.

ClickHouse (`lt_from`/`lt_to` must cover all sending transactions in `[from, to)`;
derive conservative bounds from the indexed blocks):

<!-- active-wallets-clickhouse -->
```sql
WITH first_wallet AS (
    SELECT account, min(lt) AS first_lt
    FROM gramstat.deployments FINAL
    WHERE gen_utime < toDateTime({to:UInt32}, 'UTC')
      AND init_code_hash IN {wallet_hashes:Array(String)}
    GROUP BY account
), sends AS (
    SELECT tx_account AS account, toDate(toDateTime(tx_now, 'UTC'), 'UTC') AS day,
           max(tx_lt) AS last_send_lt
    FROM messages
    WHERE tx_lt >= {lt_from:UInt64} AND tx_lt < {lt_to:UInt64}
      AND tx_now >= {from:UInt32} AND tx_now < {to:UInt32}
      AND direction = 'out' AND source = tx_account
      AND (bounced = false OR isNull(bounced))
    GROUP BY account, day
), active AS (
    SELECT s.account, s.day
    FROM sends AS s INNER JOIN first_wallet AS w ON s.account = w.account
    WHERE s.last_send_lt >= w.first_lt
)
SELECT day, count() AS active_wallets
FROM active GROUP BY day ORDER BY day;
```

`max(tx_lt)` makes identical message retries harmless without `messages FINAL`
and reduces the JOIN to one row per address/day. For MAU, select 30 UTC days ending
at `to` and replace the final SELECT with
`SELECT uniqExact(account) AS active_wallets_30d FROM active`.
Do not sum DAU. This lifetime JOIN needs a separate performance measurement;
first-deployment and daily-activity materializations can be added later.

## Jetton wallet balance history (always collected)

Both indexers write `jetton_wallet_states` in the ordinary database/schema
(`public` in PostgreSQL), independently of `--gramstat`. PostgreSQL keeps this
history even when current states are stored in Kvrocks. Migration **1.3.2** creates
the PostgreSQL table and daily materialized view; an already-applied 1.3.2 needs
`--rerun-last-migration`. ClickHouse creates its table and refreshable view at startup
(requires refreshable-view support and an Atomic/Replicated database).

| Column | Meaning |
| --- | --- |
| `account` | Raw address, including workchain |
| `mc_seqno` | Including MC; key is `(account, mc_seqno)` |
| `gen_utime` | MC time: UTC `DateTime` in CH, Unix seconds in PG |
| `last_tx_lt` | Last transaction of this address included in the MC |
| `existing` | Nullable boolean: verified active jetton wallet / confirmed absence / unknown |
| `balance` | Exact nonnegative raw jetton amount; PG `numeric(78,0)`, CH canonical decimal string |
| `nonzero` | Generated/materialized nullable boolean: `existing AND balance > 0` |

There is at most one final observation per eligible address per MC, before writer-batch
deduplication of current states. CH uses monthly `ReplacingMergeTree ORDER BY
(account, mc_seqno)`; use `FINAL` before window calculations. PG upserts the same
historical key. Owner/master metadata comes from current `jetton_wallets` when
needed; this table has no owner/master fields or additional indexes/projections.

`existing=true` requires successful `get_wallet_data` and master round-trip
verification. Other active observations require `get_wallet_data` (method ID 97026)
in the **current** code's method dictionary: a master returning another address
produces false; getter/return-shape/master-lookup failures produce NULL. An exported
getter throwing exit code 11 is still unknown. Ordinary active contracts do not
produce false/NULL rows merely because the jetton detector rejected them.

Terminal observations require a boundary transition: the first transaction of the
address in the MC starts active and the last ends frozen, uninit or nonexist.
Only then the collector loads `first.account_state_hash_before` from CellDB and
reads its Account/StateInit headers and code. If that code exports `get_wallet_data`,
it records `existing=false`. The previous state's hash/address/status are checked;
neither its data dictionary nor intermediate transaction states are read. An
ordinary account leaving active is filtered out. Already-inactive accounts and
deploy+destroy within one MC produce no terminal observation. Active-to-active
code changes are not tracked and never trigger a previous-state read.

Method inspection resolves code-library references against the MC libraries and
caches results within that MC. An unsupported dispatcher or unavailable library
does not establish eligibility: unclassified addresses are skipped, not declared
absent. Successfully verified wallets do not require a standard method dispatcher.
Failed boundary-state loads or missing final active states fail collection for
retry, rather than manufacturing an observation for an unclassified address.
Getters use MC configuration time; the filter adds no get calls or emulation.

Balances remain NULL for both false and unknown observations; generated `nonzero`
is respectively false or NULL. Unknown eligible addresses are treated conservatively
in the global counts. Mintless unclaimed rights do not substitute for a positive
balance.

An observation with `existing=NULL` starts a gap until the next definite observation.
For example, balance 50 at MC 100, NULL at MC 110, and 70 at MC 120 means a gap on
`[110,120)`, not a carried-forward balance of 50. An explicitly absent wallet ends
the active balance series. Raw history queries must retain these NULL/false rows.

```sql
-- PostgreSQL; bind the raw address as $1. CH uses the same columns and FINAL.
SELECT mc_seqno, gen_utime, last_tx_lt, existing, balance, nonzero
FROM public.jetton_wallet_states
WHERE account = $1
ORDER BY mc_seqno;
```

### Daily counts

`jetton_wallet_counts_daily` returns `day`, `mc_seqno` (day-end anchor),
`existing_count`, `nonzero_count`, and `unknown_count`. It deduplicates history,
computes signed changes in known/unknown indicators per address, sums by UTC day,
then cumulatively sums those changes. Days with MC blocks but no state changes carry
the prior counts. An unknown interval remains unknown on unchanged days too;
both exact counts are NULL while the day-end `unknown_count` is nonzero. Resolving
an unknown observation restores counts; a corrected historical replay is reflected
by the next full refresh. Only days strictly before the latest indexed MC's UTC
day are published, not the partial current indexed day.

The initial count is zero: populate the complete required history from the zero
baseline before using global counts. The query does not establish ingestion
coverage. Old `blocks` rows are not proof that the new table was populated;
historical filling requires explicit `--from N --to M --force` replay. No separate
progress table or marker column is added. PG commits history with blocks; CH writes
history before blocks. Refresh after the relevant replay/ingestion has completed.
If history was populated by the older unfiltered collector, rebuild that affected
history before replay: upsert/`ReplacingMergeTree` cannot remove now-excluded rows.

PostgreSQL creates the MV `WITH NO DATA`; populate it once, then schedule refreshes
externally (for example daily):

```sql
REFRESH MATERIALIZED VIEW public.jetton_wallet_counts_daily;
-- Subsequent refreshes preserve concurrent readers; a unique day index is provided.
REFRESH MATERIALIZED VIEW CONCURRENTLY public.jetton_wallet_counts_daily;
```

ClickHouse uses `REFRESH EVERY 1 DAY`, without `APPEND`, and `EMPTY` to avoid a full
scan during initial DDL. Control historical filling/refresh explicitly:

```sql
SYSTEM STOP VIEW jetton_wallet_counts_daily;
-- After historical filling:
SYSTEM START VIEW jetton_wallet_counts_daily;
SYSTEM REFRESH VIEW jetton_wallet_counts_daily;
SYSTEM WAIT VIEW jetton_wallet_counts_daily;
```

Both mechanisms fully recalculate the daily result; they are not incremental
per-INSERT views. The source query lives next to each backend's DDL and is exercised
on the same controlled fixture in the existing SQL tests. Production indexing and
refresh cost remain to be measured after separately authorized historical filling.

## Block fees and issuance (always collected)

Fees, issuance and burning are native block aggregates, stored in the ordinary
`blocks` table by both indexers, independently of `--gramstat`. The parser reads
the dictionary roots and `ValueFlow` of the already-loaded block.

| Column | ClickHouse type | PostgreSQL type | Meaning (nanoTON) |
| --- | --- | --- | --- |
| `transaction_fees` | `Nullable(Decimal(38,0))` | `numeric(38,0)` nullable | `ShardAccountBlocks` transaction-fee aggregate |
| `import_fees` | `Nullable(Decimal(38,0))` | `numeric(38,0)` nullable | `InMsgDescr` collected delivery/transit fees |
| `created` | `Nullable(Decimal(38,0))` | `numeric(38,0)` nullable | `ValueFlow.created.grams`: Toncoin block rewards |
| `burned` | `Nullable(Decimal(38,0))` | `numeric(38,0)` nullable | `ValueFlow.burned.grams`: protocol burning |

Gross network fees before burning are `transaction_fees + import_fees`.
`import_fees` is not the value of imported messages or `ValueFlow.fees_imported`
(fees transferred from shards to MC). Sum each MC/shard block once, by its own
`gen_utime`; the including MC's time may fall in a different hour or day.

**Toncoins minted per day** is `SUM(created)`; **Toncoins burned per day** is
`SUM(burned)`. The protocol's `ValueFlow.minted` field is used for extra currencies,
not Toncoin block rewards. `burned` already includes fee burning and incoming
Toncoin burned at the configured masterchain blackhole address. The legacy
`ValueFlow` constructor has no `burned` field and means zero burning; the native
TON decoder handles both constructors.

ClickHouse adds the columns idempotently at startup. PostgreSQL requires
`ton-index-postgres-migrate --pg "$PG"` (schema **1.3.2**, all four columns in one
migration, together with Gramstat tables). If an earlier 1.3.2 was already applied locally, add
`--rerun-last-migration`. Existing rows retain `NULL` until replayed; parsed
values include real zeros where appropriate. To fill an
already-indexed range, use `--from N --to M --force`; for PostgreSQL also use
`--no-leader --pg-no-copy` for an independent bounded replay. PostgreSQL upserts
the four aggregate columns on conflicts; ClickHouse replaces replayed block rows.

Daily ClickHouse fees, minted and burned amounts (nanoTON), with deduplication.
Each metric remains `NULL` until its fields are filled on all rows in the bucket:

```sql
SELECT
    toStartOfInterval(toDateTime(gen_utime, 'UTC'), INTERVAL 1 DAY, 'UTC') AS bucket,
    countIf(transaction_fees IS NULL OR import_fees IS NULL OR created IS NULL OR burned IS NULL) AS missing_blocks,
    if(count(transaction_fees + import_fees) = count(), sum(transaction_fees + import_fees), NULL) AS total_fees,
    if(count(created) = count(), sum(created), NULL) AS minted_nanoton,
    if(count(burned) = count(), sum(burned), NULL) AS burned_nanoton
FROM blocks FINAL
WHERE gen_utime >= 1788220800 AND gen_utime < 1790812800
GROUP BY bucket ORDER BY bucket;
```

PostgreSQL equivalent:

```sql
SELECT
    date_trunc('day', to_timestamp(gen_utime), 'UTC') AS bucket,
    count(*) FILTER (WHERE transaction_fees IS NULL OR import_fees IS NULL OR created IS NULL OR burned IS NULL) AS missing_blocks,
    CASE WHEN count(transaction_fees + import_fees) = count(*) THEN sum(transaction_fees + import_fees) END AS total_fees,
    CASE WHEN count(created) = count(*) THEN sum(created) END AS minted_nanoton,
    CASE WHEN count(burned) = count(*) THEN sum(burned) END AS burned_nanoton
FROM blocks
WHERE gen_utime >= 1788220800 AND gen_utime < 1790812800
GROUP BY bucket ORDER BY bucket;
```

Use `1 HOUR` / `'hour'` for hourly points. These checks detect missing aggregates on
existing rows; full period coverage still requires all source blocks to be indexed.

## Transactions per day

Already available without `--gramstat`: sum each MC/shard block's transaction
count by its own UTC time, including failed and system transactions.
ClickHouse, three months of complete calendar days:

```sql
SELECT toDate(toDateTime(gen_utime, 'UTC'), 'UTC') AS day,
       sum(transaction_count) AS transactions,
       count() AS blocks
FROM mainnet_d.blocks FINAL
WHERE gen_utime >= toUnixTimestamp(toDateTime('2026-07-25 00:00:00', 'UTC'))
  AND gen_utime <  toUnixTimestamp(toDateTime('2026-10-25 00:00:00', 'UTC'))
GROUP BY day ORDER BY day
SETTINGS max_threads = 4, use_query_cache = 0, use_query_condition_cache = 0;
```

The query took **2.93 s server / 3.23 s client**.

## Checks

```sh
cmake --build build --target ton-index-clickhouse ton-index-postgres ton-index-postgres-migrate test-gramstat test-gramstat-postgres -j 4
ctest --test-dir build -R '^gramstat_' --output-on-failure
```

Tests cover exact monetary conversion, account-status transitions, missing inputs,
duplicate replays, backfill selection, block-fee/ValueFlow decoding, upserts and
chart queries (including the Accounts SQL above and missing-prefix handling). With
the respective database tools installed, they use isolated `clickhouse local` storage and a temporary
Unix-socket-only PostgreSQL cluster to verify commit/rollback behavior.
