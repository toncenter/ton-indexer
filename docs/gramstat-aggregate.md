# Gramstat indexed-data aggregator

`ton-index-go/cmd/gramstat-aggregate` is a Go CLI for independent analytics
handlers. It reads indexed sources and publishes to ClickHouse `gramstat`.
The `actions` handler reads PostgreSQL. [Validator handlers](gramstat-validators.md)
publish elected counts and existing session-stats locations by round. They run
independently through the same hourly runner.

## Run

```bash
cd ton-index-go
go build -o /path/to/gramstat-aggregate ./cmd/gramstat-aggregate
gramstat-aggregate --list-handlers
gramstat-aggregate --print-schema  # apply create/upgrade DDL with an administrative CH user

# Credentials belong in a protected environment file, not command arguments.
export GRAMSTAT_PG_DSN='host=postgres.example.com port=5432 dbname=ton_index user=gramstat_reader'
export GRAMSTAT_CH_URL='http://clickhouse.example.com:8123/'
export GRAMSTAT_CH_USER='gramstat_aggregate'
# GRAMSTAT_CH_PASSWORD / PostgreSQL password supplied by the deployment environment.
gramstat-aggregate --handlers actions
gramstat-aggregate --handlers actions --from 2026-10-21 --to 2026-10-28
```

Dates are UTC and `--to` is exclusive. The normal run refreshes today and three
previous days (`--lookback-days 3`), and repairs missing closed days within the
existing metrics history. An empty destination starts with that lookback. Explicit
dates override automatic catch-up. Ranges are processed in chunks of at most seven
days; `--timeout` limits each handler (default 20 minutes). PostgreSQL statements
have a five-minute timeout and run in a read-only, repeatable-read transaction.

## Metrics and source query

| Metric | Definition |
| --- | --- |
| `jetton_senders` | Distinct non-null owner `source` addresses in successful `jetton_transfer` actions |
| `jetton_receivers` | Distinct non-null owner `destination` addresses in successful `jetton_transfer` actions |
| `jetton_transfers` | Successful `jetton_transfer` action rows, including rows with an unknown source |
| `nft_senders` | Distinct non-null previous-owner `source` addresses in successful `nft_transfer` actions |
| `nft_receivers` | Distinct non-null new-owner `destination` addresses in successful `nft_transfer` actions |
| `nft_transfers` | Successful `nft_transfer` action rows |
| `swaps_total` | Successful `jetton_swap` actions; `breakdown` contains counts by DEX |
| `swaps_users` | Distinct non-null swap `source` addresses across all DEXes; receiver differences do not matter |
| `liquid_staking_deposits` | Deposit count summed over the four selected pools; `amount_nanoton` holds their total deposit amount |
| `liquid_staking_deposits_<pool>` | Per-pool count and `amount_nanoton`; `<pool>` is `tonstakers`, `stakee`, `kton`, or `sgram` |
| `ton_dns_mints` | Successful `nft_mint` actions whose `asset` is the mainnet `.ton` collection |
| `telegram_stars_purchases` | Successful TON/USDT payments to the selected Fragment address with a Stars comment; includes unknown quantities |

The UTC day is derived from `start_utime`. Child actions are included, so a
transfer nested inside a swap participates. These are classified actions, not a
count of identified people; contract and bot addresses also count as senders and
receivers. Addresses are deduplicated per day across all assets of each type;
unknown endpoints are excluded from distinct counts but their successful
transfers still count.
Historical availability of child actions depends on the classifier's coverage.

Swaps are counted as classified actions, not their individual hops. DEX keys are
the source's `(jetton_swap_data).dex`, preserving versions such as `stonfi_v2`
and `dedust_v2`; absent/empty names use `unknown`. New DEX labels are included
automatically. Daily unique swap users are calculated directly, never by summing
per-DEX unique counts.

Liquid staking requires `stake_deposit`, `provider='liquid_staking'` and one of
these **mainnet destination pools**:

| Pool key | Destination |
| --- | --- |
| `tonstakers` | `0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA` |
| `stakee` | `0:F6FF877DD4CE1355B101572045F09D54C29309737EB52CA542CFA6C195F7CC5B` |
| `kton` | `0:3D1F01190FFB4E35F573EA5252D0F54A479BE2122106CA3F40B907B01905675C` |
| `sgram` | `0:9FE32DD92875447A6B83D031D0230C539752152B660D4A056797BE9AD2E396D5` |

`sgram` is the pool whose token metadata names it Liquid GRAM. This is the
selected four-pool series, not total coverage of every liquid-staking protocol.
Ethena, nominator pools, JVault, Coffee staking and unlisted destinations are
excluded. Amounts are `SUM(actions.amount)` in exact nanoTON; the current family
detector already subtracts a 1 TON deposit fee. No fee is subtracted again.
An unknown amount makes that pool's daily sum and the total sum NULL, while
counts and other known pool sums remain available. Empty pools have count/sum zero.

DNS mints filter the collection in `asset`:
`0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF`.
They count mint actions, not auction completions, sales or renewals.

The [Stars distribution](gramstat-stars.md) stores both purchase counts and Stars
quantities per exact purchase size, payment asset and standard/prepaid product.
It uses PostgreSQL only, with no external API calls.

The embedded [daily.sql](../ton-index-go/internal/gramstat/handlers/actions/daily.sql)
computes all metrics in one source scan using `GROUPING SETS` for day, DEX,
pool and Stars bucket. Comment parsing is materialized once to prevent repeated
regex evaluation after CTE inlining. Additional distinct counts share the scan, with additional aggregation
CPU/memory. Before aggregation, the handler finds the
MC immediately before the interval using `blocks.gen_utime`. It restricts actions
by the existing **`trace_mc_seqno_end` index**, retaining the exact timestamp filter.
The upper MC bound is the latest action MC in the same database snapshot, **not**
the MC at the interval end: a transfer can finish its trace after midnight.
PostgreSQL uses custom query plans for the actual bounds rather than a cached
generic plan. In the original seven-day comparison, the six transfer metrics
took **3.787 s**, and the pre-Stars query took **11.879 s**. With Stars, a
37-hour read-only probe took **13.24 s**; these are individual cached measurements.

## Destination and refresh semantics

`gramstat.daily_metrics` stores `(metric, day, value, calculated_at,
data_through, source_mc_seqno, is_partial, amount_nanoton, breakdown)`.
Counts are exact `UInt64`; `amount_nanoton` is `Nullable(Decimal(38,0))`, encoded
as decimal strings during transport. `breakdown` is `Map(String, UInt64)` and
holds the complete DEX distribution on the `swaps_total` row. A new version
replaces the whole map, so a disappearing DEX does not leave a stale entry.
Other metrics use NULL amounts and empty maps. `--print-schema` includes an
idempotent ALTER for older tables; apply it before upgrading the executable.
Monthly `ReplacingMergeTree(calculated_at) ORDER BY (metric, day)` replaces
absolute counts; repeats do not add to them. A decrease, or a zero after
reclassification, also replaces the old value. Read with `FINAL`:

```sql
SELECT day, value AS senders, is_partial, data_through, calculated_at
FROM gramstat.daily_metrics FINAL
WHERE metric = 'jetton_senders'
ORDER BY day;
```

```sql
-- Each staking row has both a count and its exact sum.
SELECT day, metric, value AS deposits, amount_nanoton
FROM gramstat.daily_metrics FINAL
WHERE startsWith(metric, 'liquid_staking_deposits')
ORDER BY day, metric;

SELECT day, value AS swaps, breakdown AS swaps_by_dex
FROM gramstat.daily_metrics FINAL
WHERE metric = 'swaps_total'
ORDER BY day;
```

`calculated_at` is the source transaction timestamp. `data_through` is the
exclusive requested time boundary, capped at the latest action MC's block time
and the individual day's end. A truncated day has `is_partial=1`; days beyond the
source head are not emitted. Empty days inside a successfully queried interval
get explicit zero counts. Missing source anchors and failed queries emit nothing.

The Stars distribution is written before its `telegram_stars_purchases` daily
metric, which serves as the catch-up completion marker. An interrupted insert
leaves the chunk eligible for a retry; chart readers use the self-contained
Stars snapshot rather than joining independently versioned tables. Adding the
metric repairs its missing days within established actions history.

These are **counts of currently indexed actions**. The latest source MC is an
observation boundary, not proof of continuous classification coverage. Even a
closed calendar day can change after late classification. The rolling refresh
handles recent changes; older backfills/reclassification need an explicit date
range. Each whole chunk is collected before its synchronous ClickHouse insert;
a retry can repair a failed/uncertain insert. Failed handlers leave their previous
values in place, other handlers continue, and the process exits nonzero.

## Scheduled runs (systemd)

### Prepared chain intervals and consumer API

Existing-account counts are prepared by the independent `accounts` handler;
see [Accounts chart and verified baseline](gramstat-accounts-chart.md).

The `chain` handler prepares `gramstat.chain_daily_full` from a continuously
indexed mainnet history. Configure `GRAMSTAT_CHAIN_RAW_DB` (blocks) and
`GRAMSTAT_CHAIN_METRICS_DB` (account deltas), then include `chain` in `--handlers`.
The writer needs SELECT on those two source tables and SELECT/INSERT on
`gramstat.chain_daily_full`. Apply `--print-schema` before enabling the handler.

Every refresh scans deduplicated blocks once (two threads, 90-second query limit,
2-GiB memory limit) and aggregates daily rows. A 1000-MC settling margin is followed
by a complete MC/delta-prefix check from MC 3. Missing or unordered coverage aborts
publication. The compact daily burn-prefix window is evaluated over the whole
history before any requested date filtering. The latest `(gen_utime, seqno)` MC
must also be the day's greatest MC, and successive day prefixes must be contiguous.
This makes the daily cumulative burn exactly the prefix at the selected snapshot.
See [supply chart semantics](gramstat-supply-chart.md) for the v17 cutoff and the
verified pre-burning zero baseline. NULL final balances never fall back to earlier
observations. The first and last two indexed days are conservatively partial.

Rows are replaced by day, not added to prior totals. Source blocks are never
modified. `gramstat.total_supply` archives are not inputs. Raw source history and
the API's independently prepared action/validator datasets have separate coverage.

The same handler publishes `gramstat.supply_hourly` after verifying daily MC/delta
coverage. Each UTC hour contains first/last MC and timestamps, cumulative burning
strictly before its first MC (`opening_burn`), cumulative burning through its last
MC, and corrected supply at that exact last MC. NULL burning makes dependent
prefixes unknown; NULL final balances do not fall back. The final incomplete hour
is marked partial and replaced on subsequent refreshes. Whole-history preparation
is limited to two threads and the same query/memory bounds; only the compact
hourly rows are persisted, not a second per-MC table. Apply `chain.Schema` via
`--print-schema`, grant the writer SELECT/INSERT on `gramstat.supply_hourly`, and
grant readers SELECT on it and the required raw block columns. API short-window
reads start from an hour's first MC, add intra-hour burning to its opening prefix,
and apply the requested time filter only after calculating the prefix.

### Annualized net inflation (fixed trailing 24 hours)

After checkpoints, `chain` writes `gramstat.inflation_hourly`. At each closed UTC
hour boundary `t`, it sums all deduplicated blocks' `created` and `burned` over
`[t-24h,t)`, then computes:

```text
annualized_percent = (minted_24h_nanoton - burned_24h_nanoton)
                     / supply_nanoton * 365 * 100
```

Supply is the corrected last MC balance before `t`, with its timestamp and MC
retained. This is a linear annualized rate, not compounded yield or realized
year-over-year growth. It can be negative. Exact monetary inputs are stored as
Decimal(38,0); the percentage is Decimal(38,12), calculated using Decimal256
division. The API retains the exact inputs for reproducibility.

The rolling window uses an actual 23-hour preceding time range plus the current
hour, not merely the preceding 24 rows. It requires 24 present hourly records,
known flow fields, exclusion of the initial partial source hour, and positive
known Supply. The current unclosed hour is not published; other incomplete
windows retain NULL. Hourly inputs and rates are recomputed as indexing advances.
Chart resolution/date filtering is downstream and cannot reset or resize the
24-hour calculation window. Apply the additional DDL with `--print-schema`, grant
the writer SELECT/INSERT on `gramstat.inflation_hourly`, and readers SELECT.

### Legacy bounded chain intervals

`gramstat.chain_daily` holds compact daily flow totals and the last sampled supply
snapshot from an explicitly audited bounded replay. Its DDL is included in
`gramstat-aggregate --print-schema`. Preparation is currently manual, using
[`deploy/prepare-chain.sql`](../ton-index-go/cmd/gramstat-aggregate/deploy/prepare-chain.sql)
with writer credentials and parameters `raw`, `metrics`, `from`, `to`, `mc_from`,
`mc_to`, `source`. UTC `to` is exclusive. The first and last day are conservatively
partial because shard block timestamps can straddle the including-MC bounds.
Replays replace daily rows; missing monetary inputs remain NULL.

The API and UI belong to the standalone **[1IxI1/gramstat](https://github.com/1IxI1/gramstat)**
application. They read these prepared tables with SELECT-only credentials and
have no code dependency on the indexer. Production DDL, data preparation and the
hourly aggregation job remain here. The API is not part of the TON Center API product.

### Hourly deployment

The example units in `ton-index-go/cmd/gramstat-aggregate/deploy/` expect the binary
at `/opt/gramstat/bin/gramstat-aggregate`, a root-only environment file at
`/etc/gramstat-aggregate.env`, and a service user named `gramstat`. Adjust these
paths as needed, install the units on a host with access to both databases, and
enable `gramstat-aggregate.timer`.
It runs every UTC hour, with persistent catch-up after a reboot. `flock` prevents
overlap; manual runs on that host should use the same lock. The service has no
write access to the source. For actions it needs `SELECT, INSERT` on
`gramstat.daily_metrics` and `gramstat.telegram_stars_daily`, plus `SELECT` on
`gramstat.telegram_stars_buckets` for chart readers. The validator handlers have
their own grants. DDL is applied separately by an administrator before upgrading
the binary; `--print-schema` includes all tables and views.

```bash
sudo systemctl start gramstat-aggregate.service
sudo systemctl list-timers gramstat-aggregate.timer
sudo journalctl -u gramstat-aggregate.service --since today
```

## Adding handlers

Implement `gramstat.Handler` (`Name`, `Run`) and register it in the CLI. Inject
only the source clients and writer it needs. `Request.AsOf` supplies one stable
UTC reference; handlers own their refresh policy and may publish a different
typed table. SQL and individual metric definitions stay with their handler.

The `validators` and `validator_geo` handlers demonstrate separate typed tables:
verified #34 rosters and full round-level location/distribution snapshots. GeoIP
fields are reused directly from session stats, with explicit coverage and unknown
values; no additional MMDB lookup is performed. See [validator rounds](gramstat-validators.md).

## Checks

```bash
go test -race ./internal/gramstat/... ./cmd/gramstat-aggregate
# Optional real PostgreSQL test: creates and removes its own temporary database.
GRAMSTAT_TEST_PG_DSN='host=localhost dbname=postgres user=...' \
  go test ./internal/gramstat/handlers/actions
go vet ./internal/gramstat/... ./cmd/gramstat-aggregate
```
