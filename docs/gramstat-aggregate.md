# Gramstat indexed-data aggregator

`ton-index-go/cmd/gramstat-aggregate` is a Go CLI for independent analytics
handlers. It reads indexed sources and publishes to ClickHouse `gramstat`.
The `actions` handler reads PostgreSQL. [Validator handlers](gramstat-validators.md)
publish elected counts and existing session-stats locations by round. They run
independently through the same Go scheduler. systemd supervises the long-running
process; it no longer schedules individual aggregation runs.

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

For the initial whole-history actions backfill, use one source scan instead of
repeating a scan to the latest action MC for every seven-day chunk:

```bash
gramstat-aggregate --handlers actions --from 2019-11-15 --to 2026-10-21 \
  --actions-chunk-days 3660 --actions-include-monthly \
  --actions-work-mem-mb 256 --actions-parallel-workers 2 \
  --actions-statement-timeout 24h --timeout 25h
```

The end date should be the start of already prepared recent history. This is a
long-running PostgreSQL read, not an indexer replay. No source tables or indexes
are modified. Normal runs default to no parallel scan workers and 64 MiB work_mem.
The explicit backfill settings above permit two scan workers and 256 MiB per
sort/hash operation (not a total connection memory cap). Limits are 0–4 workers
and 1–1024 MiB, applied only to this connection, never globally. Large sorts may
still spill to database temporary storage. It publishes
only after the complete source query succeeds. The genesis day is accepted only
with a verified first-MC=1 anchor; an absent opening history is not zero-filled.
Large chunks require explicit dates and do not change the regular seven-day chunk policy.
`--actions-include-monthly` requires explicit dates and a 3660-day chunk. It reuses
the materialized source for monthly distinct counts instead of scanning the raw
table again. Only complete trailing months are published; a mid-month historical
end is left to the independent monthly handler. Genesis is allowed and marked
partial. Monthly rows are published before the daily catch-up completion marker.
Use a dedicated historical-run lock only for a disjoint old range so the normal
service can continue refreshing recent days and the other independent handlers.
Use a direct PostgreSQL connection for this job: the production proxy on port
15432 disconnects silent long-running queries after approximately three minutes.
Keep the DSN in a root-only environment file. The daemon also uses a direct
connection to that same PostgreSQL server. A transport failure publishes no rows.

## Metrics and source query

### Monthly unique Activity

The independent `actions_monthly` handler prepares exact calendar-month unique
jetton/NFT senders and receivers and swap users. It queries source addresses
directly with COUNT(DISTINCT); daily unique counts are never added together.
It writes five `monthly_<metric>` rows to the existing `daily_metrics` table,
using the month's first date as `day`. The separate metric names keep daily
publication and catch-up untouched. No additional table grants are needed.
Default refresh covers the previous and current UTC calendar months. Explicit
ranges must start at a month boundary (or genesis) and end at a month boundary
or tomorrow UTC. Genesis and current incomplete months are marked partial.
Historical runs can use `--actions-statement-timeout 24h --timeout 25h` and the
same dedicated history lock, serializing them behind the daily-history scan.
The daemon schedules this handler every 15 minutes, independently of the five-minute
handlers. It uses the same protected direct-connection environment; publication
timestamps keep newer snapshots authoritative. Its deadline is five minutes.
Additive monthly counts,
amounts and Stars quantities are composed from complete sets of prepared daily
rows by Gramstat; missing inputs and unknown Stars quantities remain unknown.

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
reads the source once and materializes only the required fields. Daily totals
calculate the five distinct-address counts; DEX, pool and Stars breakdowns have
separate additive aggregates over only their relevant rows. Unlike the former
GROUPING SETS query, those breakdowns do not repeat unused distinct-address work
or sort unrelated transfers into NULL buckets. Only actual Stars comments enter
the materialized regex parser. On a read-only seven-day production comparison
(2026-11-01 through 2026-11-08, exclusive), all results matched exactly: old SQL
took 32.1–35.0 s, new SQL 25.8–25.9 s with identical 64-MiB/zero-worker settings.
These are measured interval timings, not an ETA for the entire history.
Before aggregation, the handler finds the
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
values in place and other handlers continue. One-off runs exit nonzero; the daemon
logs the error and retries at the next scheduled interval.

## Preparation and scheduling

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
aggregation service remain here. The API is not part of the TON Center API product.

### Continuous service deployment

The example service in `ton-index-go/cmd/gramstat-aggregate/deploy/` expects the binary
at `/opt/gramstat/bin/gramstat-aggregate`, a root-only environment file at
`/etc/gramstat-aggregate.env`, and a service user named `gramstat`. Adjust these
paths as needed and install it on a host with access to both databases. For an
upgrade, disable and stop all three old timers, then stop their one-off services
before replacing the binary/unit. Enable `gramstat-aggregate.service` itself.
The old aggregate, monthly and stake-locked timer templates are retired.

`--daemon` starts a bounded scheduler with these deployment defaults:

| Setting | Value |
| --- | --- |
| Regular handlers | every 5 minutes, deadline 3 minutes each |
| `actions_monthly` | every 15 minutes, deadline 5 minutes |
| Historical reconciliation | on startup and once per hour per handler |
| Concurrent handlers | at most 3; never two executions of the same handler |

Each start receives a fresh, stable UTC `Request.AsOf`. Missed ticks coalesce;
the next start is a future UTC-aligned boundary, without queued catch-up bursts.
`validators` precedes geo/stake, stake precedes changes, and geo/stake precede
locked attribution. A failed predecessor does not prevent independent handlers
or consumers of previously verified rows from running; consumers retain their
existing identity and completeness checks. Duration, mode (`recent`/`reconcile`),
error and next execution time are logged per handler. A historical rejected
roster remains an explicit reconciliation error, not fabricated data or a
process-wide shutdown. Recent runs exclude old incomplete rounds.

The frequent runs set `Request.RecentOnly`: actions skip old-gap catch-up;
validators/geo/locked refresh their recent overlap; stake changes fetch only
target rounds and their actual two predecessors. Member reads use exact cycle
IDs in bounded batches, not a broad interval between sparse targets. The hourly
pass retains full historical reconciliation. Chain/account source-prefix proofs
and wallet preparation keep their existing completeness policies.
Affiliation observations and failed-RPC attempts have one-run lifetime, so a
long-running process still retries transient failures and sees persisted corrections.

SIGTERM/SIGINT cancels in-flight I/O and drains handlers. systemd restarts an
unexpectedly exited process, with a 45-second shutdown grace period. `flock`
holds the existing single-writer lock for the process lifetime; stop the daemon
before an overlapping manual one-off run using that same lock. For a bounded
diagnostic refresh use `--recent-only` without `--daemon`. Explicit backfill dates
and large historical-scan options cannot be combined with daemon mode.

November 11, 2026 migration checks: the direct and proxied DSNs resolved to the
same PostgreSQL database/server and MC head. An index-backed exact-ID member
query returned the same 1,463 rows as the bounded previous query (zero differences;
27 ms for four rounds). The recent validator/geo/stake/changes/locked pilot took
8.29 s. Full stake-change and locked reconciliation took 7.35 s and 2.67 s;
all 6,674 prepared snapshots across both tables retained identical hashes,
amounts and statuses, with no missing rows. These are pilot measurements, not
guaranteed runtime bounds.

The deployed scheduler was observed through the 13:55 and 14:00 UTC refreshes
and the second monthly run on November 11. Both recent cycles finished all nine
regular handlers in about 48 s: actions 14.1 s, accounts 7.5 s, wallets 3.0 s,
locked stake 2.0–2.2 s and stake changes 0.18 s. The second monthly execution
took 71.75 s and scheduled its successor for 14:15 UTC. There were no transport
timeouts or process restarts during this observation. The startup historical
pass still reported the known incomplete roster `1607798599`; recent runs
succeeded without suppressing that historical rejection. All three old timers
were retired, and the daemon retained the single-writer lock.

The service has no
write access to the source. For actions it needs `SELECT, INSERT` on
`gramstat.daily_metrics` and `gramstat.telegram_stars_daily`, plus `SELECT` on
`gramstat.telegram_stars_buckets` for chart readers. The validator handlers have
their own grants. DDL is applied separately by an administrator before upgrading
the binary; `--print-schema` includes all tables and views.

```bash
sudo systemctl disable --now gramstat-aggregate.timer gramstat-activity-monthly.timer gramstat-stake-locked.timer
sudo systemctl stop gramstat-aggregate.service gramstat-activity-monthly.service gramstat-stake-locked.service
# Install the new unit/binary and protected direct-PostgreSQL environment here.
sudo systemctl daemon-reload
sudo systemctl enable --now gramstat-aggregate.service
sudo systemctl status gramstat-aggregate.service
sudo journalctl -u gramstat-aggregate.service --since today
```

## Adding handlers

Wallet preparation and exact minute/hour/day reader queries are documented in
[gramstat-wallets.md](gramstat-wallets.md). The `wallets` handler also prepares
per-MC cumulative activations and long-range checkpoints.

Implement `gramstat.Handler` (`Name`, `Run`) and register it in the CLI. Inject
only the source clients and writer it needs. `Request.AsOf` supplies one stable
UTC reference; handlers own their refresh policy and may publish a different
typed table. In daemon mode a handler instance is reused serially, so run-scoped
caches/retry suppression must be reset in `Run`. SQL and individual metric
definitions stay with their handler.

The `validators` and `validator_geo` handlers demonstrate separate typed tables:
verified #34 rosters and full round-level location/distribution snapshots. GeoIP
fields are reused directly from session stats, with explicit coverage and unknown
values; no additional MMDB lookup is performed. See [validator rounds](gramstat-validators.md).

## Checks

```bash
go test -race ./internal/gramstat/... ./cmd/gramstat-aggregate
# Optional real PostgreSQL test: creates and removes its own temporary database.
GRAMSTAT_TEST_PG_DSN='host=localhost dbname=postgres user=...' \
  go test ./internal/gramstat/handlers/actions ./internal/gramstat/handlers/validators
go vet ./internal/gramstat/... ./cmd/gramstat-aggregate
```
