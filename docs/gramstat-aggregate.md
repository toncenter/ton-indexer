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

The embedded [daily.sql](../ton-index-go/internal/gramstat/handlers/actions/daily.sql)
computes all metrics in one source scan using `GROUPING SETS` for day, DEX and
pool. Additional distinct counts share the scan, with additional aggregation
CPU/memory. Before aggregation, the handler finds the
MC immediately before the interval using `blocks.gen_utime`. It restricts actions
by the existing **`trace_mc_seqno_end` index**, retaining the exact timestamp filter.
The upper MC bound is the latest action MC in the same database snapshot, **not**
the MC at the interval end: a transfer can finish its trace after midnight.
PostgreSQL uses custom query plans for the actual bounds rather than a cached
generic plan. In a seven-day comparison within one database snapshot, the six
transfer metrics took **3.787 s**, and the full query took **11.879 s**.

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

These are **counts of currently indexed actions**. The latest source MC is an
observation boundary, not proof of continuous classification coverage. Even a
closed calendar day can change after late classification. The rolling refresh
handles recent changes; older backfills/reclassification need an explicit date
range. Each whole chunk is collected before its synchronous ClickHouse insert;
a retry can repair a failed/uncertain insert. Failed handlers leave their previous
values in place, other handlers continue, and the process exits nonzero.

## Scheduled runs (systemd)

The example units in `ton-index-go/cmd/gramstat-aggregate/deploy/` expect the binary
at `/opt/gramstat/bin/gramstat-aggregate`, a root-only environment file at
`/etc/gramstat-aggregate.env`, and a service user named `gramstat`. Adjust these
paths as needed, install the units on a host with access to both databases, and
enable `gramstat-aggregate.timer`.
It runs every UTC hour, with persistent catch-up after a reboot. `flock` prevents
overlap; manual runs on that host should use the same lock. The service has no
write access to the source and needs only `SELECT, INSERT` on
`gramstat.daily_metrics` in ClickHouse. DDL is applied separately by an administrator.

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
