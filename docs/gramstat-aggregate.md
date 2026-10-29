# Gramstat indexed-data aggregator

`ton-index-go/cmd/gramstat-aggregate` is a Go CLI for independent analytics
handlers. It reads indexed sources and publishes to ClickHouse `gramstat`.
The first handler, `actions`, reads PostgreSQL. It does not read node states.

## Run

```bash
cd ton-index-go
go build -o /path/to/gramstat-aggregate ./cmd/gramstat-aggregate
gramstat-aggregate --list-handlers
gramstat-aggregate --print-schema  # apply this DDL once with an administrative CH user

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

The UTC day is derived from `start_utime`. Child actions are included, so a
transfer nested inside a swap participates. These are classified actions, not a
count of identified people; contract and bot addresses also count as senders and
receivers. Addresses are deduplicated per day across all assets of each type;
unknown endpoints are excluded from distinct counts but their successful
transfers still count.
Historical availability of child actions depends on the classifier's coverage.

One query computes all six metrics. Additional distinct counts share the same
scan, with additional aggregation CPU/memory. Before aggregation, the handler finds the
MC immediately before the interval using `blocks.gen_utime`. It restricts actions
by the existing **`trace_mc_seqno_end` index**, retaining the exact timestamp filter.
The upper MC bound is the latest action MC in the same database snapshot, **not**
the MC at the interval end: a transfer can finish its trace after midnight.
PostgreSQL uses custom query plans for the actual bounds rather than a cached
generic plan. A live seven-day sender query with this lower bound took **5.677 s**.

## Destination and refresh semantics

`gramstat.daily_metrics` stores `(metric, day, value, calculated_at,
data_through, source_mc_seqno, is_partial)`. Counts are exact `UInt64` values.
Monthly `ReplacingMergeTree(calculated_at) ORDER BY (metric, day)` replaces
absolute counts; repeats do not add to them. A decrease, or a zero after
reclassification, also replaces the old value. Read with `FINAL`:

```sql
SELECT day, value AS senders, is_partial, data_through, calculated_at
FROM gramstat.daily_metrics FINAL
WHERE metric = 'jetton_senders'
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

For example, a future validator-countries handler can combine the selected #34
roster, timestamp-bounded session-stats IP observations and a GeoLite2 MMDB. Its
complete distribution, coverage and GeoIP version should be one versioned
snapshot in its own `gramstat` table, so removed countries cannot remain as stale
rows. It is not implemented by this first release.

## Checks

```bash
go test -race ./internal/gramstat/... ./cmd/gramstat-aggregate
# Optional real PostgreSQL test: creates and removes its own temporary database.
GRAMSTAT_TEST_PG_DSN='host=localhost dbname=postgres user=...' \
  go test ./internal/gramstat/handlers/actions
go vet ./internal/gramstat/... ./cmd/gramstat-aggregate
```
