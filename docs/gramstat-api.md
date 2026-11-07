# Gramstat read-only API

`ton-index-go/cmd/gramstat-api` is a long-running HTTP reader, separate from the
hourly `gramstat-aggregate` job. It serves prepared ClickHouse data. It has no
PostgreSQL/SS/RPC credentials, aggregation trigger, write route or SQL endpoint.

## HTTP contract

- `GET /api/health`: process liveness (not data freshness).
- `GET /api/v1/coverage`: observed first/last day and latest calculation time per
  source. These bounds do not certify gap-free ingestion.
- `GET /api/v1/dashboard?from=2026-10-21&to=2026-10-28`: UTC `[from,to)`, 1–93 days.
  `chain`, `daily`, `stars`, `rounds` arrays in a `schemaVersion: 2` response.
- `GET /api/v1/locations?round=<utime_since>`: selected round's coordinate points,
  coverage, cutoff and update timestamp. No raw IP, ADNL or internal record IDs.

GET and HEAD only. Unknown/repeated parameters, invalid dates and future days are
rejected before querying the source. Read failures return a generic 503; SQL and
internal error details stay in server logs. Missing records are empty arrays,
unknown values remain NULL. Integer money and 64-bit counters are decimal strings.
Only UI plot coordinates are converted to floating point; exact monetary strings
remain available in tooltips.

Count and geography snapshots join only when their roster digest and #34 hash
match. A stale geography snapshot never attaches to a changed roster. Round
timestamps are preserved rather than assigned to UTC daily buckets.

## Sources

The API role needs SELECT only on these tables:

```
gramstat.chain_daily
gramstat.daily_metrics
gramstat.telegram_stars_daily
gramstat.validator_rounds
gramstat.validator_geo_rounds
```

The latter four are maintained by the hourly aggregate job. `chain_daily` is a
compact preparation of a **separately audited bounded replay**; it is not yet
refreshed automatically. `--print-schema` in the aggregator includes its DDL.
Use [prepare-chain.sql](../ton-index-go/cmd/gramstat-api/deploy/prepare-chain.sql)
with a separate writer role and typed parameters `raw`, `metrics`, `from`, `to`,
`mc_from`, `mc_to`, `source`. It deduplicates raw blocks/snapshots with FINAL;
fees/minted/burned remain NULL if any corresponding block field is missing.
It replaces complete daily rows, never adds to an earlier aggregate.

Initial published chain coverage is **21–27 October 2026**, using the verified
detector-filter replay. The first/last day are conservatively partial because
shard generation timestamps can straddle the including-MC bounds. The newest
parent-cache performance replay is not used. Original source rows are unchanged.
Supply is the last available sampled full-state snapshot of the day; its own
timestamp is returned. It is not an exact midnight measurement.

Accounts, wallet activations/DAU/MAU and global token counts are not published from
this partial-history replay. Annual inflation, stake/returns and validator
attribution need their own prepared series. UI cards explain missing prerequisites.

## Deployment

Build with `go build ./cmd/gramstat-api`. The provided
[systemd unit](../ton-index-go/cmd/gramstat-api/deploy/gramstat-api.service) runs as
an unprivileged `gramstat-api` user, bound to `127.0.0.1:2008`.
Its root-owned mode-0600 `/etc/gramstat-api.env` contains:

```
GRAMSTAT_CH_URL=http://clickhouse.example.com:8123/
GRAMSTAT_CH_USER=gramstat_api
GRAMSTAT_CH_PASSWORD=<server-managed secret>
```

Use a separate ClickHouse user restricted to the application host, with `readonly=1`
and SELECT on the five tables above. Set its profile limits to match the fixed
query settings: `max_execution_time=10`, `max_threads=2`,
`max_memory_usage=268435456`, `max_result_rows=20000`,
`max_result_bytes=4194304`, `result_overflow_mode='throw'`,
`max_rows_to_read=2000000`, `read_overflow_mode='throw'`,
`join_use_nulls=1`, `output_format_json_quote_64bit_integers=1`,
`max_concurrent_queries_for_user=4`. No INSERT/DDL or raw-database grants.

The [nginx example](../ton-index-go/cmd/gramstat-api/deploy/nginx.conf) binds port
**2007 only on the private address** (`PRIVATE_BIND_ADDRESS`). `/` serves the UI,
`/api/` proxies the loopback service. Static paths are allowlisted; historical
snapshots, provenance and repository files are not served. Copy only `index.html`,
`app.mjs`, `catalogue.mjs`, `charts.mjs`, `data.mjs`, `style.css`, `favicon.svg`
into `/opt/gramstat/ui`. Validate with `nginx -t` before reload.

The intended public endpoint is **https://stats.ton.org** through HAProxy. That
external route and TLS are a later deployment step. No frontend URL rewrite is
needed: all API requests are same-origin. Configure trusted proxy IPs explicitly
before enabling real-IP forwarding/rate limiting behind HAProxy; never trust
client-supplied X-Forwarded-For globally. Private hostnames/addresses and secrets
belong only in deployment configuration, not these public examples.

The API caches complete successful responses for five minutes, coalesces same-key
misses, allows four source loads concurrently and retains at most 32 entries of
at most 4 MiB. HTTP has a 15-second request deadline; nginx also limits request
rate. Failed loads are not cached as successful zero data. The UI refreshes every
five minutes while visible and shows per-source freshness and per-point partial
coverage. Public GET responses can be cached for 60 seconds.

## Verification

```
go test -race ./internal/gramstat/... ./cmd/gramstat-api ./cmd/gramstat-aggregate
go vet ./internal/gramstat/... ./cmd/gramstat-api
```

The SQL integration test uses isolated ClickHouse-local data and executes every
read query plus the preparation SQL: retry deduplication, exact money, NULL fees,
partial boundaries and rejecting stale geo joins. HTTP tests cover input limits,
cache coalescing, rejected writes and safe errors. Before exposing an installation,
check read access and denial of writes for its actual ClickHouse role, API payloads,
static allowlist, headers and the UI in a browser.
