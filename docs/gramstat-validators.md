# Validator rounds and locations

The Go `gramstat-aggregate` runner has two independent mainnet handlers:

- `validators`: elected #34 validator count and complete roster, one snapshot per round.
- `validator_geo`: the selected roster's session-stats locations, country distribution
  and map points, one snapshot per round.

The hourly timer refreshes the current round and rounds overlapping the recent
lookback. The x-axis is the round's start; its end and observation cutoff belong
in the tooltip. Typical rounds last 65,536 seconds, but the implementation uses
source intervals rather than manufacturing fixed 18-hour buckets.

## Sources and checks

`validators` reads `validator_cycles` and `validator_cycle_members` in a read-only,
repeatable-read PostgreSQL transaction. It checks `total`, contiguous dictionary
indices, unique pubkeys, positive weights and their exact `UInt64` sum. `main`
is retained as metadata; the chart uses `total`.

These tables contain sets observed in #32/#34/#36. Their `source_mc_seqno` is
the latest indexed observation, **not** an activation block. For each round the
handler locates the first indexed MC at or after `utime_since` (within 300 seconds),
requires a key block and its immediately preceding MC before `utime_since`, and
reads `getConfigParam(config_id=34, seqno=anchor_mc_seqno)` from a TON Center v2 API.
It decodes the entire set and compares interval, total, main, indices, pubkeys,
ADNL and exact weights with PostgreSQL. The config cell hash, full MC identity
and roster digest are saved. The RPC check is reused only while the full anchor
identity, roster digest and calculation version match.

This is a **trusted historical RPC check**, not an independent Merkle-proof
verification: v2 `getConfigParam` does not return a state proof. No NodeDB replay
is performed. Unresolvable/ambiguous activation boundaries or mismatching sets
fail that round; valid sibling rounds can still be published. Historical gaps
and interval overlaps are not filled by guessing the nearest cycle.

`validator_geo` reads the verified rosters from the destination and submits them
as a transient external table to ClickHouse. Session-stats `validators.cycle_id`
is not used as a unique key or as a prerequisite for IP coverage.

The source query is [locations.sql](../ton-index-go/internal/gramstat/handlers/validators/locations.sql).
For every member it selects observations within `[utime_since, cutoff)`, with
`cutoff = min(utime_until, requested_as_of, indexed_MC_time, SS_latest_time + 1)`.
The last `(created_timestamp, record_id)` wins. An `argMax(tuple(...))` keeps
country, coordinates, city and timestamp from **one record**, including NULLs:
an older non-null coordinate cannot silently replace a missing latest one.

Coordinates and countries come directly from `node_addresses.geoip_*`. There is
no second GeoIP lookup, MMDB dependency or need to download a database. The
original GeoIP version is unknown and saved as NULL. The country is the source's
IP-geolocation label; coordinates are approximate network locations.

The processor retains each selected record ID and observation time, but does not
copy raw IPs into its published map snapshots. Multiple IPs/countries within a
round are diagnostic counts. Conflicting payloads with the same ordering key
make that participant's location unknown. Missing/out-of-range coordinates and
the `(0,0)` placeholder are unknown; an absent city does not prevent a map point.
Missing observations are never borrowed from another round.

## Destination

- `gramstat.validator_rounds`: count, full roster and #34/MC provenance.
- `gramstat.validator_geo_rounds`: full per-member geo snapshot and its aggregates.
- `gramstat.validator_geo_members`: view expanding the latest geo snapshot into
  one row per validator, including unknown locations.

Both tables use `ReplacingMergeTree(calculated_at)`, monthly partitions by round
start and key `utime_since`. This deployment is mainnet-specific; use a separate
destination for a different network. Always read the tables with `FINAL`.
Each array of members and its aggregates are one row, so replacing a snapshot
also removes old map points and disappearing countries atomically.

Geo fields:

| Field | Meaning |
| --- | --- |
| `total` | Elected roster size |
| `observed` | Participants with an observation in this round |
| `located` | Participants with usable map coordinates |
| `unknown` | `total - located` |
| `country_known` | Participants with an unambiguous two-letter country code |
| `countries` | Distinct observed country codes; NULL when none are known |
| `breakdown` | Complete country → participant-count map |
| `data_through` | Exclusive observation cutoff |
| `source_through` | Latest SS timestamp observed at query start; not proof of continuous coverage |
| `is_partial` | The observation cutoff has not reached the round's end |
| `status` | `complete`, `partial` coverage/current round, or `missing_observations` |

Country coverage and coordinate coverage are distinct. A closed round can still
have incomplete coverage; the chart should show this, along with `located/total`.
The countries count is then the number observed, not a claim about every validator.
On a query failure, previous snapshots remain available with their old timestamps;
the handler returns an error rather than publishing zero results.

## Run and deploy

```bash
gramstat-aggregate --print-schema  # apply DDL using an administrative destination user
export GRAMSTAT_PG_DSN='host=postgres.example.com dbname=ton_index user=gramstat_reader'
export GRAMSTAT_CH_URL='http://clickhouse.example.com:8123/'
export GRAMSTAT_CH_USER='gramstat_aggregate'
export GRAMSTAT_CONFIG_URL='https://toncenter.com/api/v2'
# Optional GRAMSTAT_CONFIG_API_KEY; all passwords belong in a protected environment.
export GRAMSTAT_SS_URL='http://session-stats.example.com:8123/'
export GRAMSTAT_SS_USER='gramstat_reader'
# GRAMSTAT_CH_PASSWORD and GRAMSTAT_SS_PASSWORD supplied by the deployment environment.

gramstat-aggregate --handlers validators,validator_geo --from 2026-10-21 --to 2026-10-31
gramstat-aggregate --handlers actions,validators,validator_geo
```

Dates select overlapping rounds and cap the observation cutoff; `--to` is
exclusive and an unfinished requested round remains partial. Automatic runs
repair missing rounds within established count history and missing geo snapshots
for verified rounds. New count history starts at the usual lookback, not genesis.
Older source corrections require an explicit date range. Geo batches contain at
most 16 rounds. Count refreshes are independent of SS availability; run the count
handler before geo when both are requested.

The destination runtime role needs `SELECT, INSERT` on both new tables and
`SELECT` on the view for map readers. The SS role needs only source `SELECT`;
queries additionally set `readonly=1` and resource limits. The example systemd
unit runs all three handlers hourly. Source credentials and private infrastructure
hostnames must not be committed to this repository. The public TON Center API URL
is shown directly; internal database and service addresses use placeholders.

## Charts and map queries

```sql
SELECT toDateTime(utime_since, 'UTC') AS round_start,
       toDateTime(utime_until, 'UTC') AS round_end,
       total, config34_hash, anchor_mc_seqno, is_partial
FROM gramstat.validator_rounds FINAL
ORDER BY utime_since;

SELECT utime_since, total, located, unknown, country_known, countries,
       breakdown, status, is_partial, data_through
FROM gramstat.validator_geo_rounds FINAL
ORDER BY utime_since;

SELECT pubkey, country, city, latitude, longitude, observed_at
FROM gramstat.validator_geo_members
WHERE utime_since = {round:UInt32} AND status = 'located';

-- Cluster overlapping points; a single coordinate can represent many validators.
SELECT latitude, longitude, count() AS validators
FROM gramstat.validator_geo_members
WHERE utime_since = {round:UInt32} AND status = 'located'
GROUP BY latitude, longitude;
```

On the read-only pilot of 14 rounds overlapping 21–30 October 2026, PostgreSQL
and SS rosters matched for all 5,314 member-round records. SS had in-round
observations and coordinates for 5,296 of them; 18 had no observation. The count
query took 24.5 ms server time and the location query with diagnostics took
15.7 s / approximately 96 MB memory (two threads). These are individual cached
measurements, not an SLA or proof of all historical coverage.

## Checks

```bash
GRAMSTAT_TEST_PG_DSN='host=localhost dbname=postgres user=...' \
  go test -race ./internal/gramstat/... ./cmd/gramstat-aggregate
go vet ./internal/gramstat/... ./cmd/gramstat-aggregate
```

Local integration tests exercise the actual PostgreSQL selection and embedded
ClickHouse SQL/DDL, nullable latest observations, exact weights, round boundaries,
source lag and replacement of removed points. Neither test uses production writes.
