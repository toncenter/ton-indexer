# Prepared wallet charts

`gramstat-aggregate --handlers wallets` prepares three charts:

* **Activated wallets:** cumulative first qualifying deployments, sampled at each
  including MC; hourly and daily checkpoints accelerate long ranges.
* **Activations per day:** first qualifying deployments by transaction UTC day.
* **Active wallets:** fixed Hourly / Daily / Monthly tabs. Hourly and Daily count
  aligned UTC buckets. Monthly is one exact union per UTC calendar month,
  covering [month start, next month start). Daily is the default.
  A wider window is a union, never a sum of smaller counts. Stored minute states
  remain an internal preparation detail.

After daily preparation, the same wallet handler fills missing `wallet_months`
rows from published daily `wallet_activity` states. Each row records all
contributing publication UUIDs (28–31). Only months with every day certified are
published; partial initial/current months and months spanning holes remain missing.
Certified empty windows produce zero. Source epochs make certified days immutable,
so retries may safely repeat an exact result. API reads scalar counts, never states.
The ordinary scheduled handler and manual catch-up both resume missing months.
Monthly preparation uses one thread and an 8 GiB query memory cap for exact
calendar-month unions, including large aggregate-state storage granules. API reads remain
small scalar queries with their existing read-only limits.
Apply the `wallet_months` DDL in `schema.sql` and grant SELECT/INSERT to the
aggregator and SELECT to the API before deploying this version.

Definitions are those in [gramstat.md](gramstat.md#deployments-and-wallet-charts).
An earlier non-wallet deployment does not exclude a later qualifying deployment.
Eligibility begins at the first qualifying deployment LT and survives subsequent
code changes, destruction and redeployment. Actual non-bounced outgoing messages,
including external-out, qualify irrespective of incoming message type/opcode.

The frozen registry is `fc96280e446129a605659a022311bba1d0d6bef9a03960be77c86a259dc876f9`:
19 hashes / 18 types, V1–V5, highload and the agreed WalletTg trampoline hash.
Preprocessed V2, vesting, x1000, multisig and jetton wallets are excluded. Updating
the general-purpose detector does not silently change this registry. A registry
change needs a new digest and regenerated dependent results.

## Preparation

Apply the wallet DDL included in `--print-schema` with an administrative account.
The aggregation role needs SELECT on the raw and metrics sources and SELECT/INSERT
on the prepared tables. The API role needs only SELECT on prepared tables.

```sh
export GRAMSTAT_WALLETS_SOURCE_ID=mainnet-wallets-v1
# Optional, defaults to gramstat (use an isolated database for benchmarks):
export GRAMSTAT_WALLETS_SINK_DB=gramstat
gramstat-aggregate --handlers wallets --from 2024-10-13 --to 2024-10-14
```

Explicit dates are exclusive at the upper end and must be complete UTC days.
Without dates, only `--lookback-days` closed days (at least one) near the source's
transaction-time horizon are prepared. It does not automatically backfill all
activity history. Completed days are skipped on subsequent runs.

### Following the native indexer

Set `GRAMSTAT_WALLETS_FOLLOW_NATIVE=1` for a verified source epoch using the native
delta-last writer. Before each run, the handler takes the latest delta MC minus
a 1,000-MC settling margin, checks the complete MC/delta prefix, and matches every
shard tip at that MC to its block. Only then does it advance `wallet_sources` to
the minimum tip timestamp. If parallel batches leave gaps near the high-water
mark, a bounded binary search caps advancement at a proven contiguous prefix.
Missing tips, a damaged previously verified prefix or a regressing horizon fail
without publishing the proposed watermark. The original source ID and proof are retained.

In this mode, running without dates enumerates **every missing closed UTC day**,
oldest first, rather than just the last few days or days after a maximum date.
Thus indexer jumps and scheduler downtime cannot leave permanent holes. Explicit
date requests still select only their requested window. Cumulative activations
and calendar-month unions retain their closed-day publication contract.

### Current-day Active wallets

Without explicit dates, the wallet handler also publishes hourly and daily
active-wallet counts for `[UTC midnight, complete_before)`. It uses the same
verified source prefix and minimum shard-tip horizon, not wall-clock time.
The last incomplete hour/day is marked partial by the API; no future zero
buckets are generated. Counts are exact distinct-address unions, with the same
first qualifying deployment LT and non-bounced outgoing-message conditions.

`live_counts.sql` reads LT-bounded current-day messages and resolves first
deployments for candidate senders directly from `wallet_first`, avoiding a new
full scalar lookup snapshot every five minutes. It writes scalar counts under
a fresh run UUID, then commits `wallet_live_days` last. Interrupted inserts stay
invisible; retries use a new UUID. A source endpoint already committed is skipped.
This separate marker cannot cause the closed-day preparation or complete-month
union to accept a partial day. Later complete-day publications supersede live
activity by certified cutoff, not merely insert time. Apply the added
`wallet_live_days` schema and grant SELECT/INSERT to the writer and SELECT to
the API before upgrading.

November 12 check: a bounded closed-hour comparison matched all 16,333 active
wallets exactly. The live current-day query took 9.86 s for roughly 12.5 hours.
`EXPLAIN indexes=1` confirmed the message LT range; the candidate-address filter
still touches most `wallet_first` granules because senders are distributed
throughout the account keyspace. No lifetime lookup is copied or raw history replayed.

Wallets runs every five minutes in the **existing** `gramstat-aggregate.service` Go scheduler,
after the other handlers, under the same user, environment and `flock` lock.
Add `GRAMSTAT_WALLETS_SOURCE_ID=mainnet-wallets-v1` and
`GRAMSTAT_WALLETS_FOLLOW_NATIVE=1` to `/etc/gramstat-aggregate.env`.
There is no separate Wallets scheduler. The ordinary per-handler time budget is
resumable: already published days are skipped on the next scheduled run. An initial
large backlog may take several runs; other handlers still run first each hour.

First-deployment preparation does require the entire preceding deployment history
on the first run. It then reads only MCs after its persisted progress. The first
pass streams singleton aggregate states with bounded insert buffers instead of
holding every address in one global hash table. Background merges and ordered
`minMerge` reads resolve the minima. The states make repeated/partially acknowledged inserts idempotent; no raw
`FINAL` is needed for these minima or for grouped sending addresses.

Before processing days, the handler publishes one immutable scalar lookup for
the selected MC endpoint. It resolves the first-deployment states once, then
prepares sparse per-MC cumulative counts and daily activation counts. Subsequent
days reuse this snapshot; interrupted builds cannot expose partial lookups.

Each day processes messages in one LT-bounded scan across all shards,
with a 2 GiB query memory limit and external aggregation above 300 MB. A compact
distinct-sender subquery restricts the lookup. Eligibility is checked on each
actual message; the exact set handles duplicates without constructing a large
sender/minute intermediate. Daily totals no longer rescan all lifetime states.
If a busy day exceeds the memory or execution-time limit, the handler bisects
the interval on minute boundaries and retries smaller LT-bounded windows. Any
states emitted by a failed attempt remain harmless: exact union removes duplicate
addresses. A failure even at one minute leaves the day unpublished. The large
sender set on 2024-08-27 exercised this path during the historical backfill.

`wallet_counts` includes an interval-first `by_interval` projection so long-range
daily reads skip neighboring minute/hour rows while retaining strict API limits.
For a pre-existing table, add this projection and materialize it before serving
full-history reads; new inserts maintain it automatically.
Minute states feed **full address strings** to `uniqExact(String)`, without a
custom 64-bit address hash or approximate sketch. Hour/day states merge minute sets; cached scalar counts
serve ordinary graph requests without reading the sets. First deployments are
joined only against addresses sending in the current chunk.

All MCs of a day, including those without activations, receive the exact cumulative
value. MC timestamps determine the cumulative chart's x coordinate; `tx.now`
determines Activations per day. Several MCs in one second remain distinct.

## Source completeness contract

`wallet_sources` is a source-owned manifest, **not a progress estimate inferred
from the greatest deployment/message timestamp**. Before running the handler,
the indexing/validation operator records:

| Field | Meaning |
| --- | --- |
| `source_id` | Immutable source epoch, changed on historical corrections |
| `raw_database`, `metrics_database` | Consistent raw blocks/messages and deployment/delta sources |
| `first_mc`, `through_mc` | Persisted, contiguous source prefix, inclusive |
| `complete_from`, `complete_before` | Fully collected transaction-time range `[from,to)` in Unix seconds, across all shards |
| `history_verified` | 1 only when qualifying deployment history before every displayed event is proven complete, including the boundary before `first_mc` |
| `proof_id` | Reference to the retained source validation evidence |

The source publisher must acknowledge **both deployments and messages** before
advancing this manifest, and prove that no later-included shard block can add a
transaction before `complete_before`. Account-delta continuity alone cannot prove
that sparse message/deployment writers have finished. The handler independently
checks one deduplicated MC block and account delta for every MC in the manifest.
For the native ClickHouse writer, `InsertBatchClickhouse::start_up` inserts
transactions, messages, blocks and deployments **before** account deltas. Since
commit `b4a0685` (before the full-history run), those deltas are explicitly the
completion marker, including MCs without deployments. With this writer lineage
verified, a continuous deduplicated delta prefix proves completion of these
earlier writes. The safe transaction-time horizon is the minimum timestamp of
all shard tips in `shard_state` at that endpoint, matched against `blocks`, not
the masterchain timestamp alone.

Manual snapshots require an external manifest publisher. Native following updates
the watermark inside the handler, but never promotes an unverified epoch to a
verified one.

`history_verified=0` permits isolated diagnostics and benchmarking, but the public
reader SQL excludes these results. It does not manufacture historical zeros.
If qualifying deployments precede `first_mc`, verified first-deployment states
must be seeded before the first run; otherwise verification must remain false.
Within an epoch, completed history and closed days are immutable. Corrections or
changed beginning-of-history evidence use a new source ID and explicit rebuild.

## Publication and readers

Each day has a random `run_id`. All minute states, rollups, counts, MC points and
checkpoints are inserted before the final `wallet_days` publication marker.
An interrupted day is invisible; retry uses a new run. Readers choose one complete
run per day by `(through_mc, calculated_at, run_id)` in the same query. They must
not read all physical rows or sum different revisions.

Embedded reader SQL:

* `activity_read.sql`: `step=60`, `3600` or `86400`, aligned `[from,to)` bounds;
  returns cached exact counts. Empty intervals inside a completed day have genuine
  zero rows. Missing/unverified days have no rows.
* `activated_read.sql`: `interval=0` for each MC, otherwise aligned seconds.
  Below an hour it groups MC values; hour multiples below one day use hourly
  checkpoints, and day multiples use daily checkpoints. Each point retains the
  last actual MC/time of its interval. Partial boundary intervals automatically
  read MC points at those edges. Intervals not divisible by the checkpoint step
  also fall back to MC points, preserving exact endpoints.
* `activations_read.sql`: daily activations from the selected `wallet_days` run;
  `opening_activated` is the cumulative count before that day's first MC.

Readers bind `sink`, `source`, `registry`, `from`, `to` and `step`/`interval`.
The API should select a bucket width against its point budget and disclose it.
Zooming changes the activity window and therefore the values; this is not
interpolation of a fixed DAU series. Unknown coverage stays unknown.

For lifecycle management, reclaim abandoned run IDs only after a grace period
longer than the maximum job/read duration, and never remove the selected published
run. Old source epochs should be retained through reader/cache cutover. No
automatic destructive cleanup is performed by the handler.

## Verification

`go test ./internal/gramstat/handlers/wallets` runs the actual DDL, handler and
reader SQL with clickhouse-local when installed. It covers retry after partial
preparation, raw duplicate events/messages, redeployment, prior non-wallet code,
sends before deployment, bounced-only/incoming/external-out messages, exact union,
zero versus unavailable days, unpublished/unverified output, and MC/checkpoint
consistency. Live benchmarks must use a separate sink database and bounded days.

### Bounded live benchmark, 2026-11-08

An isolated sink was populated for 2024-10-13 UTC, using the complete preceding
deployment prefix and an MC endpoint beyond midnight to include late shard
transactions. This was a performance/consistency experiment, not certification
of beginning-of-history coverage; its publication markers remain unverified.

| Reader | Points | ClickHouse elapsed | HTTP elapsed |
| --- | ---: | ---: | ---: |
| Active wallets, minute | 1,440 | 5.7–6.8 ms | 10.5–14.5 ms |
| Active wallets, hour | 24 | 6.3 ms | 8.9–9.0 ms |
| Active wallets, day | 1 | 5.9–7.2 ms | 8.5–10.4 ms |
| Activated wallets, every MC | 27,804 | 16.9 ms | 52.7 ms |
| Activated wallets, hourly checkpoints | 24 | 11.5 ms | 14.6 ms |
| Activated wallets, daily checkpoint | 1 | 11.1 ms | 14.2 ms |

The final six-hour-chunk implementation prepared the day in 62.6 seconds with
the lifetime deployment states already populated, versus 144.6 seconds with
one-hour chunks. A repeat invocation skipped the completed day in 0.25 seconds.
These preparation times are separate from interactive reader latency. Its
output again matched every raw activity point. Daily activations independently
matched the raw first-deployment query (91,844 in this bounded experiment):
3.2 ms for the prepared reader versus 21.2 seconds for the raw computation.

All 1,440 minute counts, 24 hour counts, the daily distinct and final cumulative
MC value matched independently grouped raw queries. The raw activity queries
took 6.2–6.5 seconds and read approximately 14.1 GB each; the prepared activity
reader read 1,467 rows / 176 KB including publication metadata. The daily
activated checkpoint reader read three rows, rather than all MCs.

In a separate 80-query run with eight simultaneous clients while aggregation
was running, minute-series HTTP p50 was 12.3 ms, p95 22.0 ms, maximum 29.0 ms,
with no errors. These are database HTTP measurements on one historical day with
existing server caches, not browser latency or a full-history load test.

The boundary check found 112 message rows timestamped on that UTC day but included
in the first MC of the following day. Ending the source at the day's last MC lost
six distinct active wallets. The source manifest's transaction-time horizon,
rather than MC calendar date, is therefore essential. A regression fixture covers
this distinction for both daily activity and daily versus cumulative activations.

### Preparation optimization

Query-log profiling of the original 62.6-second run found 37.9 seconds in two
full lifetime-state aggregations and 23.6 seconds in four activity joins; the
rollups themselves needed less than a second. The immutable scalar lookup and
cached activation totals remove that repeated lifetime work. Building the lookup
and both activation summaries took 28.5 seconds **once** for the whole prefix.
With the snapshot ready, the same 2024-10-13 day took **7.44 seconds**. Three
additional days (October 10–12) took **23.15 seconds total**. All 1,440 minute
counts, hour/day unions and the final cumulative MC value still matched the
independent raw queries. Interactive readers and their schema are unchanged.

### Beginning-of-history validation

Archived `getBlockTransactionsExt` responses for mainnet MC 1 and 2 were decoded
with `@ton/core`, and each transaction cell hash checked against the returned
transaction ID. MC 1 contains nine transactions; MC 2 contains eleven. All are
`active → active` or `nonexist → nonexist`: neither block contains a deployment.
The [verified MC 2 account baseline](accounts-baseline.json) proves that the
basechain is still its empty zerostate. Thus no qualifying wallet deployments
are omitted before the native full-history run's MC 3 boundary.

Block root hashes (base64):

* MC 1: `8GYhhrigd8CwZGrRT59iulLDcgiTYuvOAzFJxugc0Ts=`
* MC 2: `4bzgnFItQjTVEMYL9c/VHshMJttG9gDIXCzsMQdjKSU=`

This validation concerns the **current first qualifying deployment definition**,
not the older first-ever-active or execution-code definitions.
