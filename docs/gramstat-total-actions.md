# Total actions

`actions_total_v1` counts successful recognized operations per UTC day. Its
`daily_metrics.breakdown` is an exact additive map of action type to count.
The metric and map are published in one row; replacements include decreases,
removed types and real zeros. No new ClickHouse tables or API grants are needed.

## Counting contract

- Time is `actions.start_utime`. The existing MC-index lower bound and captured
  source-head upper bound retain traces that finish after the requested day.
- `success=true`, with a frozen allowlist in `handlers/actions/types.go`.
- Count a row only when `ancestor_type` has no overlap with that allowlist.
  Accepted ancestors suppress children independently of UI selections. Excluded
  technical wrappers do not suppress useful children. This is not a count of
  traces, messages, transactions, unique users or user intentions.
- Technical calls, contract deployments, tick-tocks, unknown types, validator
  elections, gasless wrappers, NFT discovery, bridge verification/commit packets,
  MEV housekeeping and pool/vault creation are excluded. Unknown future types do
  not enter automatically.
- Ordinary `actions` collection computes type counts from the same materialized
  source as existing Activity. Addresses are retained only for existing distinct
  metrics. Existing transfer/Stars metrics continue to include nested rows.
- An observed day without a type means zero for that type. An absent day remains
  unknown. Current-day totals retain the existing source cutoff and partial flag.

The UI maps each type into exactly one of GRAM transfers, Jetton transfers,
Swaps, Liquidity, Staking, NFT transfers, NFT marketplace, Mint / burn, DNS,
Lending, Bridges, or Other recognized actions. Its corresponding frozen registry
is `gramstat/ui/action-types.mjs`. A taxonomy change needs a new metric version
and consistent rebuild; never mix definitions within a version.

Monthly totals sum exact covered daily counts without another PostgreSQL scan.
Missing historical days leave the month unknown; current months can be partial.
The Total overlay equals the sum of visible categories. Type checkboxes change
that sum without reclassifying or restoring nested actions.

## Initial history and refresh

Recurring `actions` collection includes the new metric automatically. Missing-day
repair starts from this metric's own prepared history floor, rather than causing
repeated whole-source-tail scans when it is first introduced. For initial history:

```sh
gramstat-aggregate --handlers actions_total --from 2019-11-15 --to 2026-11-13 \
  --actions-chunk-days 3660 --actions-work-mem-mb 256 \
  --actions-parallel-workers 2 --actions-statement-timeout 24h --timeout 25h
```

This explicit backfill scans once for day/type counts and writes only
`actions_total_v1`. It does not recalculate existing unique counts, Stars or
amounts. Use a dedicated historical lock with disjoint writer ownership.
Ordinary refresh continues through the shared query every five minutes.

Automatic detection of historical reclassification is deferred. Normal lookback
replaces today and the previous three days; gap repair does not discover changed
values in already populated older days. Use explicit `actions` date ranges to
refresh affected daily metrics and `actions_monthly` for affected monthly uniques.
Reclassification can change both the breakdown and the total number of operations.

## Verification

PostgreSQL fixtures cover accepted ancestors, excluded wrappers, unsuccessful
operations, unknown types, late traces and unchanged legacy transfer counts.
The count-only path matches shared aggregation; EXPLAIN confirms one source scan.
UI tests cover exact sums, selected totals, missing/zero/partial periods, monthly
aggregation and rejected inconsistent breakdowns.

A repeatable-read production comparison on November 9–12, 2026 through MC
105008676 returned the same existing metrics and the same new counts as an
independent type-only query. The shared query took 23.60 s (24.02 s with EXPLAIN
ANALYZE), versus 14.66 s previously. One worker, 64 MiB work_mem; source pages
were cached. Total operations: 2,281,703; 2,331,808; 1,995,453; 1,540,599 (last
day partial). The type-only reference took 10.43 s. These are measured warm-cache
times, not a cold-storage latency guarantee.

### Production rollout, November 12, 2026

Aggregator `d258a1d` publishes the new metric during ordinary five-minute refresh;
two observed runs took 25.68/25.76 s. The initial count-only backfill published
2,551 days (2019-11-15 through 2026-11-08) in 528.31 s. Together with recent
refreshes, 2,555 consecutive daily publications extend through 2026-11-12, with
only that day's row partial. Every stored total equals its exact type-map sum.

The 16 zero days coincide with timestamp gaps between consecutive source MCs:
3295708 at 2020-04-17 23:16:38 UTC → 3295709 at 2020-04-19 06:24:48 UTC, and
8536840 at 2020-12-13 05:45:30 UTC → 8536841 at 2020-12-29 16:52:04 UTC.
They are not holes in the new daily publication. API/UI reconciliation matched
all daily maps and 83 complete calendar months. November 2019 remains unavailable
as a full month because coverage starts on the 15th; November 2026 is partial.

Browser checks covered daily/monthly All time, desktop/mobile, exact stack/Total
alignment, tri-state categories, hiding all types, isolating `renew_dns`, URL
reload, expanded-chart Escape behavior and both color schemes. The full-history
dashboard's gzip GSC1 body measured 679,112 bytes. On the test link its transfer
and queued chart requests exceeded the previous shared 25-second UI deadline;
the range-load budget was extended to 75 seconds while preserving cancellation
on range changes. A full-history browser load then completed in 54.53 s without
API or JavaScript errors. Server preparation and link transfer are distinct costs.
