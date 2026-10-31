# Telegram Stars purchases through Fragment

The hourly `actions` handler publishes a daily purchase-size distribution from
already classified PostgreSQL actions. The x-axis is the UTC day; a series can
represent one exact package size (50, 75, 100, 150, etc.), or a chart can group
sizes into ranges without another source scan. Both **purchase counts** and
**Stars quantities** are available, with TON/USDT and standard/prepaid splits.

## Definition and source

A purchase is one successful payment action addressed to mainnet Fragment:

`0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A`

Supported payments:

- `ton_transfer`: comment from `(ton_transfer_data).content`.
- `jetton_transfer`: comment from `(jetton_transfer_data).comment`, with the
  **USDT master** in `asset`:
  `0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE`.

The comment must contain the case-sensitive phrase `Telegram Stars`. Other
Fragment payments, failed actions, other recipients and other jettons are excluded.
Source action timestamps (`start_utime`) determine the day. Child actions are
included. Multiple payments from one batch or wallet count separately; individual
blockchain transactions and sender addresses are not the counting unit.

These are indexed on-chain **payments labelled as Stars purchases**, not receipts
confirming off-chain delivery. Coverage is limited to this recipient and these
payment assets; it does not claim to include every Stars seller, app-store purchase
or historical Fragment address. Comments without a Stars label cannot be attributed
and are excluded. No buyer identity is inferred from the sender wallet.

The collector calls **no external API**. The recipient and token are pinned
constants; explorer metadata was consulted only to verify the addresses during
development. PostgreSQL is the source and ClickHouse is the destination.

## Quantity parsing

The accepted prefix is `N Telegram Stars` or `N Prepaid Telegram Stars`, with
whitespace after the quantity and a word boundary after `Stars`. `N` is a positive
integer fitting `UInt64`. Observed integral decimal notation such as `50.0` or
`50.000` is normalized to 50; leading zeroes are normalized too.

Prepaid comments retain `product='prepaid'`; other Stars comments use `standard`.
An unparsable quantity does not remove the payment from the purchase count:

| `quantity_status` | Meaning |
| --- | --- |
| `known` | Positive integral size available |
| `missing_or_unrecognized_quantity` | Missing number, fractional size, separators or unsupported comment format |
| `zero_quantity` | A comment explicitly says zero; not a valid purchase size |
| `out_of_range` | Quantity exceeds `UInt64` |

Unknown sizes are `NULL`, never zero. They are grouped by payment asset, product
and reason. No quantity is guessed from the TON/USDT payment amount. A raw `50`
and `50.0` belong to the same bucket. The exact parser is in
[daily.sql](../ton-index-go/internal/gramstat/handlers/actions/daily.sql).

## Storage and refresh

`gramstat.telegram_stars_daily` stores **one complete snapshot per day**:

- `purchases`: total payment-action count, including unknown sizes.
- `unknown_purchases`: number of payments with an unknown/invalid quantity.
- `known_stars_total`: exact sum over known quantities only (`Decimal(38,0)`).
- `stars_total`: full quantity when every size is known, otherwise `NULL`.
- `buckets`: `(payment_asset, product, stars_per_purchase, quantity_status,
  purchases, stars_total)` tuples. Each known bucket's quantity sum is
  `stars_per_purchase * purchases`; unknown buckets have a NULL sum.
- `calculated_at`, `data_through`, `source_mc_seqno`, `is_partial`, `calc_version`.

An empty covered day has count/sum zero and an empty bucket array. Read the table
with `FINAL`. `ReplacingMergeTree(calculated_at)` replaces the entire array, so
removed sizes, currencies or unknown buckets cannot remain as stale chart entries.
The view `gramstat.telegram_stars_buckets` expands only the latest snapshots.

The existing `actions` SQL adds a Stars grouping set while retaining **one scan
through the actions MC index**. It materializes parsed comments to evaluate the
regex once per candidate, not once per aggregation expression. All existing
metrics are collected in the same read-only, repeatable-read transaction.

The handler writes the full distribution first, then publishes
`telegram_stars_purchases` in `gramstat.daily_metrics` as a completion marker.
The two tables are not a cross-table transaction: use the Stars snapshot's own
totals and timestamps for charts. Retries repair interrupted writes. The ordinary
hourly refresh covers today plus three preceding days and fills missing days in
established actions history. Older source corrections require explicit dates.
The current day and source-lagged days retain `is_partial=1`.

## Graph queries

```sql
-- Daily totals, including known subtotal and explicit quantity coverage.
SELECT day, purchases, unknown_purchases, stars_total, known_stars_total,
       is_partial, data_through
FROM gramstat.telegram_stars_daily FINAL
ORDER BY day;

-- Daily series by exact size, combining the payment assets and products.
-- A NULL size is the "unknown size" series; its Stars sum stays NULL.
SELECT day, stars_per_purchase, sum(purchases) AS purchases,
       if(countIf(stars_total IS NULL) > 0, NULL, sum(stars_total)) AS stars_quantity
FROM gramstat.telegram_stars_buckets
GROUP BY day, stars_per_purchase
ORDER BY day, stars_per_purchase;

-- Detailed split, suitable for filters or stacked charts.
SELECT day, payment_asset, product, stars_per_purchase, quantity_status,
       purchases, stars_total
FROM gramstat.telegram_stars_buckets
ORDER BY day, payment_asset, product, stars_per_purchase;
```

All counts and quantities are exact integers. Transport monetary/quantity sums
as decimal strings to clients that cannot represent integers above 2^53 exactly.

## Deployment and verification

Apply the new DDL from `gramstat-aggregate --print-schema` before replacing the
binary. The runtime role needs `SELECT, INSERT` on `telegram_stars_daily` and the
existing `daily_metrics`; chart readers need `SELECT` on the table/view.
There are no new environment variables, API keys or PostgreSQL migrations.

```bash
gramstat-aggregate --handlers actions --from 2026-10-30 --to 2026-11-01
```

The integrated read-only probe reproduced 224,953 payments / 45,021,726 known
Stars for 30 October 2026, and 457,394 payments / 55,151,570 known Stars for
31 October through 13:00 UTC. Unknown quantities were 1,521 and 3,693 respectively;
the full Stars total is therefore NULL for both snapshots. Existing daily metrics
matched their previous results. Local PostgreSQL integration tests cover parsing,
filters, nested/batched payments, late traces and one source scan; ClickHouse-local
tests exercise exact integers and replacement/removal of buckets.
