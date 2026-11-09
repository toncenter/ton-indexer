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

Parseable quantities must pass the payment-price filter below before contributing
to the known subtotal. The [payment/refund audit](gramstat-stars-payment-audit.md)
documents spoofed quantities and protocol bounces. `known` still does not confirm
off-chain delivery of Stars.

The accepted prefix is `N Telegram Stars` or `N Prepaid Telegram Stars`, with
whitespace after the quantity and a word boundary after `Stars`. `N` is a positive
integer fitting `UInt64`. Observed integral decimal notation such as `50.0` or
`50.000` is normalized to 50; leading zeroes are normalized too.

Prepaid comments retain `product='prepaid'`; other Stars comments use `standard`.
An unparsable quantity does not remove the payment from the purchase count:

| `quantity_status` | Meaning |
| --- | --- |
| `known` | Positive integral size, with a known payment amount passing the price floor |
| `missing_or_unrecognized_quantity` | Missing number, fractional size, separators or unsupported comment format |
| `zero_quantity` | A comment explicitly says zero; not a valid purchase size |
| `out_of_range` | Quantity exceeds `UInt64` |
| `missing_payment_amount` | Payment amount unavailable; even a parseable claim is excluded from the known subtotal |

Unknown sizes are `NULL`, never zero. They are grouped by payment asset, product
and reason. No quantity is guessed from the TON/USDT payment amount. A raw `50`
and `50.0` belong to the same bucket. The exact parser is in
[daily.sql](../ton-index-go/internal/gramstat/handlers/actions/daily.sql).

## Payment-price filter (v2)

- **GRAM:** compute `paid / declared Stars` for positive, known-size payments in
  the same calendar **UTC hour**, weighted by actual paid nanograms (including
  payment multiplicity). Standard/prepaid share the reference. Choose the first
  ascending price whose cumulative monetary weight reaches half the total (lower
  median at an exact tie). Admit `paid >= declared Stars * reference / 2`.
- **USDT:** use a fixed price of **0.015 USDT/Star** and an **80%** floor:
  `paid_micro_usdt >= declared Stars * 12000`. No hourly reference or dollar/GRAM
  exchange rate is involved. This assumes the fixed historical dollar price
  agreed for this dataset; it is not inferred from today's GRAM quote.
- GRAM comes from **`actions.value`**, USDT from **`actions.amount`**.
- Known zero payments are rejected, including zero-paid unknown-size comments.
  Missing amounts remain unknown. Positive payments with unrecognized quantities
  remain unknown-size purchases, never contribute to the median or known subtotal,
  and are **not** claimed to have passed a price check.
- No minimum liquidity or cross-hour fallback is imposed. A single positive
  known-size GRAM payment can set its hour's price. This rule makes cheap spoofing
  expensive in busy hours, but does not establish independent validity in sparse
  hours. Arbitrary median capture costs roughly the other payments' monetary weight.
- The current hour is provisional within the partial current-day snapshot; a
  later refresh recalculates it. Late-completing traces are included through the
  snapshot's source MC; requested start/end times apply to action start time.

SQL groups identical `(hour, asset, product, quantity/status, paid)` payments before
transport. Go sorts exact rational prices and compares cross-products using big
integers: no floats, rounding or inferred Stars quantities. Group weights are
`paid * purchases`, not Stars quantities or payment counts.

`rejected_purchases` counts only successful labelled payments excluded by this
filter (underpriced or zero-paid). Failed on-chain actions are excluded earlier
and are not counted as rejected. `purchases + rejected_purchases` is the original
successful labelled payment count. Retained unknowns are included in purchases.

## Storage and refresh

`gramstat.telegram_stars_daily` stores **one complete snapshot per day**:

- `purchases`: total payment-action count, including unknown sizes.
- `unknown_purchases`: number of payments with an unknown/invalid quantity.
- `rejected_purchases`: successful labelled payments excluded by the price filter.
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

The existing `actions` SQL adds grouped Stars payments while retaining **one scan
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

Snapshots produced by this filter carry `calc_version='stars-v2-hourly-gram50-usdt80'`.
The explicit-date `stars` handler rebuilds only Stars distributions and the Stars
purchase metric. It traverses Fragment's `(account, trace_end_utime, ...)` index
and joins actions by their complete primary key, rather than scanning unrelated
actions. Matching both stored LT fields prevents stale account-index entries from
duplicating corrected actions. It intentionally does not cap trace end time at
the action window's end, so late traces remain included. For a whole-history
rebuild, use one range to avoid rereading the late-trace tail for many small chunks.

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

# Stars-only history: already indexed actions; no indexer replay.
gramstat-aggregate --handlers stars --from 2024-07-12 --to 2026-11-10 \
  --actions-chunk-days 3660 --actions-statement-timeout 4h --timeout 5h
```

Before the v2 price filter, the integrated read-only probe reproduced 224,953 payments / 45,021,726 known
Stars for 30 October 2026, and 457,394 payments / 55,151,570 known Stars for
31 October through 13:00 UTC. Unknown quantities were 1,521 and 3,693 respectively;
the full Stars total is therefore NULL for both snapshots. Existing daily metrics
matched their previous results. Local PostgreSQL integration tests cover parsing,
filters, nested/batched payments, late traces and one source scan; ClickHouse-local
tests exercise exact integers and replacement/removal of buckets.

V2 validation on 2026-11-09 compared the ordinary MC-index path with the targeted
account-index path for November 8: identical 136,928 raw payments and 28,991
hour/price groups; 32,779,343 known Stars, 987 unknown purchases and no rejections.
All other daily Activity fields matched the previous SQL. Measured SQL times were
6.814s (old), 4.492s (new regular), and 33.730s (targeted with late-trace tail);
these are individual warm-cache observations, not a general speedup claim.

For the August 23, 2025 anomaly, the same bounded MC interval used by the audit
reproduced all 47,587 raw payments and the old known subtotal exactly. The hourly
filter retained **47,420 payments / 18,327,439 known Stars**, with 96 unknowns;
167 payments were rejected (164 underpriced known quantities, 3 zero-paid unknown
quantities). Exact Python `Fraction` and the production Go filter agreed on both
days. Local integration tests also cover late traces, stale account-index entries,
amount-field selection, inclusive thresholds, partial periods and one raw scan.
