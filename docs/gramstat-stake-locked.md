# Total Stake Locked: two-round selected principal

`validator_stake_locked` prepares `gramstat.validator_stake_locked_rounds`.
For a round n, total is the exact selected principal of n plus n−1. These
must be two adjacent, verified intervals, not twice the current amount or
the two nearest available observations. Missing source rounds stay missing.
Amounts are integer nanograms (`Decimal(38,0)`); 1 GRAM = 1,000,000,000 nanograms.

This is an explicit **two-full-round accounting convention**. It does not
claim that every unit is still frozen in the Elector at every instant:
config #15 has a separate post-round holding time. Rewards, pending election
applications, pool cash, token supply and loan receivables are not added.

## Sources and categories

Selected principal, holder address and pubkey come from PostgreSQL
`validator_cycles`, `validator_cycle_members` and
`validator_election_participants`. Every member set must match the #34-verified
roster digest and sum exactly to the round's selected total. Pool contracts
with multiple validator keys contribute each selected position once;
the same address in two distinct rounds contributes both distinct positions.

The classification sources are separate from principal:

* **Telegram**: historical `node_addresses.geoip_isp = 'Telegram Messenger'`
  by ADNL. The exact observation chosen by `validator_geo` is read using its
  primary-key `record_id`; NULL or conflicting records stay unknown. An
  established historical Telegram observation may fill a telemetry gap only
  after its first timestamp and before any conflicting ISP observation.
  Classification is bounded by each round's observed `data_through`; the
  round start is the graph's x-axis, not a claim that telemetry was already
  available at that exact activation second. ISP identifies hosting, not the
  ultimate beneficiary of the stake.
* **Pool-contract group**: a matching round-specific Session Stats type,
  reconciled by interval, index, pubkey, ADNL, weight and holder. Official
  affiliation is **not** a condition for inclusion.
* **Others**: the residual selected principal, including unclassified positions.
  `unattributed_nanogram`, type/ISP coverage and individual reasons are retained.

| Source type | Unverified-affiliation label |
| --- | --- |
| `liquid_staking_controller` | Tonstakers-like (the shared Tonstakers/Stakee controller family) |
| `hipo_controller` | Hipo-like |
| `whales_proxy` | Whales-like, including non-official and conventional pools |
| `bemo_nominator` | Bemo-like, not a claim of a verified Bemo parent |

Other wallet/nominator/vesting types are preserved in member records but are
not automatically included in this group. In particular, **not every
whales_proxy is liquid staking**. `pools_nanogram` is deliberately neutral:
it measures selected principal of these implementations, including validator
self-collateral, rather than exclusively LST-customer-funded capital.

Telegram and pool categories must not overlap. An overlap keeps the total
but suppresses all three category totals with `overlapping_attribution`.
If either round has no type or ISP attribution, category totals are NULL,
not zero; the independently verified total can still be present. Individual
missing observations produce `partial_attribution`, with the unidentified
remainder in Others. `is_partial` describes the current time interval.

## Confirmed affiliations and exact addresses

The embedded [address registry](../ton-index-go/internal/gramstat/handlers/validators/locked_affiliations.csv)
contains 24 Tonstakers controllers, 4 Stakee controllers, 2 Hipo loan contracts
and the 2 official wsTON proxies documented in the 2026-10-17 source capture.
Only one wsTON proxy was elected in that captured active round.
This evidence is applied **only to round 1792233224**: a live getter observed
then does not establish historic or future affiliation. Other rounds remain
eligible by family and get `-like` labels until additional dated evidence is
published. A single-nominator contract is not relabelled merely because its
address appears in a registry for a different implementation.

| Protocol | Parent pool / treasury | Evidence |
| --- | --- | --- |
| Tonstakers | `EQCkWxfyhAkim3g2DjKQQg8T5P4g-Q1-K_jErGcDJZ4i-vqR` | `get_validator_controller_data`, stack[12] matches the [official pool](https://tonstakers.com/faq/en/article/official-tonstakers-links) |
| Stakee | `EQD2_4d91M4TVbEBVyBF8J1UwpMJc361LKVCz6bBlffMW05o` | Same getter matches the [Stakee pool](https://stakee.com/) |
| Hipo | `EQCLyZHP4Xe8fpchQz76O-_RmUhaVc_9BAoGyJrwJrcbz2eZ` | `get_loan_state`, stack[1] matches the [official treasury](https://github.com/HipoFinance/contract) |
| Whales wsTON | `EQB0SoxuGDx5qjVt0P_bPICFeWdFLBmVopHhjgfs0q-wsTON` | Two proxies from the [official liquid-pool page](https://tonwhales.com/staking/liquid) |

Saved evidence SHA256: standard-controller mapping
`76488bca9ef545abcd1a3e61e9a6003adc06cf4b634ff1a81de27cf29c10fb0e`;
the two Hipo getter responses
`428e3a848b08076ec8cafc4a5acc447f15f6337ec752ed428acb4e5c11b227fe` and
`182697e0af95ba7f7d0daf5ba65ad21665aa68990e47fcc113b7ee91232af6fc`.
Historical Hipo loans and Bemo-labelled holders do not need official-pool
verification to qualify by family, but their affiliation remains unverified.

## Storage and refresh

Each row atomically contains both rounds' full classified position lists,
category sums, per-protocol/`-like` breakdown, source observation references,
both config hashes and roster digests. Read with `FINAL`. Hash-based writes
avoid replacing unchanged payloads. Raw IP addresses are not copied.

Automatic runs refresh recent rounds and missing history, plus changed
round/geo provenance or round totals. Explicit dates reconcile older changes
to source contract types and unchanged-total member corrections.
SS validator membership starts on 2023-11-22 in the inspected source; earlier
attribution is unavailable, not a zero historical pool balance.

```sh
gramstat-aggregate --handlers validator_stake_locked --from 2026-11-09 --to 2026-11-11
```

Apply `locked_schema.sql` once, grant `SELECT, INSERT` on the new table to the
aggregation role and `SELECT` to the API role. Run after `validators` and
`validator_geo`. The independent `gramstat-stake-locked` service/timer uses
the existing aggregate environment and lock; its binary is deployed separately.

Pilot: the recent pair reconciled 724 positions exactly to
`1293534592018375276` nanograms. Broad pool types contributed
`297491676296108533` (Tonstakers-like `171065375589688854`, Whales-like
`117619488977401504`, Hipo-like `8806811729018175`). The three-round pilot
initially took 2.9 s; the once-per-run Telegram-history read took 1.4 s.
The exact-record ISP query uses the leading primary-key `record_id` column.

## Approved chart text (awaiting UI integration)

> Selected stake across the current and previous validator rounds. Telegram is identified by historical Telegram Messenger ISP observations. Liquid Staking includes positions held by verified Tonstakers and Stakee controllers, Hipo loan contracts, and the official Whales wsTON proxies, including validators’ own collateral. Others includes all remaining positions.

> Also includes elected positions held by contracts of the same staking types whose affiliation with the named protocols has not been confirmed. These are labelled Tonstakers-like, Hipo-like, or Whales-like.

Link text: **Included contracts and attribution rules**. The wider Whales
family includes conventional pools; the grouping name and display of the
additional historical Bemo-like family must remain explicit when integrating
the chart.
