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

The embedded [identity registry](../ton-index-go/internal/gramstat/handlers/validators/locked_affiliations.json)
contains 98 Tonstakers controllers, 24 Stakee controllers, 952 Hipo loan contracts
and 2 official wsTON proxies. It was rebuilt from **1,284 historical account-state
checks for 1,118 holders**, covering each holder's first and last observed
participation (one check for a one-round loan). Every response matched the
requested verified MC seqno, root hash and file hash. The code cell hash and
static data layout were checked before associating a controller with its pool
or a loan with its treasury. No checked holder changed parent pool or code hash
between its two observations.

The old restriction to round `1792233224` was an implementation error: it made
all named protocols appear only around the date of the original one-off audit.
Affiliation now follows the verified **holder address and code family**, not
that audit's date. The reviewed standard controller stores the pool in static
identity data; the Hipo loan stores an immutable treasury. A different source
contract family cannot inherit a cached affiliation.

| Protocol | Parent pool / treasury | Evidence |
| --- | --- | --- |
| Tonstakers | `EQCkWxfyhAkim3g2DjKQQg8T5P4g-Q1-K_jErGcDJZ4i-vqR` | Controller static data matches the [official pool](https://tonstakers.com/faq/en/article/official-tonstakers-links) |
| Stakee | `EQD2_4d91M4TVbEBVyBF8J1UwpMJc361LKVCz6bBlffMW05o` | Same static layout matches the [Stakee pool](https://stakee.com/) |
| Hipo | `EQCLyZHP4Xe8fpchQz76O-_RmUhaVc_9BAoGyJrwJrcbz2eZ` | Loan data matches the [official treasury](https://github.com/HipoFinance/contract) |
| Whales wsTON | `EQB0SoxuGDx5qjVt0P_bPICFeWdFLBmVopHhjgfs0q-wsTON` | Two proxies from the [official liquid-pool page](https://tonwhales.com/staking/liquid) |

Reviewed on-chain holder code hashes:

| Family | Code cell hash, hex |
| --- | --- |
| Standard liquid-staking controller | `dd3ce98db487c7585803933bffba7a57eb4e663099059d08b83db0b4ce060793` |
| Hipo loan | `56fb96fc4b9051deecfce8b04ce3c888990ba80fe6bd07154e351506ee9907a0` |
| Whales proxy | `6097b64a3b9db526a6b26497afeae3f224a8f639d37b6534d46df21fe0589c21` |

Schemas: [standard controller and immutable identity](https://github.com/ton-blockchain/liquid-staking-contract/blob/1f4e9badbed52a4cf80cc58e4bb36ed375c6c8e7/docs/controller.md),
[controller storage](https://github.com/ton-blockchain/liquid-staking-contract/blob/1f4e9badbed52a4cf80cc58e4bb36ed375c6c8e7/contracts/controller.func),
[Hipo loan storage](https://github.com/HipoFinance/contract/blob/85e7777861f20c3b4e1a0a564943375eaa72a76d/contracts/loan.fc).
These are trusted historical RPC checks, not independently verified Merkle proofs.
The registry records original response digests and MC identities. The generator
`go run ./cmd/gramstat-stake-registry --capture capture.jsonl --registry ... --ui ...`
rechecks the captured states and produces both the embedded registry and the
browser module from the same records.

Twenty Bemo-labelled holders and 22 other Whales proxies remain unconfirmed;
their elected positions still qualify for the corresponding `-like` family.

After the identity correction, all 3,335 prepared snapshots were reconciled:
no rows disappeared and **Total, Telegram, pool subtotal and Others were
unchanged in every row**. Named Tonstakers now appears in 983 classified points,
Stakee in 980, Hipo in 973 and Whales wsTON in 984, beginning in the available
September 2024 attribution history. No Tonstakers-like or Hipo-like amounts
remain in this checked history; Bemo-like and the other Whales-like positions
remain unconfirmed. The former simultaneous October 17 naming transition is gone.

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
New standard controllers and Hipo loans are checked automatically at their
verified round's MC anchor. Successful identity observations are cached atomically
in `--stake-affiliation-cache` (default
`/var/lib/gramstat-aggregate/stake-affiliations.json`). Missing RPC data or an
unreviewed code hash keeps the affiliation unconfirmed. The API calculation
contract remains `stake-locked-v1`; this correction changes names within the
pool subtotal, not the selected-principal or Telegram calculations.

Pilot: the recent pair reconciled 724 positions exactly to
`1293534592018375276` nanograms. Broad pool types contributed
`297491676296108533` (Tonstakers-like `171065375589688854`, Whales-like
`117619488977401504`, Hipo-like `8806811729018175`). The three-round pilot
initially took 2.9 s; the once-per-run Telegram-history read took 1.4 s.
The exact-record ISP query uses the leading primary-key `record_id` column.

### Completed historical aggregation, 2026-11-10

Release `eb8a393` processed the indexed history in **6 min 1.087 s**, without
an indexer replay. The independent hourly timer is enabled; an actual automatic
window run completed successfully in **7.508 s**.

* 3,335 round-axis snapshots; 3,123 have a verified two-round total.
* 984 have category sums: 526 complete source coverage, 458 partial attribution.
* All 413 round-axis points starting in 2026 have category sums.
* First available two-round total: **2020-04-24 16:06:12 UTC**.
* First category split: **2024-09-29 12:58:48 UTC**.
* Zero category overlaps or discrepancies in the pair, category, individual
  position and family-breakdown sums.

Missing total reasons: 196 snapshots lack selected stake, 14 lack an adjacent
interval (including the initial point), and 2 involve an unverified round.
Another 2,139 snapshots have total principal but no defensible complete-pair
attribution. Most precede historical ISP coverage. Later source holes include
missing type rosters in February/October/November 2025 and missing ISP coverage
in August–September 2025. These remain NULL category sums, not zero balances.

Latest checked pair, current round **2026-11-09 22:54:00 UTC**:

| Group | Exact GRAM |
| --- | ---: |
| Total | 1,293,534,592.018375276 |
| Telegram | 217,254,026.640880000 |
| Pool-contract group | 297,491,676.296108533 |
| Others | 778,788,889.081386743 |

All 724 positions have reconciled type records; 720 have usable ISP evidence,
including historical Telegram evidence. The snapshot is `partial_attribution`.
The complete per-position audit is stored with the aggregate, including full
holder addresses, implementation types, groups and evidence timestamps.

## Chart display and approved text

The UI displays each `pool_breakdown` entry as its own stepped area, legend
control and exact tooltip row. Confirmed protocol names and their unconfirmed
`-like` categories remain distinct. The pool subtotal is not an additional layer.
The independent Total line remains available when category attribution is missing.
Category labels describe the dated evidence in the aggregate; they do not prove
that a contract without a confirmed affiliation is unofficial.

> Stakes remain locked after a validation round ends, so validators need separate funds for consecutive rounds. To show the **total capital committed to staking**, this chart adds the selected stakes of the current and previous rounds, treating each stake as locked for two full rounds.

> Liquid Staking is broken down by protocol and contract type. Protocol names indicate confirmed affiliation; the “-like” suffix indicates a recognized staking contract type whose protocol affiliation has not *yet* been confirmed. Amounts include validators’ own collateral.

> Others is the total stake selected by the Elector across both rounds, minus the amounts attributed to Telegram and Liquid Staking.

The **Verified staking contracts** button opens an in-page registry, like Wallet
codes, with pool/treasury addresses, checked holder code hashes and expandable
holder addresses. There is no external Markdown link. The wider Whales
family includes conventional pools. Historical `bemo_nominator` positions are
displayed as Bemo-like unless a dated protocol affiliation has been confirmed.
