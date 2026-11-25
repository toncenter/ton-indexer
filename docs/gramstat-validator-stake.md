# Validator stake: three stages per election

`validator_stake` publishes `gramstat.validator_stake_rounds` for the three-line
Stake chart, keyed by the nominal start of a verified validation round:

| Line | Source | Meaning |
| --- | --- | --- |
| Submitted | PostgreSQL `validator_elections.total_stake` | Accepted applications, including unselected candidates and excess capital; only finished, successful elections |
| Selected | PostgreSQL `validator_cycles.total_stake` | Maximum observed selected principal, retained by the indexer's sticky upsert |
| Remaining | ClickHouse `validator_round_results.total_stake` | Principal after fines, immediately before release; rewards are excluded |
| Reward | ClickHouse `validator_round_results.bonuses` | Accumulated election reward before distribution, separate from principal |

These are three stages of the **same election**, joined by its explicit
`election_id`, not simultaneous balances or a sum of overlapping rounds. All
amounts are nullable integer nanoGRAM (`Decimal(38,0)`) and remain exact strings
in Go/JSON. Missing stages stay NULL; a known zero remains zero.
Reward shares the final result's provenance and coverage. It is not net income
after fines or the sum of final per-holder credits (rounding and bans can affect
distribution). The `stake-v2` schema adds nullable `reward_nanogram`; apply the
idempotent ALTER emitted by `--print-schema` before upgrading the runner. Its
first run refreshes historical snapshots with the new field.

The collector uses the existing verified config #34 rounds, requires matching
nominal intervals and an unambiguous election-to-cycle link, and compares the
final result's validator-set hash with that round's verified config hash. It
checks `remaining <= selected <= submitted` wherever the operands are known.
Bootstrap/ambiguous rounds absent from verified count history stay absent here.
Invalid results leave the final amount NULL and return an error after publishing
valid sibling rows; `invalid_source` distinguishes them from missing observations.

Selected capital is a maximum **observed** principal. It relies on the indexer
having collected the original selection; a populated cycle alone is not proof
of complete historical coverage. Likewise, a missing final result does not mean
zero rewards, zero principal, or necessarily an unfinished round.

## Operation

### Two-round staking APR (`stake-v3`)

The same scheduled `validator_stake` handler also prepares:

```text
apr_percent = reward_nanogram / selected_nanogram
              * (365 * 86400) / apr_cycle_seconds * 100
apr_cycle_seconds = next.utime_until - current.utime_since
```

The next verified round must start exactly at the current round's end. Both
intervals must be closed and fully covered, with verified config/roster proofs.
Do not double a single historical interval or skip a missing/ambiguous round.
Ordinary mainnet intervals are 65,536 s each (18 h, 12 min, 16 s), giving a
131,072 s capital cycle (36 h, 24 min, 32 s). This is an effective capital-use
cycle, not a measurement of continuous Elector custody. Use the stored verified
intervals for historical changes; unresolved historical rounds remain gaps.

The current election must have a positive Selected amount and a verified released
Reward. A successor does not need its own release result. Missing amounts, zero
denominators, unfinished intervals and absent successors produce NULL, with an
`apr_status` explaining the missing prerequisite. Known zero rewards produce zero.
Amounts remain integer nanoGRAM; Go `big.Int`/`big.Rat` avoids float conversion.
Only the final percentage is rounded half away from zero to 12 decimal places,
stored as `Nullable(Decimal(76,12))` (enough for the full source amount domain).

This is simple annualized reward/Selected, with no compounding, pool fee deduction
or principal-loss subtraction. Remaining after penalties determines reward
allocation, not this chart's denominator. It is a network aggregate, not a claim
that a penalized validator or a pool depositor realizes this exact rate.

`apr_next_utime_until`, `apr_next_config34_hash`, `apr_next_roster_digest` record
the successor proof. These fields and the APR status/value participate in the
snapshot hash. All-history hourly reconciliation repairs late rewards, corrected
intervals and removed successors without rewriting unchanged rows. Coverage is
read for all rounds even when an explicit input date range ends before the next
round. Apply the additional idempotent ALTER in `--print-schema` before upgrading.

### Running

Apply the DDL emitted by `--print-schema` as the destination administrator.
Grant the aggregate role SELECT/INSERT on `gramstat.validator_stake_rounds`,
SELECT on the configured source's `validator_round_results`, and its existing
SELECT on `gramstat.validator_rounds`. The API needs SELECT on the new aggregate.

```sh
export GRAMSTAT_STAKE_RESULTS_DB=mainnet_index
# Existing GRAMSTAT_PG_DSN and GRAMSTAT_CH_* settings are reused.
gramstat-aggregate --handlers validator_stake --from 2019-01-01 --to 2026-11-10
```

Explicit dates select overlapping rounds, not the date of release. Final results
are read through the run's `AsOf` so an old round includes its later release.
This is a current historical reconstruction, not a historical point-in-time API.

The standard Go scheduler runs `validator_stake` every five minutes after `validators` and before
`wallets`. Automatic runs reconcile the small complete cycle/result tables, so
late releases, old backfills and corrections are picked up outside the lookback.
Unchanged content hashes produce no insert. Replacements atomically update all
three stages, including clearing a previously known result. Read with `FINAL`.
PostgreSQL is read-only and bounded by statement/connect timeouts; ClickHouse
source access is SELECT-only. No blockchain replay is performed by this handler.

Related: [round results](validator-round-results.md),
[validator history](gramstat-validator-history.md), and TON's
[system contracts](https://docs.ton.org/foundations/system).
