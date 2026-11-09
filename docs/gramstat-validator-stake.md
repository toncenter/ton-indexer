# Validator stake: three stages per election

`validator_stake` publishes `gramstat.validator_stake_rounds` for the three-line
Stake chart, keyed by the nominal start of a verified validation round:

| Line | Source | Meaning |
| --- | --- | --- |
| Submitted | PostgreSQL `validator_elections.total_stake` | Accepted applications, including unselected candidates and excess capital; only finished, successful elections |
| Selected | PostgreSQL `validator_cycles.total_stake` | Maximum observed selected principal, retained by the indexer's sticky upsert |
| Remaining | ClickHouse `validator_round_results.total_stake` | Principal after fines, immediately before release; rewards are excluded |

These are three stages of the **same election**, joined by its explicit
`election_id`, not simultaneous balances or a sum of overlapping rounds. All
amounts are nullable integer nanoGRAM (`Decimal(38,0)`) and remain exact strings
in Go/JSON. Missing stages stay NULL; a known zero remains zero.

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

The standard hourly service runs `validator_stake` after `validators` and before
`wallets`. Automatic runs reconcile the small complete cycle/result tables, so
late releases, old backfills and corrections are picked up outside the lookback.
Unchanged content hashes produce no insert. Replacements atomically update all
three stages, including clearing a previously known result. Read with `FINAL`.
PostgreSQL is read-only and bounded by statement/connect timeouts; ClickHouse
source access is SELECT-only. No blockchain replay is performed by this handler.

Related: [round results](validator-round-results.md),
[validator history](gramstat-validator-history.md), and TON's
[system contracts](https://docs.ton.org/foundations/system).
