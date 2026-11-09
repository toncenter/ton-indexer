# Stake changes between rounds

`validator_stake_changes` runs in the common hourly aggregator after
`validator_stake`. It reconciles the entire indexed history into
`gramstat.validator_stake_changes`, keyed by the **new** validation round `n`.
There is no separate timer. Explicit windows restrict output, not predecessor
discovery. Content hashes suppress unchanged writes and repair late old inputs.

## Population and timing

Compare selected principal in `n` with `n−2` for the same stake-holder address.
The intermediate round `n−1` must exist, and all three verified intervals must
abut exactly. Never skip an absent/ambiguous round or assume a fixed 131,072 s
offset. This gives two interleaved chains: `n → n+2` and `n−1 → n+1`.

PostgreSQL cycle members supply selected stake; election participants supply
holder addresses. Validator keys may rotate. All keys belonging to one holder
are summed before comparison, and every matched holder has equal weight. This
is neither a validator-count distribution nor a capital-weighted distribution.
New/exited holders are reported separately and excluded from the five-category
denominator. The current round can be ongoing: its elected selection is known.
The old round's release must already have happened before `n` starts.

Every member must have a holder and stake; its roster digest must match the
verified config roster, and the sum must match the independently prepared
Selected total. Unknown member mappings/amounts invalidate the comparison rather
than silently shrinking the population. The API checks the three config/roster
proofs and old release/current selection again before exposing the result.

## Reward reference and fines

The reward reference for holder `h` is the exact rational quantity:

```text
expected(h) = remaining(h, n−2) * bonuses(n−2) / total_remaining(n−2)
delta(h) = selected(h, n) − selected(h, n−2)
```

When Selected equals Remaining, the old member selection is already the final
principal. Otherwise read the minimum observed stake per old public key from
ClickHouse `validator_cycle_members`, bounded by the release MC. Frozen stakes
only decrease through fines. Accept those per-member minima only when all are
known, none exceeds its selected amount, and their sum equals the independently
captured final `validator_round_results.total_stake`. An unreconciled allocation
stays NULL (`unknown_penalty_allocation`); do not allocate fines proportionally.

This is a proportional share of accumulated bonuses, not a reconstructed final
per-holder payment (bans and integer rounding may affect credits). Changes can
include external deposits, withdrawals and movements between holders. A category
does not prove the source of the funds or an operator's reinvestment policy.

## Five mutually exclusive categories

1. `unchanged`: delta is exactly zero.
2. `decreased`: delta is negative.
3. For positive delta, `near_reward`: `abs(delta−expected) <= max(0.02*expected, 5 GRAM)`.
4. Otherwise `below_reward`: positive delta less than expected.
5. Otherwise `above_reward`: delta greater than expected.

All money remains integer nanoGRAM (`big.Int`); comparisons multiply out the
rational denominator, without floating-point conversion or early rounding.
The stored UInt32 counts sum to `matched`. Missing observations have NULL counts,
not zero. `previous_holders = matched + exited_holders` and
`current_holders = matched + new_holders`. An empty intersection is not a 0% point.

The UI plots a stepped stacked area with a fixed 0–100% axis. It rounds cumulative
shares to four decimal percentage places and differences them so displayed shares
sum to exactly 100%. Hiding a series does not renormalize the other categories.
Tooltips include the predecessor, individual counts, matched/current coverage and
new/exited counts. Coverage gaps remain gaps throughout the full history.

## Deployment

Apply `StakeChangesSchema` (also in `--print-schema`). The existing aggregate role
needs SELECT/INSERT on `gramstat.validator_stake_changes` and SELECT on the source
`validator_cycle_members`, alongside its current round-result grants. The API
needs SELECT on the new aggregate only. PostgreSQL access remains read-only.
The penalty reader batches at most 64 rounds per bounded source request.

Run `gramstat-aggregate --handlers validator_stake_changes` for initial history;
add the handler after `validator_stake` and before `wallets` in the existing service.
