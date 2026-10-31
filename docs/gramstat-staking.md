# Staked Gram: Elector history

`gramstat-aggregate --handlers staking` derives daily stake amounts from the
indexer's PostgreSQL history and Session Stats. No blockchain RPC is required.

## Amounts

All calculations use integer nanoGRAM (`1 GRAM = 10^9 nanoGRAM`). JSON uses exact
decimal strings; SQL views expose `numeric(38,0)` / `Decimal(38,0)`. Go uses
`big.Int`.

For the elections whose stakes are **still present in the Elector** at the anchor:

* **Submitted**: final total of accepted applications, including losing candidates;
  not the sum of inbound transfers.
* **Selected**: original principal frozen for the winning positions.
* **Remaining**: principal still in their frozen dictionaries, after reductions.

These amounts refer to the same elections and are not additive. Include each
simultaneously retained election once; the same holder in two elections has two
positions. Disappearance from a complete Elector observation closes an election.
`unfreeze_at` alone does not. Elector credits, rewards and pool cash are excluded.

`frozen_principal_reduction_nanogram = selected - remaining` describes reductions
of **currently retained positions**, not fines during that calendar day or since
genesis. Source positions also preserve `banned`: confiscation on release must be
distinguished from ordinary release. Raw observations support separate penalty
event analysis; disappearance itself is never counted as a fine.

Original selected stake requires a preceding complete observation without this
election, matching native `previous_data_hash`/`data_hash`, unchanged Elector
address/code hash, and matching successful finished applications in the first
observation. Complaint/vote activity in that MC prevents inferring an unpenalized
origin. Current pubkey/holder pairs must match, without increased principal.
A late start or broken chain leaves submitted/selected/reduction NULL; remaining
can still be exact. Legacy cycle tables retain `GREATEST` semantics and are not
used to invent original amounts.

## Native collection and storage

The existing comparison of Elector data before/after transactions is retained.
Unchanged data cause no extra getter. Changed data cause one `past_elections`
call for the final MC state, shared with legacy cycle records. Key blocks and
the explicit starting MC of a replay also produce observations. Reward-only
changes can therefore produce unchanged principal amounts.
`participant_list_extended` is also read at most once per observation and shared
with existing application tables. Getters use historical MC time/config; no new
transaction replay is introduced.

The parser supports nested/flattened historical getter tuples, preserves holder,
weight, stake and banned, and reconciles exact sums. A malformed entry invalidates
the entire observation. Getter/parse failures are recorded as incomplete;
state-loading failures use scheduler retry. A successful empty list is distinct
from a failed read. The aggregator never skips a failed read to carry an older
good amount through it.

| Object | Contents |
| --- | --- |
| `elector_stake_states` | One versioned complete/incomplete observation per MC, including elections, positions, applications and Elector hashes |
| `elector_frozen_positions` | View expanding complete observations into `(mc_seqno,election_id,pubkey)` rows |
| `elector_frozen_elections` (PG) | Small indexed child table for locating election origins |
| `stake_holder_states` | Classification by `(account,mc_seqno)`, code/data hashes, pool and error |
| `validator_set_states` | Native config #34 cell/hash at key blocks and replay boundaries |

Collection is independent of `--gramstat`. PG migration **1.3.2** creates source
objects in `public`; history stays in PG with Kvrocks enabled and commits with
blocks. Replaying an MC replaces the whole observation and removes former
election children, including when the new observation is empty or incomplete.
CH writes source tables before blocks. An Elector observation is one
`ReplacingMergeTree` row with JSON arrays and decimal strings. Read with `FINAL`
before expansion; the supplied positions view does this. Whole-row replacement
prevents orphan members after replay.

Example: MC 100 records A=100 GRAM, B=200. A fine at MC 200 adds A=95, B=200;
the old list remains. A complete empty list at MC 300 means release, not a
295-GRAM fine. An incomplete list means the amount is unknown.

## Holder classification

Holders are masterchain accounts. Final states of changed masterchain accounts
are classified, including code upgrades and disappearance. Elector observations
reuse those results; untouched holders are read from already loaded states.
A holder is probed at most once per MC, even across elections. Getters run only
when their methods are exported.

* Nominator pool: the code hash used by `NominatorPoolContract`.
* Single nominator v1.0/v1.1: [published Orbs hashes](https://github.com/orbs-network/single-nominator#official-code-hash).
* Standard LST controller: pool from `get_validator_controller_data` entry 12
  (entry 11 is the validator wallet).
* Hipo: treasury from `get_loan_state` entry 1.
* wsTON: first-party proxy addresses documented in the one-off investigation,
  enabled from the independent observation at 2026-10-17 11:20 UTC. Earlier
  attribution needs an earlier reviewed interval.

Daily queries use the latest holder observation at/before the anchor, including
unknown results. Unsupported code, missing history and failed getters remain
unknown. Generic Whales proxies are not all liquid; the unproven historic Bemo
label is not accepted. Pool/treasury addresses remain available for service-level
queries. LST amounts can include validator self-stake and are not customer funds
or TVL ([TON Docs](https://docs.ton.org/nodes/staking/liquid-staking)).

## Aggregation and Telegram

`gramstat.staking_daily` uses `ReplacingMergeTree(calculated_at)` keyed by UTC day.
DDL is printed by `--print-schema` and applied explicitly. The handler refreshes
the usual recent window or an explicit range, in batches of at most seven days.
Anchors are the last indexed MC strictly before the next UTC day/requested
current cutoff. Quiet days carry the latest state; incomplete history has NULL
amounts and an explicit status.

`breakdown` partitions remaining principal into liquid staking, nominator pool,
single nominator and unknown. Telegram is independent and must not be added to
that partition: Telegram stake can also be single-nominator stake.

For each ADNL, SS supplies the latest `(created_timestamp,record_id)` record in
the 24 hours ending at the anchor. Only `geoip_isp='Telegram Messenger'` is tagged.
Missing/latest-NULL ISP or conflicting records are unknown. Future observations
are excluded. This is an infrastructure label, not proof of ownership, and uses
a time-bounded lookup rather than the manual chart's sticky first-observation rule.

`telegram_observed_nanogram` is the tagged amount found;
`telegram_unknown_nanogram` is principal without usable ISP data.
`telegram_nanogram` is exact only when unknown principal is zero. All three are
NULL if Elector principal itself is unknown. Query failures leave previous
published rows untouched and fail the handler.

```sql
SELECT day, remaining_nanogram, submitted_nanogram, selected_nanogram,
       frozen_principal_reduction_nanogram, breakdown,
       telegram_nanogram, telegram_unknown_nanogram, status
FROM gramstat.staking_daily FINAL ORDER BY day;
```

## Deployment and coverage

Apply/rerun PG migration 1.3.2 and upgrade the indexer, then apply the aggregator's
`--print-schema` DDL. Configure `GRAMSTAT_PG_DSN`, destination `GRAMSTAT_CH_*`
and read-only `GRAMSTAT_SS_*`. Native #34 replaces `GRAMSTAT_CONFIG_URL`/API key.
Run `--handlers validators,validator_geo,staking` for a populated range.

Old `blocks` do not prove coverage of these histories. Older ranges require
explicit source replay with `--from N --to M --force`; start before selection
for original totals. Starting mid-election establishes remaining principal,
not its origin. Establish continuous collection over the published range and
preserve uncertainty flags. Replay and production cost measurements are separate
operator actions, not migration effects.
