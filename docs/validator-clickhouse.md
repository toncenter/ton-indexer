# Validator source tables in ClickHouse

`ton-index-clickhouse` stores the existing validator events, election snapshots,
key-block validator sets and complaints produced by `StakingEventProcessor`.
It also stores the method IDs already extracted by interface detection.
Collection is independent of `--gramstat` and uses the ordinary database selected
by `--ch`. Tables are created at startup and inserted before the batch's `blocks`.

Election getters and contract-method parsing use bounded [inter-block caches](gramstat-caches.md).
Successful method metadata remains known even when a different historical config
lacks the referenced library; it is not a claim of executability at that MC.

## Tables and replacement keys

| Table | Logical key / observation key | Reading |
| --- | --- | --- |
| `validator_events` | `(mc_seqno, tx_lt, tx_hash)` | `FINAL` deduplicates events |
| `validator_elections` | `election_id` | `FINAL` selects greatest `source_mc_seqno` |
| `validator_election_participants` | `(election_id, validator_pubkey, stake_holder_address)` | `FINAL` selects greatest `source_mc_seqno` |
| `validator_cycles` | `(utime_since, source_mc_seqno)` | `FINAL`, then group by round |
| `validator_cycle_members` | `(utime_since, validator_index, source_mc_seqno)` | `FINAL`, then group by member |
| `validator_complaints` | `(election_id, complaint_hash)` | `FINAL` selects greatest `source_mc_seqno` |
| `contract_methods` | `code_hash` | `FINAL`; sorted, unique `methods Array(UInt64)` |

Columns correspond to the existing PostgreSQL source tables. Timestamps and
election IDs are `UInt32` Unix seconds, logical times/query IDs/weights are exact
`UInt64`, and monetary amounts are exact integer nanoGRAM `Decimal(38,0)`.
Transaction and code hashes are base64 `FixedString(44)`; validator pubkeys, ADNL
and complaint/vset hashes retain the collector's hexadecimal representation.
Holder and reward addresses retain their raw workchain-prefixed representation.

Event pubkey, ADNL, election ID, query ID and reason are nullable. Cycle election
ID and total stake, member stake, and complaint ADNL are nullable too. Unknown
enrichment remains NULL; a known zero stake remains zero. Complaint
`voted_validators` retains the collector's JSON array as a `String`.
`approved_percent` remains `Float64`, as in the existing collector; money never
passes through floating point.

## Round and member reduction

A cycle can be observed in config #36, #34 and #32. These are observations of a
validator set, not three separate rounds. The collector emits cycle/member rows
on key blocks. Later observations can have a smaller stake after fines, or NULL
enrichment after the Elector releases the election. Replacing all observations
with the newest one would lose the selected stake and sometimes the election link.

ClickHouse retains these already-collected observations using `source_mc_seqno`
in their replacement key. Retries of the same observation converge to one row;
different observations survive merges. This is not a per-masterblock stake series.
PostgreSQL instead folds the observations into a single upserted row using
`GREATEST` for stakes and keeping its non-null election link.

Read the equivalent selected capital in ClickHouse with `max(total_stake)` and
`max(stake)`, never by summing observations. The election-to-cycle link is immutable,
so `max(election_id)` retains its known value across later NULL observations. All-NULL
enrichment remains NULL. Config fields come from the latest observation.

<!-- validator-cycles-clickhouse -->
```sql
SELECT utime_since,
       max(election_id) AS election_id,
       max(total_stake) AS total_stake,
       argMax(tuple(utime_until, total, main, total_weight), c.source_mc_seqno) AS config,
       max(c.source_mc_seqno) AS source_mc_seqno
FROM validator_cycles AS c FINAL
GROUP BY utime_since
```

<!-- validator-members-clickhouse -->
```sql
SELECT utime_since, validator_index,
       argMax(tuple(validator_pubkey, adnl_addr, weight), m.source_mc_seqno) AS validator,
       max(stake) AS stake,
       max(m.source_mc_seqno) AS source_mc_seqno
FROM validator_cycle_members AS m FINAL
GROUP BY utime_since, validator_index
```

Use `argMax(tuple(...), source_mc_seqno)` for any other cycle configuration fields
needed by a reader. `FINAL` matters before reduction if an observation has been
corrected by reindexing: otherwise the old value can still contribute to `max`.
For mutable elections, participants and complaints, `source_mc_seqno` orders
observations rather than insertion time. A late older batch cannot replace a
newer snapshot. Participant source seqnos describe the latest observation;
PostgreSQL may retain the last changed observation when it suppresses no-op updates.

Join reduced cycles to [final round results](validator-round-results.md) by
`election_id`, and reduced members to election participants by election ID and
validator pubkey. Participant rows supply the holder address. Its historical
`account_states.code_hash` can be matched to `contract_methods` or a known-code
registry. These raw tables supply data for classification; they do not assign a
staking service automatically.

## Backfill and downstream use

Existing `blocks` rows do not prove that newly added validator tables were populated.
Fill an already indexed range explicitly with `--from N --to M --force`.
Each batch uses inserts only, with retry deduplication via `ReplacingMergeTree`.
Source MC and parent election/cycle identities are checked before inserting any of
these seven tables. A failed insert causes the ordinary indexer retry.

An isolated run can use its own `--ch` database and `--gramstat-db` as described in
[the Gramstat guide](gramstat.md#isolated-clickhouse-runs). The optional Gramstat
database is separate from these source tables.

The existing Go validator aggregator still reads PostgreSQL and publishes
`gramstat.validator_rounds` / `validator_geo_rounds`; those are distinct derived
datasets. A ClickHouse-source reader can use the reductions above independently.
