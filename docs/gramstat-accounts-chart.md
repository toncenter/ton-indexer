# Existing-account charts

An account is included when its state is `uninit`, `active`, or `frozen`.
`nonexist` is excluded. This includes all workchains and is not a count of wallets
or addresses ever seen. Deleted accounts reduce the count; status transitions
move between components without changing the total.

## Verified opening state

The full mainnet dataset starts at MC 3. A direct archived-state enumeration at
MC 2 found **7 active accounts**, all in the masterchain, and zero uninitialized
or frozen accounts. The basechain zerostate had no accounts. Thus the opening
vector is `(uninit=0, active=7, frozen=0)`, not zero.

Independent enumeration at MC 3 still gave 7. At MC 1001 it gave 9 (8 in the
masterchain and 1 in the basechain). The 999 deduplicated delta rows for MC 3–1001
sum to `(0,+2,0)`, exactly matching the independent states.

Hashes and exact counts are recorded in [accounts-baseline.json](accounts-baseline.json).
The [native probe](../ton-index-worker/tools/accounts-state-probe.cpp) enumerates
each anchored shard dictionary once, includes the MC dictionary, handles shard
zerostates, and rejects invalid account/state tags. It ran through an isolated
secondary NodeDB reader, with the primary database bind-mounted read-only. No
indexer restart or replay was needed. The baseline includes initial system
accounts; the older zero-opening delta-only examples intentionally omitted them.

## Preparation and reads

The independent Go `accounts` handler reads `GRAMSTAT_CHAIN_METRICS_DB` and
`gramstat.accounts_baseline`. The baseline table is operator-provisioned; the
writer only needs SELECT on it. It verifies a continuous delta prefix starting
immediately after the baseline MC, leaves a 1000-MC settling margin, and writes
`gramstat.accounts_hourly` with opening and closing counts for each UTC hour.
Negative counts, missing MCs, or inconsistent temporal ordering fail publication.
All sums use `FINAL` deduplication before accumulation.

The handler does not apply viewport date filters before accumulation. Add it to
`--handlers` and apply its DDL via `--print-schema`. Grant SELECT on the source
delta table and baseline, and SELECT/INSERT on `gramstat.accounts_hourly`.

Readers use hourly rollups for long ranges. Short-range reads take the opening
counts of the first selected hour, add each MC's signed deltas, then filter the
viewport. Raw reads are bounded both by MC and the source table's timestamp key.
Counts remain exact integer strings in HTTP responses. Multiple MCs in a second
remain distinct at full resolution. Hours with no MC do not manufacture new
observations; the chart uses steps between state observations.
