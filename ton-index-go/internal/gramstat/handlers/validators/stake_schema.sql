CREATE TABLE IF NOT EXISTS gramstat.validator_stake_rounds (
    utime_since UInt32,
    utime_until UInt32,
    election_id Nullable(UInt32),
    submitted_nanogram Nullable(Decimal(38,0)),
    selected_nanogram Nullable(Decimal(38,0)),
    remaining_nanogram Nullable(Decimal(38,0)),
    reward_nanogram Nullable(Decimal(38,0)),
    apr_percent Nullable(Decimal(76,12)),
    apr_cycle_seconds Nullable(UInt32),
    apr_next_utime_until Nullable(UInt32),
    apr_next_config34_hash Nullable(String),
    apr_next_roster_digest Nullable(String),
    apr_status LowCardinality(String),
    election_source_mc_seqno Nullable(UInt32),
    cycle_source_mc_seqno UInt32,
    unfreeze_mc_seqno Nullable(UInt32),
    unfreeze_utime Nullable(UInt32),
    unfreeze_tx_hash Nullable(String),
    result_vset_hash Nullable(String),
    config34_hash String,
    roster_digest String,
    results_source String,
    status LowCardinality(String),
    snapshot_hash String,
    calc_version LowCardinality(String),
    calculated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(toDateTime(utime_since, 'UTC'))
ORDER BY utime_since;
ALTER TABLE gramstat.validator_stake_rounds
    ADD COLUMN IF NOT EXISTS reward_nanogram Nullable(Decimal(38,0));
ALTER TABLE gramstat.validator_stake_rounds
    ADD COLUMN IF NOT EXISTS apr_percent Nullable(Decimal(76,12)),
    ADD COLUMN IF NOT EXISTS apr_cycle_seconds Nullable(UInt32),
    ADD COLUMN IF NOT EXISTS apr_next_utime_until Nullable(UInt32),
    ADD COLUMN IF NOT EXISTS apr_next_config34_hash Nullable(String),
    ADD COLUMN IF NOT EXISTS apr_next_roster_digest Nullable(String),
    ADD COLUMN IF NOT EXISTS apr_status LowCardinality(String);
