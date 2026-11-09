CREATE TABLE IF NOT EXISTS gramstat.validator_stake_rounds (
    utime_since UInt32,
    utime_until UInt32,
    election_id Nullable(UInt32),
    submitted_nanogram Nullable(Decimal(38,0)),
    selected_nanogram Nullable(Decimal(38,0)),
    remaining_nanogram Nullable(Decimal(38,0)),
    reward_nanogram Nullable(Decimal(38,0)),
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
