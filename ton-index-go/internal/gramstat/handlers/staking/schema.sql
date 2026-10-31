CREATE DATABASE IF NOT EXISTS gramstat;
CREATE TABLE IF NOT EXISTS gramstat.staking_daily (
    day Date, anchor_mc_seqno UInt32, state_mc_seqno UInt32,
    data_through UInt32, is_partial UInt8, calculated_at DateTime64(6,'UTC'),
    calc_version LowCardinality(String), status LowCardinality(String),
    remaining_nanogram Nullable(Decimal(38,0)),
    submitted_nanogram Nullable(Decimal(38,0)), selected_nanogram Nullable(Decimal(38,0)),
    frozen_principal_reduction_nanogram Nullable(Decimal(38,0)),
    breakdown Map(String,Decimal(38,0)),
    telegram_nanogram Nullable(Decimal(38,0)),
    telegram_observed_nanogram Nullable(Decimal(38,0)), telegram_unknown_nanogram Nullable(Decimal(38,0)),
    positions UInt32, elections UInt32, unknown_origins UInt32
) ENGINE=ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(day) ORDER BY day;
