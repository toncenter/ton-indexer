CREATE TABLE IF NOT EXISTS gramstat.validator_stake_locked_rounds (
    utime_since UInt32, utime_until UInt32, previous_utime_since UInt32,
    config34_hash String, roster_digest String,
    previous_config34_hash String, previous_roster_digest String,
    current_selected_nanogram Nullable(Decimal(38,0)),
    previous_selected_nanogram Nullable(Decimal(38,0)),
    total_nanogram Nullable(Decimal(38,0)),
    telegram_nanogram Nullable(Decimal(38,0)), pools_nanogram Nullable(Decimal(38,0)),
    others_nanogram Nullable(Decimal(38,0)), unattributed_nanogram Nullable(Decimal(38,0)),
    pool_breakdown Map(String,Decimal(38,0)),
    positions UInt32, typed_positions UInt32, isp_observed_positions UInt32,
    affiliated_pool_positions UInt32, overlap_positions UInt32,
    data_through UInt32, previous_data_through UInt32, is_partial UInt8,
    status LowCardinality(String), snapshot_hash String,
    calc_version LowCardinality(String), calculated_at DateTime64(6,'UTC'),
    members Array(Tuple(
        utime_since UInt32, idx UInt16, pubkey String, adnl_addr String, holder String,
        stake_nanogram Nullable(Decimal(38,0)), contract_type Nullable(String),
        type_status String, family String, protocol String, affiliation_evidence String,
        isp Nullable(String), isp_status String, observed_at Nullable(UInt32),
        record_id Nullable(String), group String
    ))
) ENGINE=ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(toDateTime(utime_since,'UTC')) ORDER BY utime_since;
