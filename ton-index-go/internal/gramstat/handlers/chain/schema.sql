CREATE TABLE IF NOT EXISTS gramstat.supply_hourly (
    hour DateTime('UTC'),
    first_mc UInt32, last_mc UInt32,
    first_utime UInt32, last_utime UInt32,
    opening_burn Nullable(Decimal(38,0)),
    cumulative_burn Nullable(Decimal(38,0)),
    supply_nanoton Nullable(Decimal(38,0)),
    mc_count UInt32, is_partial UInt8, source_mc_to UInt32,
    calculated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(calculated_at) ORDER BY hour;
