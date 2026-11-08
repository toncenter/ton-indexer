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

CREATE TABLE IF NOT EXISTS gramstat.inflation_hourly (
    point_at DateTime('UTC'), window_from DateTime('UTC'),
    minted_24h_nanoton Nullable(Decimal(38,0)), burned_24h_nanoton Nullable(Decimal(38,0)),
    supply_nanoton Nullable(Decimal(38,0)), supply_utime UInt32, supply_mc UInt32,
    annualized_percent Nullable(Decimal(38,12)), source_mc_to UInt32,
    calculated_at DateTime64(6,'UTC')
) ENGINE = ReplacingMergeTree(calculated_at) ORDER BY point_at;
