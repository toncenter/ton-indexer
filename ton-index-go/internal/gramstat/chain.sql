CREATE TABLE IF NOT EXISTS gramstat.chain_daily (
    day Date,
    transactions UInt64,
    fees_nanoton Nullable(Decimal(38,0)),
    minted_nanoton Nullable(Decimal(38,0)),
    burned_nanoton Nullable(Decimal(38,0)),
    supply_nanoton Nullable(Decimal(38,0)),
    supply_utime Nullable(UInt32),
    is_partial UInt8,
    source_mc_from UInt32, source_mc_to UInt32,
    source_id String,
    calculated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY day;
