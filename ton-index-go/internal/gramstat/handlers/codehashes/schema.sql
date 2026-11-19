CREATE DATABASE IF NOT EXISTS gramstat;

-- A bounded row per Base64 prefix, rather than ~457k rows every ten minutes.
-- Unpublished generations are never visible through the completion table.
CREATE TABLE IF NOT EXISTS gramstat.account_code_hash_counts (
    source_id LowCardinality(String),
    snapshot_id UUID,
    observed_day Date,
    prefix FixedString(1),
    mc_before UInt32,
    mc_after UInt32,
    counts Map(String, UInt64) CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_day)
ORDER BY (source_id, snapshot_id, prefix);

-- Written LAST, after read-back verification of all 64 prefix rows.
CREATE TABLE IF NOT EXISTS gramstat.account_code_hash_snapshots (
    source_id LowCardinality(String),
    snapshot_id UUID,
    observed_day Date,
    started_at DateTime64(6, 'UTC'),
    finished_at DateTime64(6, 'UTC'),
    published_at DateTime64(6, 'UTC'),
    hash_count UInt64,
    indexed_accounts UInt64,
    source_mc_min UInt32,
    source_mc_max UInt32,
    pages UInt64,
    zero_hashes UInt64,
    consistency LowCardinality(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_day)
ORDER BY (source_id, finished_at, snapshot_id);
