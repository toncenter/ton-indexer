CREATE TABLE IF NOT EXISTS gramstat.account_type_features (
    feature_id UUID,
    feature_day Date,
    prefix FixedString(1),
    assignments Map(String, String) CODEC(ZSTD(3)),
    methods Map(String, Array(UInt32)) CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(feature_day)
ORDER BY (feature_id, prefix);

-- Published only after all 64 feature rows have been verified.
CREATE TABLE IF NOT EXISTS gramstat.account_type_feature_sets (
    feature_id UUID,
    feature_day Date,
    digest FixedString(64),
    abi_revision FixedString(64),
    classification_version String,
    methods_started_at DateTime64(6, 'UTC'),
    methods_finished_at DateTime64(6, 'UTC'),
    published_at DateTime64(6, 'UTC'),
    contracts Map(String, String) CODEC(ZSTD(3)),
    code_contracts Map(String, Array(String)) CODEC(ZSTD(3)),
    method_names Map(UInt32, Array(String)) CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(feature_day)
ORDER BY feature_id;

-- Completion record: the entire small tree plus immutable source identities.
CREATE TABLE IF NOT EXISTS gramstat.account_type_snapshots (
    publication_id UUID,
    source_id LowCardinality(String),
    counts_snapshot_id UUID,
    observed_day Date,
    feature_id UUID,
    feature_day Date,
    abi_revision FixedString(64),
    classification_version String,
    started_at DateTime64(6, 'UTC'),
    finished_at DateTime64(6, 'UTC'),
    published_at DateTime64(6, 'UTC'),
    indexed_accounts UInt64,
    hash_count UInt64,
    source_mc_min UInt32,
    source_mc_max UInt32,
    nodes_json String CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_day)
ORDER BY (source_id, publication_id);
