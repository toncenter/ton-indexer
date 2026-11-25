CREATE TABLE IF NOT EXISTS gramstat.validator_stake_affiliations (
    holder String, family LowCardinality(String), protocol LowCardinality(String),
    pool String, code_hash String, data_hash String,
    utime_since UInt32, mc_seqno UInt32, root_hash String, file_hash String,
    response_sha256 String, first_mc_seqno UInt32, first_response_sha256 String,
    borrower String, loan_since UInt32,
    verification_version LowCardinality(String), checked_at DateTime64(6,'UTC')
) ENGINE=ReplacingMergeTree(checked_at) ORDER BY (family,holder);
