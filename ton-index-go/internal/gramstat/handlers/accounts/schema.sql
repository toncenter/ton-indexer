CREATE TABLE IF NOT EXISTS gramstat.accounts_baseline (
    source_database String, mc_seqno UInt32, gen_utime DateTime('UTC'),
    uninit UInt64, active UInt64, frozen UInt64,
    state_hash FixedString(64), evidence String, baseline_id String,
    calculated_at DateTime64(6,'UTC')
) ENGINE=ReplacingMergeTree(calculated_at) ORDER BY(source_database,mc_seqno);

CREATE TABLE IF NOT EXISTS gramstat.accounts_hourly (
    hour DateTime('UTC'), first_mc UInt32, last_mc UInt32,
    first_utime UInt32, last_utime UInt32,
    opening_uninit UInt64, opening_active UInt64, opening_frozen UInt64,
    uninit UInt64, active UInt64, frozen UInt64,
    mc_count UInt32, source_mc_to UInt32, baseline_id String,
    calculated_at DateTime64(6,'UTC')
) ENGINE=ReplacingMergeTree(calculated_at) ORDER BY hour;
