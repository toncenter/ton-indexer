CREATE DATABASE IF NOT EXISTS gramstat;
CREATE TABLE IF NOT EXISTS gramstat.validator_rounds (
    utime_since UInt32, utime_until UInt32,
    total UInt16, main UInt16, total_weight UInt64,
    source_mc_seqno UInt32,
    anchor_mc_seqno UInt32, anchor_utime UInt32,
    anchor_root_hash String, anchor_file_hash String,
    config34_hash String, roster_digest String,
    calculated_at DateTime64(6, 'UTC'), data_through UInt32, is_partial UInt8,
    calc_version LowCardinality(String),
    members Array(Tuple(idx UInt16, pubkey String, adnl_addr String, weight UInt64))
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(toDateTime(utime_since, 'UTC')) ORDER BY utime_since;

CREATE TABLE IF NOT EXISTS gramstat.validator_geo_rounds (
    utime_since UInt32, utime_until UInt32,
    config34_hash String, roster_digest String,
    total UInt16, observed UInt16, located UInt16, country_known UInt16, unknown UInt16,
    countries Nullable(UInt16), breakdown Map(String, UInt16),
    calculated_at DateTime64(6, 'UTC'), data_through UInt32, source_through UInt32,
    is_partial UInt8, status LowCardinality(String), calc_version LowCardinality(String),
    source LowCardinality(String) DEFAULT 'session_stats_mainnet_v2.node_addresses',
    geoip_version Nullable(String) DEFAULT NULL,
    members Array(Tuple(
        pubkey String, adnl_addr String, observed_at Nullable(UInt32), record_id Nullable(String),
        country Nullable(String), city Nullable(String),
        latitude Nullable(Float64), longitude Nullable(Float64),
        samples UInt64, ip_versions UInt32, country_versions UInt32, ordering_conflicts UInt32,
        status String
    ))
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(toDateTime(utime_since, 'UTC')) ORDER BY utime_since;

-- The whole roster and its distribution are replaced atomically. Removed points
-- cannot survive a corrected snapshot as orphan per-member rows.
CREATE VIEW IF NOT EXISTS gramstat.validator_geo_members AS
SELECT utime_since, utime_until, config34_hash, calculated_at, data_through, is_partial,
       member.pubkey AS pubkey, member.adnl_addr AS adnl_addr,
       member.observed_at AS observed_at, member.record_id AS record_id,
       member.country AS country, member.city AS city,
       member.latitude AS latitude, member.longitude AS longitude,
       member.samples AS samples, member.ip_versions AS ip_versions,
       member.country_versions AS country_versions, member.ordering_conflicts AS ordering_conflicts,
       member.status AS status
FROM gramstat.validator_geo_rounds FINAL ARRAY JOIN members AS member;
