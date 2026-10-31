CREATE DATABASE IF NOT EXISTS gramstat;
CREATE TABLE IF NOT EXISTS gramstat.telegram_stars_daily (
    day Date,
    purchases UInt64,
    unknown_purchases UInt64,
    known_stars_total Decimal(38,0),
    stars_total Nullable(Decimal(38,0)),
    buckets Array(Tuple(
        payment_asset String, product String,
        stars_per_purchase Nullable(UInt64), quantity_status String,
        purchases UInt64, stars_total Nullable(Decimal(38,0))
    )),
    calculated_at DateTime64(6, 'UTC'),
    data_through DateTime('UTC'), source_mc_seqno UInt32, is_partial UInt8,
    calc_version LowCardinality(String) DEFAULT 'stars-v1'
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(day) ORDER BY day;

CREATE VIEW IF NOT EXISTS gramstat.telegram_stars_buckets AS
SELECT day, calculated_at, data_through, source_mc_seqno, is_partial,
       bucket.payment_asset AS payment_asset, bucket.product AS product,
       bucket.stars_per_purchase AS stars_per_purchase,
       bucket.quantity_status AS quantity_status,
       bucket.purchases AS purchases, bucket.stars_total AS stars_total
FROM gramstat.telegram_stars_daily FINAL ARRAY JOIN buckets AS bucket;
