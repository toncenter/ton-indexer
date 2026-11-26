-- Addresses use uppercase raw 0:HEX, with native TON as '' and pTON masters unchanged.
-- The database name is a parameter of the writer (`dex_snapshots` below).

CREATE DATABASE IF NOT EXISTS dex_snapshots;

-- Each changed pool has one trusted snapshot or removal per masterchain block.
CREATE TABLE IF NOT EXISTS dex_snapshots.pool_snapshots
(
    pool            String CODEC(ZSTD(3)),
    mc_seqno        UInt32 CODEC(Delta, ZSTD(3)),
    block_utime     DateTime CODEC(Delta, ZSTD(3)),
    last_tx_lt      UInt64 CODEC(Delta, ZSTD(3)),
    dex             LowCardinality(String),               -- dedust | stonfi
    dex_version     UInt8,
    pool_type       LowCardinality(String),               -- stonfi: constant_product, stableswap, ... | dedust: volatile, stable
    code_hash       FixedString(44),                      -- base64
    router          String CODEC(ZSTD(3)),                -- STON.fi router, '' otherwise

    removed         Bool,

    asset0          String CODEC(ZSTD(3)),                -- in pool order: jetton master, '' = DeDust native TON
    asset1          String CODEC(ZSTD(3)),
    reserve0        UInt128,                              -- raw units (Coins < 2^120)
    reserve1        UInt128,
    lp_fee          UInt32,                               -- numerators over fee_denominator
    protocol_fee    UInt32,
    ref_fee         UInt32,
    fee_denominator UInt32,
    amp             Nullable(UInt128),                    -- curve-specific parameters
    rate            Nullable(UInt128),
    w0              Nullable(UInt128),
    is_locked       Bool,
    lp_supply       Nullable(UInt128),

    spot            Nullable(Float64),                    -- raw asset1 per raw asset0 at the mid, without fees
    depth0          Nullable(Float64),                    -- raw asset0 in that lowers spot by 2%
    depth1          Nullable(Float64),                    -- raw asset1 in that lowers the inverse spot by 2%
    math_version    UInt16,                               -- 0 = no spot (curve not supported or removed)

    writer          LowCardinality(String),               -- live | backfill
    ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(math_version)
PARTITION BY toYYYYMM(block_utime)
ORDER BY (pool, mc_seqno);

CREATE TABLE IF NOT EXISTS dex_snapshots.pool_latest
(
    pool         String,
    version      UInt64,                                  -- mc_seqno << 16 | math_version
    block_utime  DateTime,
    removed      Bool,
    asset0       String,
    asset1       String,
    reserve0     UInt128,
    reserve1     UInt128,
    is_locked    Bool,
    spot         Nullable(Float64),
    math_version UInt16
)
ENGINE = ReplacingMergeTree(version)
ORDER BY pool;

CREATE MATERIALIZED VIEW IF NOT EXISTS dex_snapshots.pool_latest_mv TO dex_snapshots.pool_latest
AS SELECT
    pool,
    bitOr(bitShiftLeft(toUInt64(mc_seqno), 16), toUInt64(math_version)) AS version,
    block_utime, removed, asset0, asset1, reserve0, reserve1, is_locked, spot, math_version
FROM dex_snapshots.pool_snapshots;

-- Each pool's newest state per UTC day, so the pool states before a given minute need no scan of all older snapshots.
CREATE TABLE IF NOT EXISTS dex_snapshots.pool_daily
(
    pool         String,
    day          Date,
    version      UInt64,                                  -- mc_seqno << 16 | math_version
    block_utime  DateTime,
    removed      Bool,
    reserve0     UInt128,
    reserve1     UInt128,
    is_locked    Bool,
    spot         Nullable(Float64),
    math_version UInt16
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (pool, day)
SETTINGS index_granularity = 1024;

CREATE MATERIALIZED VIEW IF NOT EXISTS dex_snapshots.pool_daily_mv TO dex_snapshots.pool_daily
AS SELECT
    pool, toDate(block_utime, 'UTC') AS day,
    bitOr(bitShiftLeft(toUInt64(mc_seqno), 16), toUInt64(math_version)) AS version,
    block_utime, removed, reserve0, reserve1, is_locked, spot, math_version
FROM dex_snapshots.pool_snapshots;

-- Each pool's newest swap fee, one row per pool.
CREATE TABLE IF NOT EXISTS dex_snapshots.pool_fees
(
    pool         String,
    version      UInt64,                                  -- mc_seqno << 16 | math_version
    fee          Float64                                  -- (lp_fee + protocol_fee) / fee_denominator, e.g. 0.003
)
ENGINE = ReplacingMergeTree(version)
ORDER BY pool;

CREATE MATERIALIZED VIEW IF NOT EXISTS dex_snapshots.pool_fees_mv TO dex_snapshots.pool_fees
AS SELECT
    pool,
    bitOr(bitShiftLeft(toUInt64(mc_seqno), 16), toUInt64(math_version)) AS version,
    (lp_fee + protocol_fee) / fee_denominator AS fee
FROM dex_snapshots.pool_snapshots
WHERE NOT removed AND fee_denominator > 0;
