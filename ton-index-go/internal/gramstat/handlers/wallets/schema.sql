-- Source epochs are immutable histories. Repairs to previously acknowledged
-- blocks require a new source_id, rather than silently retaining old minima.
CREATE TABLE IF NOT EXISTS gramstat.wallet_sources (
 source_id String, raw_database String, metrics_database String,
 first_mc UInt32, through_mc UInt32, complete_from UInt32, complete_before UInt32,
 history_verified UInt8, proof_id String
) ENGINE=MergeTree ORDER BY (source_id, through_mc);

CREATE TABLE IF NOT EXISTS gramstat.wallet_first (
 source_id String, registry String, account String,
 first AggregateFunction(min, Tuple(UInt64, UInt32, UInt32, String))
) ENGINE=AggregatingMergeTree ORDER BY (source_id, registry, account);

CREATE TABLE IF NOT EXISTS gramstat.wallet_progress (
 source_id String, registry String, through_mc UInt32
) ENGINE=MergeTree ORDER BY (source_id, registry, through_mc);

-- A run is invisible until wallet_days is written. Partial insert failures and
-- retries cannot change an already published day or double its distinct counts.
CREATE TABLE IF NOT EXISTS gramstat.wallet_activity (
 source_id String, registry String, day Date, run_id UUID,
 step UInt32, bucket UInt32, addresses AggregateFunction(uniqExact, String)
) ENGINE=AggregatingMergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, run_id, step, bucket);

CREATE TABLE IF NOT EXISTS gramstat.wallet_counts (
 source_id String, registry String, day Date, run_id UUID,
 step UInt32, bucket UInt32, active UInt64
) ENGINE=MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, run_id, step, bucket);

CREATE TABLE IF NOT EXISTS gramstat.wallet_activations_mc (
 source_id String, registry String, day Date, run_id UUID,
 mc_seqno UInt32, utime UInt32, activations UInt64, activated UInt64
) ENGINE=MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, run_id, mc_seqno);

CREATE TABLE IF NOT EXISTS gramstat.wallet_days (
 source_id String, registry String, day Date, run_id UUID,
 through_mc UInt32, complete_before UInt32, history_verified UInt8,
 proof_id String, activations UInt64, opening_activated UInt64,
 first_mc UInt32, last_mc UInt32, calculated_at DateTime64(6,'UTC')
) ENGINE=MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, through_mc, calculated_at, run_id);

CREATE TABLE IF NOT EXISTS gramstat.wallet_activated_checkpoints (
 source_id String, registry String, day Date, run_id UUID,
 step UInt32, bucket UInt32, mc_seqno UInt32, utime UInt32, activated UInt64
) ENGINE=MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, run_id, step, bucket);
