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

-- Immutable, scalar lookup for one complete first-deployment prefix. Build once
-- per backfill snapshot, not once per day/chunk. UUID publication makes retries
-- safe without FINAL or GROUP BY over the full address history on every read.
CREATE TABLE IF NOT EXISTS gramstat.wallet_first_lookup (
 source_id String, registry String, lookup_id UUID, account String,
 first_lt UInt64, first_mc UInt32, first_utime UInt32
) ENGINE=MergeTree ORDER BY (source_id, registry, lookup_id, account);

CREATE TABLE IF NOT EXISTS gramstat.wallet_first_mc (
 source_id String, registry String, lookup_id UUID,
 mc_seqno UInt32, activations UInt64, activated UInt64
) ENGINE=MergeTree ORDER BY (source_id, registry, lookup_id, mc_seqno);

CREATE TABLE IF NOT EXISTS gramstat.wallet_first_daily (
 source_id String, registry String, lookup_id UUID, day Date, activations UInt64
) ENGINE=MergeTree ORDER BY (source_id, registry, lookup_id, day);

CREATE TABLE IF NOT EXISTS gramstat.wallet_lookup_versions (
 source_id String, registry String, through_mc UInt32, lookup_id UUID
) ENGINE=MergeTree ORDER BY (source_id, registry, through_mc, lookup_id);

-- A run is invisible until wallet_days is written. Partial insert failures and
-- retries cannot change an already published day or double its distinct counts.
CREATE TABLE IF NOT EXISTS gramstat.wallet_activity (
 source_id String, registry String, day Date, run_id UUID,
 step UInt32, bucket UInt32, addresses AggregateFunction(uniqExact, String)
) ENGINE=AggregatingMergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, run_id, step, bucket);

-- Only full, certified 30-day windows are published. Source epochs make their
-- contributing closed days immutable; retries insert the same exact count.
CREATE TABLE IF NOT EXISTS gramstat.wallet_monthly (
 source_id String, registry String, day Date, active UInt64,
 runs Array(UUID), calculated_at DateTime64(6,'UTC')
) ENGINE=MergeTree ORDER BY (source_id, registry, day, calculated_at);

-- Calendar-month distincts; deliberately separate from historical rolling MAU.
CREATE TABLE IF NOT EXISTS gramstat.wallet_months (
 source_id String, registry String, month Date, active UInt64,
 runs Array(UUID), calculated_at DateTime64(6,'UTC')
) ENGINE=MergeTree ORDER BY (source_id, registry, month, calculated_at);

CREATE TABLE IF NOT EXISTS gramstat.wallet_counts (
 source_id String, registry String, day Date, run_id UUID,
 step UInt32, bucket UInt32, active UInt64,
 PROJECTION by_interval (SELECT * ORDER BY (source_id, registry, step, day, run_id, bucket))
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

-- Only the certified prefix of an open day. Commit after its scalar counts;
-- this marker never makes a day eligible for immutable closed-month unions.
CREATE TABLE IF NOT EXISTS gramstat.wallet_live_days (
 source_id String, registry String, day Date, run_id UUID,
 through_mc UInt32, complete_before UInt32, proof_id String,
 calculated_at DateTime64(6,'UTC')
) ENGINE=MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id,registry,day,through_mc,calculated_at,run_id);

-- Incremental, immutable MC points. Failed/repeated batches are harmless:
-- readers deduplicate MCs and cap all point/checkpoint reads at the committed state.
CREATE TABLE IF NOT EXISTS gramstat.wallet_activation_stream (
 source_id String,registry String,day Date,mc_seqno UInt32,utime UInt32,activated UInt64
) ENGINE=MergeTree PARTITION BY toYYYYMM(day) ORDER BY (source_id,registry,day,mc_seqno);

CREATE TABLE IF NOT EXISTS gramstat.wallet_activation_stream_checkpoints (
 source_id String,registry String,day Date,step UInt32,bucket UInt32,
 mc_seqno UInt32,utime UInt32,activated UInt64
) ENGINE=MergeTree PARTITION BY toYYYYMM(day) ORDER BY (source_id,registry,step,day,bucket,mc_seqno);

CREATE TABLE IF NOT EXISTS gramstat.wallet_activation_stream_state (
 source_id String,registry String,base_mc UInt32,from_utime UInt32,
 through_mc UInt32,to_utime UInt32,activated UInt64,day Date,
 activations Nullable(UInt64),complete_before UInt32,calculated_at DateTime64(6,'UTC')
) ENGINE=MergeTree ORDER BY (source_id,registry,through_mc);

CREATE TABLE IF NOT EXISTS gramstat.wallet_activated_checkpoints (
 source_id String, registry String, day Date, run_id UUID,
 step UInt32, bucket UInt32, mc_seqno UInt32, utime UInt32, activated UInt64
) ENGINE=MergeTree PARTITION BY toYYYYMM(day)
ORDER BY (source_id, registry, day, run_id, step, bucket);
