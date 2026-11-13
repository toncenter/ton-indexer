-- Aggregator-owned preparation: run with a writer role after auditing a complete bounded replay.
-- Date bounds are exclusive at the top; both edge days remain conservative partials
-- because shard block timestamps can straddle the including-MC range boundaries.
INSERT INTO gramstat.chain_daily
WITH flows AS (
 SELECT toDate(toDateTime(gen_utime, 'UTC')) AS day,
        sum(transaction_count) AS transactions,
        if(count(transaction_fees + import_fees) = count(), sum(transaction_fees + import_fees), NULL) AS fees,
        if(count(created) = count(), sum(created), NULL) AS minted,
        if(count(burned) = count(), sum(burned), NULL) AS burned
 FROM {raw:Identifier}.blocks FINAL
 WHERE gen_utime >= toUnixTimestamp(toDateTime({from:Date}, 'UTC'))
   AND gen_utime < toUnixTimestamp(toDateTime({to:Date}, 'UTC')) GROUP BY day
), supply AS (
 SELECT toDate(gen_utime, 'UTC') AS day, argMax(total, tuple(gen_utime, mc_seqno)) AS amount,
        max(toUnixTimestamp(gen_utime)) AS at
 FROM {metrics:Identifier}.total_supply FINAL
 WHERE gen_utime >= toDateTime({from:Date}, 'UTC') AND gen_utime < toDateTime({to:Date}, 'UTC') GROUP BY day
)
SELECT f.day, f.transactions, f.fees, f.minted, f.burned, s.amount, s.at,
       toUInt8(f.day = {from:Date} OR f.day = {to:Date} - 1),
       {mc_from:UInt32}, {mc_to:UInt32}, {source:String}, now64(6)
FROM flows AS f LEFT JOIN supply AS s USING(day)
SETTINGS join_use_nulls=1, max_threads=2, max_execution_time=120;
