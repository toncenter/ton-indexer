WITH hours AS (
 SELECT toStartOfHour(toDateTime(gen_utime, 'UTC')) AS hour,
        min(seqno) AS first_mc, max(seqno) AS last_mc, count() AS mc_count,
        min(gen_utime) AS first_utime,
        sum(burned) AS burn, countIf(burned IS NULL) AS missing,
        argMax(tuple(global_balance, gen_utime, seqno), tuple(gen_utime, seqno)) AS snapshot
 FROM {raw:Identifier}.blocks FINAL
 WHERE workchain=-1 AND shard=-9223372036854775808 AND seqno BETWEEN 3 AND {end:UInt32}
 GROUP BY hour
), prefixes AS (
 SELECT *, sum(burn) OVER w AS cumulative, sum(missing) OVER w AS missing_prefix,
        sum(mc_count) OVER w AS prefix_count, min(first_mc) OVER w AS prefix_start,
        max(last_mc) OVER w AS prefix_end
 FROM hours WINDOW w AS (ORDER BY hour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT hour, toUInt32(first_mc), toUInt32(last_mc), toUInt32(first_utime), toUInt32(snapshot.2),
       if(missing_prefix - missing = 0, cumulative - burn, NULL) AS opening_burn,
       if(missing_prefix = 0, cumulative, NULL) AS cumulative_burn,
       multiIf(snapshot.1 IS NULL, NULL,
          last_mc >= 103190382 OR last_mc < 30410593, snapshot.1,
          missing_prefix > 0, NULL, snapshot.1 - cumulative) AS supply_nanoton,
       toUInt32(mc_count), toUInt8(last_mc = {end:UInt32}), {end:UInt32}, now64(6)
FROM prefixes
WHERE throwIf(prefix_start != 3 OR prefix_count != toUInt64(last_mc)-2
              OR prefix_end != last_mc OR snapshot.3 != last_mc,
              'Incomplete or unordered supply checkpoint prefix') = 0
ORDER BY hour
