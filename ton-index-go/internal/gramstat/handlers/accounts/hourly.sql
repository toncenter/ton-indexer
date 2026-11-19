WITH hours AS (
 SELECT toStartOfHour(gen_utime) AS hour, min(mc_seqno) AS first_mc, max(mc_seqno) AS last_mc,
        count() AS n, min(toUInt32(gen_utime)) AS first_utime,
        argMax(tuple(toUInt32(gen_utime),mc_seqno),tuple(gen_utime,mc_seqno)) AS last,
        sum(delta_uninit) AS du, sum(delta_active) AS da, sum(delta_frozen) AS df
 FROM {metrics:Identifier}.account_deltas FINAL
 WHERE mc_seqno > {baseline_mc:UInt32} AND mc_seqno <= {end:UInt32}
 GROUP BY hour
), prefixes AS (
 SELECT *, sum(n) OVER w AS prefix_count, min(first_mc) OVER w AS prefix_start,
        max(last_mc) OVER w AS prefix_end,
        toInt64({baseline_uninit:UInt64}) + sum(du) OVER w AS count_uninit,
        toInt64({baseline_active:UInt64}) + sum(da) OVER w AS count_active,
        toInt64({baseline_frozen:UInt64}) + sum(df) OVER w AS count_frozen
 FROM hours WINDOW w AS (ORDER BY hour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT hour, first_mc, last_mc, first_utime, last.1,
       toUInt64(count_uninit-du), toUInt64(count_active-da), toUInt64(count_frozen-df),
       toUInt64(count_uninit), toUInt64(count_active), toUInt64(count_frozen),
       toUInt32(n), {end:UInt32}, {baseline_id:String}, now64(6)
FROM prefixes
WHERE throwIf(prefix_start != {baseline_mc:UInt32}+1 OR prefix_count != last_mc-{baseline_mc:UInt32}
 OR prefix_end != last_mc OR last.2 != last_mc OR least(count_uninit,count_active,count_frozen,
 count_uninit-du,count_active-da,count_frozen-df)<0,'Invalid Accounts prefix or baseline')=0
ORDER BY hour
