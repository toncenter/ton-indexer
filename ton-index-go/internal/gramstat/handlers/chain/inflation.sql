WITH flows AS (
 SELECT toStartOfHour(toDateTime(gen_utime,'UTC')) AS hour, count() AS flow_count,
        sum(created) AS minted, countIf(created IS NULL) AS missing_minted,
        sum(burned) AS burned_amount, countIf(burned IS NULL) AS missing_burned
 FROM {raw:Identifier}.blocks FINAL
 WHERE mc_block_seqno BETWEEN 3 AND {end:UInt32}
 GROUP BY hour
), hourly AS (
 SELECT s.hour, s.supply_nanoton, s.last_utime, s.last_mc, s.is_partial,
        f.minted, f.burned_amount, f.missing_minted, f.missing_burned, f.flow_count,
        min(s.hour) OVER () AS source_first_hour
 FROM gramstat.supply_hourly AS s FINAL
 LEFT JOIN flows AS f USING(hour)
 WHERE s.source_mc_to = {end:UInt32}
), windows AS (
 SELECT *, count() OVER w AS hours,
        min(hour) OVER w AS first_hour,
        sum(minted) OVER w AS minted_sum,
        sum(burned_amount) OVER w AS burned_sum,
        sum(missing_minted) OVER w AS missing_minted_sum,
        sum(missing_burned) OVER w AS missing_burned_sum,
        countIf(flow_count = 0 OR minted IS NULL OR burned_amount IS NULL) OVER w AS missing_hours
 FROM hourly
 WINDOW w AS (ORDER BY toUInt32(hour) RANGE BETWEEN 82800 PRECEDING AND CURRENT ROW)
), amounts AS (
 SELECT *,
        hours = 24 AND first_hour > source_first_hour AND missing_hours = 0 AS complete,
        if(complete AND missing_minted_sum = 0,minted_sum,NULL) AS minted_24h,
        if(complete AND missing_burned_sum = 0,burned_sum,NULL) AS burned_24h
 FROM windows
)
SELECT hour + INTERVAL 1 HOUR AS point_at, hour - INTERVAL 23 HOUR AS window_from,
       minted_24h, burned_24h, supply_nanoton, last_utime, last_mc,
       if(minted_24h IS NOT NULL AND burned_24h IS NOT NULL AND supply_nanoton > 0,
          divideDecimal(toDecimal256((minted_24h-burned_24h)*36500,12),
                        toDecimal256(if(supply_nanoton > 0,supply_nanoton,1),12),12), NULL),
       {end:UInt32}, now64(6)
FROM amounts WHERE is_partial = 0 ORDER BY point_at
