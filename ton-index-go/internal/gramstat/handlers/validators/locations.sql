WITH observed AS (
    SELECT r.utime_since, r.idx,
           count() AS samples,
           uniqExact(n.ipv4) AS ip_versions,
           uniqExactIf(n.geoip_country_iso,
               n.geoip_country_iso IS NOT NULL AND n.geoip_country_iso != '') AS country_versions,
           uniqExact(tuple(n.created_timestamp,n.record_id,n.ipv4,n.port,
               n.geoip_country_iso,n.geoip_latitude,n.geoip_longitude,n.geoip_city))
             - uniqExact(tuple(n.created_timestamp,n.record_id)) AS ordering_conflicts,
           argMax(tuple(n.created_timestamp,n.record_id,n.geoip_country_iso,
                        n.geoip_latitude,n.geoip_longitude,n.geoip_city),
                  tuple(n.created_timestamp,n.record_id)) AS latest
    FROM session_stats_mainnet_v2.node_addresses n
    ALL INNER JOIN roster r ON upper(n.adnl_addr) = r.adnl_addr
    WHERE n.created_timestamp >= toDateTime({from:UInt32}, 'UTC')
      AND n.created_timestamp < toDateTime({to:UInt32}, 'UTC')
      AND toUnixTimestamp(n.created_timestamp) >= r.utime_since
      AND toUnixTimestamp(n.created_timestamp) < r.cutoff
    GROUP BY r.utime_since, r.idx
)
SELECT r.utime_since, r.idx, r.pubkey,
       o.samples, o.ip_versions, o.country_versions, o.ordering_conflicts,
       if(o.samples=0,NULL,toUnixTimestamp(tupleElement(o.latest,1))) AS observed_at,
       if(o.samples=0,NULL,tupleElement(o.latest,2)) AS record_id,
       tupleElement(o.latest,3) AS country,
       tupleElement(o.latest,4) AS latitude,
       tupleElement(o.latest,5) AS longitude,
       tupleElement(o.latest,6) AS city
FROM roster r LEFT JOIN observed o ON r.utime_since=o.utime_since AND r.idx=o.idx
ORDER BY r.utime_since, r.idx
FORMAT JSONEachRow
