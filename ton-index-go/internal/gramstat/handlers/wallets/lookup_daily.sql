INSERT INTO {sink:Identifier}.wallet_first_daily
SELECT {source:String},{registry:String},{lookup:UUID},
 toDate(toDateTime(first_utime,'UTC')),count()
FROM {sink:Identifier}.wallet_first_lookup
WHERE source_id={source:String} AND registry={registry:String} AND lookup_id={lookup:UUID}
GROUP BY toDate(toDateTime(first_utime,'UTC'))
