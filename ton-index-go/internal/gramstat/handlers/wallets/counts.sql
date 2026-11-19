-- Dense zero buckets exist only inside a completed source day. Missing days
-- never produce zero rows at read time.
INSERT INTO {sink:Identifier}.wallet_counts
WITH counts AS (
 SELECT step,bucket,uniqExactMerge(addresses) AS active
 FROM {sink:Identifier}.wallet_activity
 WHERE source_id={source:String} AND registry={registry:String}
  AND day=toDate({day:String}) AND run_id={run:UUID}
 GROUP BY step,bucket
), grid AS (
 SELECT toUInt32(step) AS step,
  toUInt32(toUnixTimestamp(toDateTime({day:String},'UTC'))+i*step) AS bucket
 FROM (SELECT arrayJoin([60,3600,86400]) AS step)
 ARRAY JOIN range(toUInt32(86400/step)) AS i
)
SELECT {source:String},{registry:String},toDate({day:String}),{run:UUID},
 g.step,g.bucket,ifNull(c.active,toUInt64(0))
FROM grid g LEFT JOIN counts c ON g.step=c.step AND g.bucket=c.bucket
