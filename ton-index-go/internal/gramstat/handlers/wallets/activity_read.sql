-- step is 60/3600/86400; from/to are aligned Unix seconds, to exclusive.
-- Missing or unverified days remain absent, never synthesized as zero.
WITH published AS (
 SELECT day,argMax(run_id,tuple(through_mc,calculated_at,run_id)) AS run_id
 FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1
 AND day >= toDate(toDateTime({from:UInt32},'UTC'))
 AND day <= toDate(toDateTime({to:UInt32}-1,'UTC')) GROUP BY day
)
SELECT bucket,toString(active) AS active
FROM {sink:Identifier}.wallet_counts
WHERE source_id={source:String} AND registry={registry:String}
 AND step={step:UInt32} AND bucket >= {from:UInt32} AND bucket < {to:UInt32}
 AND (day,run_id) IN (SELECT day,run_id FROM published)
ORDER BY bucket FORMAT JSON
