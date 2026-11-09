INSERT INTO {sink:Identifier}.wallet_monthly
WITH published AS (
 SELECT day,argMax(run_id,tuple(through_mc,calculated_at,run_id)) AS run
 FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1
 AND day BETWEEN toDate({monthly_day:String})-29 AND toDate({monthly_day:String})
 GROUP BY day
), snapshot AS (
 SELECT count() AS days,arrayMap(x->x.2,arraySort(groupArray((day,run)))) AS runs FROM published
), total AS (
 SELECT uniqExactMerge(addresses) AS active FROM {sink:Identifier}.wallet_activity
 PREWHERE source_id={source:String} AND registry={registry:String} AND step=86400
 AND day BETWEEN toDate({monthly_day:String})-29 AND toDate({monthly_day:String})
 WHERE (day,run_id) IN (SELECT day,run FROM published)
)
SELECT {source:String},{registry:String},toDate({monthly_day:String}),active,runs,now64(6)
FROM total CROSS JOIN snapshot WHERE days=30
