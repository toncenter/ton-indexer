-- interval=0 returns every MC, including MCs sharing a second. Otherwise use
-- aligned intervals: <3600 reads MCs, hour/day multiples read checkpoints.
WITH if({interval:UInt32}>=86400,86400,3600) AS base_step,
 ({interval:UInt32}>=3600 AND modulo({interval:UInt32},base_step)=0) AS use_checkpoints,
 intDiv(toUInt64({from:UInt32})+base_step-1,base_step)*base_step AS inner_from,
 intDiv({to:UInt32},base_step)*base_step AS inner_to,
 published AS (
 SELECT day,argMax(run_id,tuple(through_mc,calculated_at,run_id)) AS run_id
 FROM {sink:Identifier}.wallet_days
 WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1
 AND day >= toDate(toDateTime({from:UInt32},'UTC'))
 AND day <= toDate(toDateTime({to:UInt32}-1,'UTC')) GROUP BY day
), points AS (
 SELECT mc_seqno,utime,activated FROM {sink:Identifier}.wallet_activations_mc
 WHERE (NOT use_checkpoints
 OR (modulo({from:UInt32},base_step)!=0 AND utime<inner_from)
 OR (modulo({to:UInt32},base_step)!=0 AND utime>=inner_to))
 AND source_id={source:String} AND registry={registry:String}
 AND utime >= {from:UInt32} AND utime < {to:UInt32}
 AND (day,run_id) IN (SELECT day,run_id FROM published)
 UNION ALL
 SELECT mc_seqno,utime,activated FROM {sink:Identifier}.wallet_activated_checkpoints
 WHERE use_checkpoints AND source_id={source:String} AND registry={registry:String}
 AND step=base_step AND bucket>=inner_from AND bucket<inner_to
 AND utime >= {from:UInt32} AND utime < {to:UInt32}
 AND (day,run_id) IN (SELECT day,run_id FROM published)
)
SELECT max(p.mc_seqno) AS mc_seqno,argMax(p.utime,p.mc_seqno) AS utime,
 toString(argMax(p.activated,p.mc_seqno)) AS activated
FROM points p
GROUP BY if({interval:UInt32}=0,toUInt64(p.mc_seqno),toUInt64(intDiv(p.utime,greatest({interval:UInt32},1))))
ORDER BY mc_seqno FORMAT JSON
