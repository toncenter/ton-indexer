SELECT day,toString(argMax(d.activations,tuple(through_mc,calculated_at,run_id))) AS activations
FROM {sink:Identifier}.wallet_days d
WHERE source_id={source:String} AND registry={registry:String} AND history_verified=1
 AND day >= toDate(toDateTime({from:UInt32},'UTC'))
 AND day < toDate(toDateTime({to:UInt32},'UTC'))
GROUP BY day ORDER BY day FORMAT JSON
