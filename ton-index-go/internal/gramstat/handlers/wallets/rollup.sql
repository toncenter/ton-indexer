INSERT INTO {sink:Identifier}.wallet_activity
SELECT source_id, registry, day, run_id, {step:UInt32},
 intDiv(bucket,{step:UInt32})*{step:UInt32}, uniqExactMergeState(addresses)
FROM {sink:Identifier}.wallet_activity
WHERE source_id={source:String} AND registry={registry:String}
 AND day=toDate({day:String}) AND run_id={run:UUID} AND step=60
GROUP BY source_id,registry,day,run_id,intDiv(bucket,{step:UInt32})*{step:UInt32}
