INSERT INTO {sink:Identifier}.wallet_activated_checkpoints
SELECT source_id,registry,day,run_id,{step:UInt32},
 intDiv(utime,{step:UInt32})*{step:UInt32},max(mc_seqno),
 argMax(utime,mc_seqno),argMax(activated,mc_seqno)
FROM {sink:Identifier}.wallet_activations_mc
WHERE source_id={source:String} AND registry={registry:String}
 AND day=toDate({day:String}) AND run_id={run:UUID}
GROUP BY source_id,registry,day,run_id,intDiv(utime,{step:UInt32})*{step:UInt32}
