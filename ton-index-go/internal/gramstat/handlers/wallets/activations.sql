-- MC chart time is the including MC's time; daily activations use tx.now.
INSERT INTO {sink:Identifier}.wallet_activations_mc
WITH events AS (
 SELECT mc_seqno,activations AS n FROM {sink:Identifier}.wallet_first_mc
 WHERE source_id={source:String} AND registry={registry:String} AND lookup_id={lookup:UUID}
 AND mc_seqno BETWEEN {first_mc:UInt32} AND {last_mc:UInt32}
), blocks AS (
 SELECT toUInt32(seqno) AS mc_seqno,toUInt32(gen_utime) AS utime
 FROM {raw:Identifier}.blocks FINAL
 WHERE workchain=-1 AND shard=-9223372036854775808
 AND seqno BETWEEN {first_mc:UInt32} AND {last_mc:UInt32}
)
SELECT {source:String},{registry:String},toDate({day:String}),{run:UUID},
 mc_seqno,utime,ifNull(n,toUInt64(0)),
 {opening:UInt64}+sum(ifNull(n,toUInt64(0))) OVER (ORDER BY mc_seqno)
FROM blocks LEFT JOIN events USING(mc_seqno)
ORDER BY mc_seqno
