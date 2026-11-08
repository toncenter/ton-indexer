-- MC chart time is the including MC's time; daily activations use tx.now.
INSERT INTO {sink:Identifier}.wallet_activations_mc
WITH firsts AS (
 SELECT minMerge(first) AS initial FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
 GROUP BY account
), events AS (
 SELECT initial.2 AS mc_seqno,count() AS n FROM firsts
 WHERE initial.2 BETWEEN {first_mc:UInt32} AND {last_mc:UInt32}
 GROUP BY mc_seqno
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
