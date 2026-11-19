-- The certified source epoch makes every MC prefix immutable. Repeated inserts
-- produce the same point; the stream watermark is committed only after all writes.
INSERT INTO {sink:Identifier}.wallet_activation_stream
WITH firsts AS (
 SELECT account,minMerge(first) AS initial FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
 AND account IN (SELECT account FROM {metrics:Identifier}.deployments
  WHERE mc_seqno>{previous:UInt32} AND mc_seqno<={end:UInt32}
  AND init_code_hash IN {hashes:Array(String)})
 GROUP BY account HAVING initial.2>{previous:UInt32} AND initial.2<={end:UInt32}
), events AS (
 SELECT initial.2 AS mc_seqno,count() AS n FROM firsts GROUP BY mc_seqno
), blocks AS (
 SELECT toUInt32(seqno) AS mc_seqno,toUInt32(gen_utime) AS utime
 FROM {raw:Identifier}.blocks FINAL
 WHERE workchain=-1 AND shard=-9223372036854775808
 AND seqno>{previous:UInt32} AND seqno<={end:UInt32}
)
SELECT {source:String},{registry:String},toDate(toDateTime(utime,'UTC')),
 mc_seqno,utime,{opening:UInt64}+sum(ifNull(n,toUInt64(0))) OVER (ORDER BY mc_seqno)
FROM blocks LEFT JOIN events USING(mc_seqno) ORDER BY mc_seqno
