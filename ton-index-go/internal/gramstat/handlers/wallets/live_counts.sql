-- Current-day exact counts use only candidate senders' first-deployment states.
-- Do not rebuild the lifetime scalar lookup every five minutes.
INSERT INTO {sink:Identifier}.wallet_counts
WITH senders AS (
 SELECT DISTINCT tx_account AS account FROM {raw:Identifier}.messages
 PREWHERE tx_lt >= {lo:UInt64} AND tx_lt < {hi:UInt64}
 WHERE tx_now >= {from:UInt32} AND tx_now < {to:UInt32}
 AND mc_block_seqno BETWEEN {first:UInt32} AND {end:UInt32}
 AND direction='out' AND source=tx_account AND (bounced=false OR isNull(bounced))
), wallets AS (
 SELECT account,minMerge(first) AS initial FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
 AND account IN (SELECT account FROM senders)
 GROUP BY account HAVING initial.2 <= {end:UInt32}
), counts AS (
 SELECT arrayJoin([toUInt32(3600),toUInt32(86400)]) AS step,
 intDiv(m.tx_now,step)*step AS bucket,uniqExact(m.tx_account) AS active
 FROM {raw:Identifier}.messages m INNER JOIN wallets w ON m.tx_account=w.account
 PREWHERE m.tx_lt >= {lo:UInt64} AND m.tx_lt < {hi:UInt64}
 WHERE m.tx_now >= {from:UInt32} AND m.tx_now < {to:UInt32}
 AND m.mc_block_seqno BETWEEN {first:UInt32} AND {end:UInt32}
 AND m.direction='out' AND m.source=m.tx_account AND (m.bounced=false OR isNull(m.bounced))
 AND m.tx_lt >= w.initial.1 GROUP BY step,bucket
), grid AS (
 SELECT step,{from:UInt32}+i*step AS bucket
 FROM (SELECT arrayJoin([toUInt32(3600),toUInt32(86400)]) AS step)
 ARRAY JOIN range(toUInt32(intDiv({to:UInt32}-{from:UInt32}+step-1,step))) AS i
)
SELECT {source:String},{registry:String},toDate({day:String}),{run:UUID},
 g.step,g.bucket,ifNull(c.active,toUInt64(0))
FROM grid g LEFT JOIN counts c ON g.step=c.step AND g.bucket=c.bucket
