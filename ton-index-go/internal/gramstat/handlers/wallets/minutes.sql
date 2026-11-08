-- Restrict first-deployment lookup to this chunk's senders. Message retries and
-- several messages from one transaction collapse before the join.
INSERT INTO {sink:Identifier}.wallet_activity
WITH sends AS (
 SELECT tx_account AS account, intDiv(tx_now,60)*60 AS bucket, max(tx_lt) AS last_lt
 FROM {raw:Identifier}.messages
 PREWHERE tx_lt >= {lo:UInt64} AND tx_lt < {hi:UInt64}
 WHERE tx_now >= {from:UInt32} AND tx_now < {to:UInt32}
  AND mc_block_seqno <= {end:UInt32}
  AND direction='out' AND source=tx_account
  AND (bounced=false OR isNull(bounced))
 GROUP BY account,bucket
), wallets AS (
 SELECT account, minMerge(first) AS initial
 FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
  AND account IN (SELECT account FROM sends)
 GROUP BY account
)
SELECT {source:String}, {registry:String}, toDate({day:String}), {run:UUID},
 toUInt32(60), bucket, uniqExactState(s.account)
FROM sends s INNER JOIN wallets w ON s.account=w.account
WHERE last_lt >= initial.1 AND initial.2 <= {end:UInt32}
GROUP BY bucket
