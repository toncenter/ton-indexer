-- Filter the compact scalar lookup by distinct senders, not sender/minute pairs.
-- Check eligibility on each actual message; uniqExact handles repeated messages
-- without materializing a large (address, minute, max LT) intermediate twice.
INSERT INTO {sink:Identifier}.wallet_activity
WITH senders AS (
 SELECT DISTINCT tx_account AS account
 FROM {raw:Identifier}.messages
 PREWHERE tx_lt >= {lo:UInt64} AND tx_lt < {hi:UInt64}
 WHERE tx_now >= {from:UInt32} AND tx_now < {to:UInt32}
  AND mc_block_seqno <= {end:UInt32}
  AND direction='out' AND source=tx_account
  AND (bounced=false OR isNull(bounced))
), wallets AS (
 SELECT account,first_lt
 FROM {sink:Identifier}.wallet_first_lookup
 WHERE source_id={source:String} AND registry={registry:String}
  AND lookup_id={lookup:UUID} AND first_mc <= {end:UInt32}
  AND account IN (SELECT account FROM senders)
)
SELECT {source:String}, {registry:String}, toDate({day:String}), {run:UUID},
 toUInt32(60), intDiv(m.tx_now,60)*60 AS bucket, uniqExactState(m.tx_account)
FROM {raw:Identifier}.messages m INNER JOIN wallets w ON m.tx_account=w.account
PREWHERE m.tx_lt >= {lo:UInt64} AND m.tx_lt < {hi:UInt64}
WHERE m.tx_now >= {from:UInt32} AND m.tx_now < {to:UInt32}
 AND m.mc_block_seqno <= {end:UInt32}
 AND m.direction='out' AND m.source=m.tx_account
 AND (m.bounced=false OR isNull(m.bounced))
 AND m.tx_lt >= w.first_lt
GROUP BY bucket
