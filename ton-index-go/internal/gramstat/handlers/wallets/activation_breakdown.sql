-- Resolve lifetime minima only for this transaction-time window's candidates.
-- Match the complete first-event tuple, including tx hash: a later code change
-- or redeployment must never reclassify an address. Duplicate inserts collapse.
WITH candidates AS (
 SELECT account,lt,mc_seqno,toUInt32(gen_utime) AS utime,toString(tx_hash) AS hash,
  assumeNotNull(toString(init_code_hash)) AS code
 FROM {metrics:Identifier}.deployments
 WHERE gen_utime>=toDateTime({day_from:UInt32},'UTC') AND gen_utime<toDateTime({before:UInt32},'UTC')
 AND mc_seqno BETWEEN {first:UInt32} AND {end:UInt32}
 AND init_code_hash IN {hashes:Array(String)}
 GROUP BY account,lt,mc_seqno,utime,hash,code
), firsts AS (
 SELECT account,minMerge(first) AS initial FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
 AND account IN (SELECT account FROM candidates)
 GROUP BY account
 HAVING initial.2<={end:UInt32} AND initial.3>={day_from:UInt32} AND initial.3<{before:UInt32}
), matched AS (
 SELECT c.account AS account,c.utime AS utime,min(c.code) AS code,uniqExact(c.code) AS codes,max(c.mc_seqno) AS mc
 FROM candidates c INNER JOIN firsts f ON c.account=f.account
 AND tuple(c.lt,c.mc_seqno,c.utime,c.hash)=f.initial
 GROUP BY c.account,c.utime
)
SELECT toString(toDate(toDateTime(utime,'UTC'))) AS day,code,toString(count()) AS activations,
 toUInt32(countIf(codes!=1)) AS ambiguous,max(mc) AS through_mc
FROM matched GROUP BY day,code ORDER BY day,code FORMAT JSON
