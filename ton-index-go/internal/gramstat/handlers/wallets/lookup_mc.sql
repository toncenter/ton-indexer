INSERT INTO {sink:Identifier}.wallet_first_mc
SELECT {source:String},{registry:String},{lookup:UUID},mc_seqno,n,
 sum(n) OVER (ORDER BY mc_seqno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (
 SELECT first_mc AS mc_seqno,count() AS n
 FROM {sink:Identifier}.wallet_first_lookup
 WHERE source_id={source:String} AND registry={registry:String} AND lookup_id={lookup:UUID}
 GROUP BY first_mc
)
