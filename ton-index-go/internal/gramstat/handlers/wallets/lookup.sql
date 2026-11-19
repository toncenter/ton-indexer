INSERT INTO {sink:Identifier}.wallet_first_lookup
SELECT {source:String},{registry:String},{lookup:UUID},account,
 initial.1,initial.2,initial.3
FROM (
 SELECT account,minMerge(first) AS initial
 FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
 GROUP BY account
) WHERE initial.2 <= {end:UInt32}
SETTINGS min_insert_block_size_rows=65536,min_insert_block_size_bytes=16777216,
 max_insert_block_size=65536,max_block_size=8192
