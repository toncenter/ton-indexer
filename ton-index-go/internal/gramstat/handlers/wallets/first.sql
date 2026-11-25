INSERT INTO {sink:Identifier}.wallet_first
SELECT {source:String}, {registry:String}, account,
 initializeAggregation('minState', tuple(lt, mc_seqno, toUInt32(gen_utime), toString(tx_hash)))
FROM {metrics:Identifier}.deployments
WHERE mc_seqno > {previous:UInt32} AND mc_seqno <= {end:UInt32}
 AND init_code_hash IN {hashes:Array(String)}
-- Stream singleton states; background merges and minMerge resolve address minima.
-- A global GROUP BY on tens of millions of addresses is unnecessary here.
SETTINGS min_insert_block_size_rows=65536, min_insert_block_size_bytes=16777216,
 max_insert_block_size=65536, max_block_size=8192
