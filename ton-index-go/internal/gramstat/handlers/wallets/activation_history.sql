-- Historical format upgrades resolve the whole lifetime minimum in ONE ordered
-- pass over indexed deployments. No candidate-account set or large join, and no
-- repeated lifetime scan for each month. Never restrict the input's lower time
-- bound: an earlier qualifying deployment must exclude every redeployment.
WITH firsts AS (
 SELECT account,
  min(tuple(lt,mc_seqno,toUInt32(gen_utime),toString(tx_hash),
      indexOf({hashes:Array(String)},assumeNotNull(toString(init_code_hash))))) AS initial,
  min(tuple(lt,mc_seqno,toUInt32(gen_utime),toString(tx_hash),
      -toInt16(indexOf({hashes:Array(String)},assumeNotNull(toString(init_code_hash)))))) AS reverse_code
 FROM {metrics:Identifier}.deployments
 WHERE mc_seqno BETWEEN {first:UInt32} AND {end:UInt32}
 AND gen_utime<toDateTime({before:UInt32},'UTC')
 AND init_code_hash IN {hashes:Array(String)}
 GROUP BY account
 HAVING initial.3>={day_from:UInt32} AND initial.3<{before:UInt32}
)
SELECT toString(toDate(toDateTime(initial.3,'UTC'))) AS day,
 {hashes:Array(String)}[initial.5] AS code,toString(count()) AS activations,
 toUInt32(countIf(toInt16(initial.5)!=-reverse_code.5)) AS ambiguous,max(initial.2) AS through_mc
FROM firsts GROUP BY day,code ORDER BY day,code FORMAT JSON
