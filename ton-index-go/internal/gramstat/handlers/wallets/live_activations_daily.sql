-- Transactions use their own UTC date, not the including MC's date. Candidate
-- deployments may be redeployments; only their lifetime minimum can qualify.
SELECT toString(count()) AS activations FROM (
 SELECT account,minMerge(first) AS initial FROM {sink:Identifier}.wallet_first
 WHERE source_id={source:String} AND registry={registry:String}
 AND account IN (SELECT account FROM {metrics:Identifier}.deployments
  WHERE mc_seqno BETWEEN {day_first_mc:UInt32} AND {end:UInt32}
  AND gen_utime>=toDateTime({day_from:UInt32},'UTC') AND gen_utime<toDateTime({before:UInt32},'UTC')
  AND init_code_hash IN {hashes:Array(String)})
 GROUP BY account
 HAVING initial.2<={end:UInt32} AND initial.3>={day_from:UInt32} AND initial.3<{before:UInt32}
) FORMAT JSONEachRow
