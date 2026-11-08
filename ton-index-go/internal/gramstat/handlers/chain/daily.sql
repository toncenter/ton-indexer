-- One deduplicated block scan. MC timestamps are monotonic: a day's last MC
-- therefore has exactly the cumulative burn of all MC days through that day.
-- Check this ordering and the complete MC/delta prefix before publishing.
WITH days AS (
 SELECT toDate(toDateTime(gen_utime, 'UTC')) AS day,
        sum(transaction_count) AS transactions,
        if(count(transaction_fees + import_fees) = count(), sum(transaction_fees + import_fees), NULL) AS fees,
        if(count(created) = count(), sum(created), NULL) AS minted,
        if(count(burned) = count(), sum(burned), NULL) AS burned_amount,
        countIf(workchain = -1) AS mc_count,
        minIf(seqno, workchain = -1) AS first_mc,
        maxIf(seqno, workchain = -1) AS last_mc,
        sumIf(burned, workchain = -1) AS mc_burn,
        countIf(workchain = -1 AND burned IS NULL) AS missing_burn,
        argMaxIf(tuple(global_balance, gen_utime, seqno), tuple(gen_utime, seqno), workchain = -1) AS snapshot
 FROM {raw:Identifier}.blocks FINAL
 WHERE mc_block_seqno BETWEEN 3 AND {end:UInt32}
 GROUP BY day
), deltas AS (
 SELECT toDate(gen_utime, 'UTC') AS day, count() AS delta_count
 FROM {metrics:Identifier}.account_deltas FINAL WHERE mc_seqno BETWEEN 3 AND {end:UInt32}
 GROUP BY day
), prefixes AS (
 SELECT *, sum(mc_count) OVER w AS prefix_count,
        sum(delta_count) OVER w AS prefix_deltas,
        sum(mc_burn) OVER w AS cumulative_burn,
        sum(missing_burn) OVER w AS missing_prefix_burn,
        min(first_mc) OVER w AS prefix_start,
        max(last_mc) OVER w AS prefix_end,
        min(day) OVER () AS first_day, max(day) OVER () AS last_day
 FROM days LEFT JOIN deltas USING(day) WHERE mc_count > 0
 WINDOW w AS (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT day, toString(transactions) AS transactions,
       toString(fees) AS fees_nanoton, toString(minted) AS minted_nanoton,
       toString(burned_amount) AS burned_nanoton,
       toString(multiIf(snapshot.1 IS NULL, NULL,
          snapshot.3 >= 103190382 OR snapshot.3 < 30410593, snapshot.1,
          missing_prefix_burn > 0, NULL,
          snapshot.1 - cumulative_burn)) AS supply_nanoton,
       toUInt32(snapshot.2) AS supply_utime,
       toUInt8(day = first_day OR day >= last_day - 1) AS is_partial,
       toUInt32(3) AS source_mc_from, {end:UInt32} AS source_mc_to,
       'mainnet-full-v1' AS source_id,
       formatDateTime(now64(6), '%Y-%m-%d %H:%i:%S.%f', 'UTC') AS calculated_at,
       toUInt8(prefix_start = 3 AND prefix_count = toUInt64(last_mc) - 2
          AND prefix_deltas = prefix_count AND delta_count = mc_count
          AND snapshot.3 = last_mc AND prefix_end = last_mc) AS coverage_ok,
       last_mc
FROM prefixes ORDER BY day FORMAT JSONEachRow
