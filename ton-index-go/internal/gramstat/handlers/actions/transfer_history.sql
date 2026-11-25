-- Count-only historical upgrade: no distinct-address sets or other action types.
-- Same start-time window, source-head MC and ancestor registry as daily.sql.
SELECT (to_timestamp(start_utime) AT TIME ZONE 'UTC')::date AS day,
       count(*) FILTER (WHERE type='jetton_transfer') AS jettons,
       count(*) FILTER (WHERE type='jetton_transfer' AND asset='0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE') AS usdt,
       count(*) FILTER (WHERE type='jetton_transfer' AND asset IS NULL) AS unknown_asset,
       count(*) FILTER (WHERE type='ton_transfer') AS grams,
       count(*) FILTER (WHERE type='ton_transfer' AND value>=1000000000) AS gram_ge_one,
       count(*) FILTER (WHERE type='ton_transfer' AND value IS NULL) AS unknown_value
FROM public.actions
WHERE trace_mc_seqno_end BETWEEN $1 AND $2 AND start_utime >= $3 AND start_utime < $4
  AND success AND type IN ('ton_transfer','jetton_transfer')
  AND (type='jetton_transfer' OR NOT (coalesce(ancestor_type,ARRAY[]::varchar[]) && $5::varchar[]))
GROUP BY 1
