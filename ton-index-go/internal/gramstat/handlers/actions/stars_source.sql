WITH src AS (
    SELECT (to_timestamp(a.start_utime) AT TIME ZONE 'UTC')::date AS day,
           a.start_utime / 3600 * 3600 AS stars_hour, a.type,
           CASE WHEN a.type = 'ton_transfer' THEN a.value ELSE a.amount END AS stars_paid,
           CASE WHEN a.type = 'ton_transfer' THEN (a.ton_transfer_data).content
                ELSE (a.jetton_transfer_data).comment END AS stars_comment
    FROM public.action_accounts aa
    JOIN public.actions a ON a.trace_id = aa.trace_id AND a.action_id = aa.action_id
        AND a.trace_mc_seqno_end = aa.trace_mc_seqno_end
        AND a.trace_end_lt = aa.trace_end_lt AND a.end_lt = aa.action_end_lt
    WHERE aa.account = '0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A'
      AND aa.trace_end_utime >= $3
      AND a.trace_mc_seqno_end >= $1 AND a.trace_mc_seqno_end <= $2
      AND a.start_utime >= $3 AND a.start_utime < $4
      AND a.destination = '0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A'
      AND a.success = true
      AND (a.type = 'ton_transfer' OR (a.type = 'jetton_transfer'
           AND a.asset = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE'))
)
