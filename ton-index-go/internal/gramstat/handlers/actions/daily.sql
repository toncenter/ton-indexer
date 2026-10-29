WITH src AS (
    SELECT (to_timestamp(start_utime) AT TIME ZONE 'UTC')::date AS day,
           type, source, destination, amount, asset,
           CASE WHEN type = 'jetton_swap'
                THEN coalesce(nullif((jetton_swap_data).dex, ''), 'unknown')
           END AS dex,
           CASE WHEN type = 'stake_deposit' AND (staking_data).provider = 'liquid_staking'
                THEN CASE destination
                    WHEN '0:A45B17F28409229B78360E3290420F13E4FE20F90D7E2BF8C4AC6703259E22FA' THEN 'tonstakers'
                    WHEN '0:F6FF877DD4CE1355B101572045F09D54C29309737EB52CA542CFA6C195F7CC5B' THEN 'stakee'
                    WHEN '0:3D1F01190FFB4E35F573EA5252D0F54A479BE2122106CA3F40B907B01905675C' THEN 'kton'
                    WHEN '0:9FE32DD92875447A6B83D031D0230C539752152B660D4A056797BE9AD2E396D5' THEN 'sgram'
                END
           END AS liquid_pool
    FROM public.actions
    WHERE trace_mc_seqno_end >= $1 AND trace_mc_seqno_end <= $2
      AND start_utime >= $3 AND start_utime < $4
      AND success = true
      AND type IN ('jetton_transfer', 'nft_transfer', 'jetton_swap', 'stake_deposit', 'nft_mint')
), g AS (
    SELECT day, dex, liquid_pool, grouping(dex, liquid_pool) AS kind,
           count(DISTINCT source) FILTER (WHERE type = 'jetton_transfer') AS jetton_senders,
           count(DISTINCT destination) FILTER (WHERE type = 'jetton_transfer') AS jetton_receivers,
           count(*) FILTER (WHERE type = 'jetton_transfer') AS jetton_transfers,
           count(DISTINCT source) FILTER (WHERE type = 'nft_transfer') AS nft_senders,
           count(DISTINCT destination) FILTER (WHERE type = 'nft_transfer') AS nft_receivers,
           count(*) FILTER (WHERE type = 'nft_transfer') AS nft_transfers,
           count(*) FILTER (WHERE type = 'jetton_swap') AS swaps,
           count(DISTINCT source) FILTER (WHERE type = 'jetton_swap') AS swap_users,
           count(*) FILTER (WHERE liquid_pool IS NOT NULL) AS deposits,
           CASE WHEN count(*) FILTER (WHERE liquid_pool IS NOT NULL AND amount IS NULL) > 0
                THEN NULL
                ELSE coalesce(sum(amount) FILTER (WHERE liquid_pool IS NOT NULL), 0)
           END AS deposit_nanoton,
           count(*) FILTER (
               WHERE type = 'nft_mint'
                 AND asset = '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
           ) AS dns_mints
    FROM src
    GROUP BY GROUPING SETS ((day), (day, dex), (day, liquid_pool))
)
SELECT day,
       max(jetton_senders) FILTER (WHERE kind = 3),
       max(jetton_receivers) FILTER (WHERE kind = 3),
       max(jetton_transfers) FILTER (WHERE kind = 3),
       max(nft_senders) FILTER (WHERE kind = 3),
       max(nft_receivers) FILTER (WHERE kind = 3),
       max(nft_transfers) FILTER (WHERE kind = 3),
       max(swaps) FILTER (WHERE kind = 3),
       max(swap_users) FILTER (WHERE kind = 3),
       coalesce(jsonb_object_agg(dex, swaps) FILTER (WHERE kind = 1 AND dex IS NOT NULL), '{}'::jsonb),
       max(deposits) FILTER (WHERE kind = 3),
       (max(deposit_nanoton) FILTER (WHERE kind = 3))::text,
       coalesce(jsonb_object_agg(liquid_pool, jsonb_build_object(
           'count', deposits, 'amount_nanoton', deposit_nanoton::text
       )) FILTER (WHERE kind = 2 AND liquid_pool IS NOT NULL), '{}'::jsonb),
       max(dns_mints) FILTER (WHERE kind = 3)
FROM g
GROUP BY day ORDER BY day
