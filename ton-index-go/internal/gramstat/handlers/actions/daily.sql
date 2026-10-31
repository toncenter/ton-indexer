WITH payments AS (
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
            END AS liquid_pool,
            CASE WHEN destination = '0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A'
                 THEN CASE WHEN type = 'ton_transfer' THEN (ton_transfer_data).content
                           WHEN type = 'jetton_transfer'
                             AND asset = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE'
                           THEN (jetton_transfer_data).comment END
            END AS stars_comment
    FROM public.actions
    WHERE trace_mc_seqno_end >= $1 AND trace_mc_seqno_end <= $2
      AND start_utime >= $3 AND start_utime < $4
      AND success = true
      AND (type IN ('jetton_transfer', 'nft_transfer', 'jetton_swap', 'stake_deposit', 'nft_mint')
           OR (type = 'ton_transfer'
               AND destination = '0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A'))
), parsed AS MATERIALIZED (
    -- PostgreSQL otherwise inlines the regex into every use of the bucket key.
    -- Materialize once; all grouping sets still share one indexed actions scan.
    SELECT *, CASE WHEN stars_comment LIKE '%Telegram Stars%' THEN
        -- Fractional quantities are not integral Stars. Accept the observed .0
        -- notation, but never infer quantities from the TON/USDT payment value.
        regexp_match(stars_comment,
            '^([0-9]+)(?:[.]0+)?[[:space:]]+(?:Prepaid[[:space:]]+)?Telegram Stars(?:[[:space:]]|$)')
        END AS stars_parts
    FROM payments
), quantities AS (
    SELECT *, CASE WHEN length(ltrim(stars_parts[1], '0')) <= 20
                  THEN coalesce(nullif(ltrim(stars_parts[1], '0'), ''), '0')::numeric
             END AS stars_quantity
    FROM parsed
), src AS (
    SELECT *, CASE WHEN stars_comment LIKE '%Telegram Stars%' THEN jsonb_build_object(
        'payment_asset', CASE WHEN type = 'ton_transfer' THEN 'TON' ELSE 'USDT' END,
        'product', CASE WHEN stars_comment LIKE '%Prepaid Telegram Stars%' THEN 'prepaid' ELSE 'standard' END,
        'stars_per_purchase', CASE WHEN stars_quantity > 0 AND stars_quantity <= 18446744073709551615
                                   THEN stars_quantity::text END,
        'quantity_status', CASE WHEN stars_parts IS NULL THEN 'missing_or_unrecognized_quantity'
                                WHEN stars_quantity = 0 THEN 'zero_quantity'
                                WHEN stars_quantity IS NULL OR stars_quantity > 18446744073709551615 THEN 'out_of_range'
                                ELSE 'known' END
    ) END AS stars_bucket
    FROM quantities
), g AS (
    SELECT day, dex, liquid_pool, stars_bucket, grouping(dex, liquid_pool, stars_bucket) AS kind,
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
            ) AS dns_mints,
           count(*) FILTER (WHERE stars_bucket IS NOT NULL) AS stars_purchases
    FROM src
    GROUP BY GROUPING SETS ((day), (day, dex), (day, liquid_pool), (day, stars_bucket))
)
SELECT day,
        max(jetton_senders) FILTER (WHERE kind = 7),
        max(jetton_receivers) FILTER (WHERE kind = 7),
        max(jetton_transfers) FILTER (WHERE kind = 7),
        max(nft_senders) FILTER (WHERE kind = 7),
        max(nft_receivers) FILTER (WHERE kind = 7),
        max(nft_transfers) FILTER (WHERE kind = 7),
        max(swaps) FILTER (WHERE kind = 7),
        max(swap_users) FILTER (WHERE kind = 7),
        coalesce(jsonb_object_agg(dex, swaps) FILTER (WHERE kind = 3 AND dex IS NOT NULL), '{}'::jsonb),
        max(deposits) FILTER (WHERE kind = 7),
        (max(deposit_nanoton) FILTER (WHERE kind = 7))::text,
       coalesce(jsonb_object_agg(liquid_pool, jsonb_build_object(
           'count', deposits, 'amount_nanoton', deposit_nanoton::text
            )) FILTER (WHERE kind = 5 AND liquid_pool IS NOT NULL), '{}'::jsonb),
        max(dns_mints) FILTER (WHERE kind = 7),
        max(stars_purchases) FILTER (WHERE kind = 7),
        coalesce(jsonb_agg(stars_bucket || jsonb_build_object('purchases', stars_purchases))
                 FILTER (WHERE kind = 6 AND stars_bucket IS NOT NULL), '[]'::jsonb)
FROM g
GROUP BY day ORDER BY day
