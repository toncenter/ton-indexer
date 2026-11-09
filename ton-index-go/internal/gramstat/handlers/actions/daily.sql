WITH payments AS (
    SELECT (to_timestamp(start_utime) AT TIME ZONE 'UTC')::date AS day,
           start_utime / 3600 * 3600 AS hour,
           type, source, destination, amount, value, asset,
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
), src AS MATERIALIZED (
    -- One source-table scan. Keep only columns needed by the independent
    -- aggregates; in particular, do not retain arbitrary assets or amounts.
    SELECT day, type,
           CASE WHEN type IN ('jetton_transfer','nft_transfer','jetton_swap') THEN source END AS source,
           CASE WHEN type IN ('jetton_transfer','nft_transfer') THEN destination END AS destination,
           CASE WHEN liquid_pool IS NOT NULL THEN amount END AS amount,
           dex, liquid_pool, stars_comment,
           CASE WHEN stars_comment IS NOT NULL THEN hour END AS stars_hour,
           CASE WHEN stars_comment IS NOT NULL
                THEN CASE WHEN type = 'ton_transfer' THEN value ELSE amount END
           END AS stars_paid,
           (type = 'nft_mint' AND asset = '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF') AS dns_mint
    FROM payments
), daily AS (
    -- Unique addresses are needed only here, never for DEX/pool/Stars buckets.
    SELECT day,
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
           count(*) FILTER (WHERE dns_mint) AS dns_mints
    FROM src GROUP BY day
), dex_counts AS (
    SELECT day, dex, count(*) AS swaps FROM src
    WHERE dex IS NOT NULL GROUP BY day, dex
), dex_days AS (
    SELECT day, jsonb_object_agg(dex, swaps) AS breakdown FROM dex_counts GROUP BY day
), pool_counts AS (
    SELECT day, liquid_pool, count(*) AS deposits,
           CASE WHEN count(*) FILTER (WHERE amount IS NULL) > 0 THEN NULL ELSE sum(amount) END AS deposit_nanoton
    FROM src WHERE liquid_pool IS NOT NULL GROUP BY day, liquid_pool
), pool_days AS (
    SELECT day, jsonb_object_agg(liquid_pool, jsonb_build_object(
        'count', deposits, 'amount_nanoton', deposit_nanoton::text)) AS breakdown
    FROM pool_counts GROUP BY day
), parsed AS MATERIALIZED (
    -- Parse once, and materialize only the actual Stars payments rather than
    -- every transfer, swap and mint in the history.
    SELECT day, type, stars_comment, stars_hour, stars_paid,
        -- Fractional quantities are not integral Stars. Accept the observed .0
        -- notation, but never infer quantities from the TON/USDT payment value.
        regexp_match(stars_comment,
            '^([0-9]+)(?:[.]0+)?[[:space:]]+(?:Prepaid[[:space:]]+)?Telegram Stars(?:[[:space:]]|$)')
        AS stars_parts
    FROM src WHERE stars_comment LIKE '%Telegram Stars%'
), quantities AS (
    SELECT *, CASE WHEN length(ltrim(stars_parts[1], '0')) <= 20
                  THEN coalesce(nullif(ltrim(stars_parts[1], '0'), ''), '0')::numeric
             END AS stars_quantity
    FROM parsed
), star_keys AS (
    SELECT day, stars_hour, stars_paid, jsonb_build_object(
        'payment_asset', CASE WHEN type = 'ton_transfer' THEN 'TON' ELSE 'USDT' END,
        'product', CASE WHEN stars_comment LIKE '%Prepaid Telegram Stars%' THEN 'prepaid' ELSE 'standard' END,
        'stars_per_purchase', CASE WHEN stars_quantity > 0 AND stars_quantity <= 18446744073709551615
                                   THEN stars_quantity::text END,
        'quantity_status', CASE WHEN stars_parts IS NULL THEN 'missing_or_unrecognized_quantity'
                                WHEN stars_quantity = 0 THEN 'zero_quantity'
                                WHEN stars_quantity IS NULL OR stars_quantity > 18446744073709551615 THEN 'out_of_range'
                                ELSE 'known' END
    ) AS stars_bucket
    FROM quantities
), star_counts AS (
    -- Lossless compression before transport. Monetary weight is paid * count,
    -- never declared quantity. Exact rational sorting/filtering happens in Go.
    SELECT day, stars_hour, stars_paid, stars_bucket, count(*) AS purchases
    FROM star_keys GROUP BY day, stars_hour, stars_paid, stars_bucket
), star_days AS (
    SELECT day, sum(purchases)::bigint AS purchases,
           jsonb_agg(stars_bucket || jsonb_build_object(
               'hour', stars_hour, 'paid', stars_paid::text, 'purchases', purchases)) AS buckets
    FROM star_counts GROUP BY day
)
SELECT d.day, d.jetton_senders, d.jetton_receivers, d.jetton_transfers,
       d.nft_senders, d.nft_receivers, d.nft_transfers, d.swaps, d.swap_users,
       coalesce(x.breakdown, '{}'::jsonb), d.deposits, d.deposit_nanoton::text,
       coalesce(p.breakdown, '{}'::jsonb), d.dns_mints,
       coalesce(s.purchases, 0), coalesce(s.buckets, '[]'::jsonb)
FROM daily d
LEFT JOIN dex_days x USING (day)
LEFT JOIN pool_days p USING (day)
LEFT JOIN star_days s USING (day)
ORDER BY d.day
