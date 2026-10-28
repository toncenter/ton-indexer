#pragma once

#include <string>

namespace jetton_history::postgres {

inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS public.jetton_wallet_states (
    account tonaddr NOT NULL,
    mc_seqno bigint NOT NULL,
    gen_utime bigint NOT NULL,
    last_tx_lt bigint NOT NULL,
    existing boolean,
    balance numeric(78, 0),
    nonzero boolean GENERATED ALWAYS AS (existing AND balance > 0) STORED,
    PRIMARY KEY (account, mc_seqno),
    CHECK ((existing IS TRUE AND balance IS NOT NULL AND balance >= 0)
        OR (existing IS NOT TRUE AND balance IS NULL))
);
)SQL";

// The source must cover history from its agreed zero baseline. Ordinary indexer
// progress/replay management establishes coverage; sparse rows cannot prove it.
inline constexpr const char* DAILY_QUERY = R"SQL(
WITH horizon AS (
    SELECT (to_timestamp(max(gen_utime)) AT TIME ZONE 'UTC')::date AS end_day
    FROM blocks WHERE workchain = -1
), samples AS (
    SELECT account, mc_seqno, (to_timestamp(gen_utime) AT TIME ZONE 'UTC')::date AS day,
           CASE WHEN existing IS TRUE THEN 1 ELSE 0 END::bigint AS known_existing,
           CASE WHEN nonzero IS TRUE THEN 1 ELSE 0 END::bigint AS known_nonzero,
           CASE WHEN existing IS NULL THEN 1 ELSE 0 END::bigint AS unknown
    FROM public.jetton_wallet_states
), changes AS (
    SELECT day,
           known_existing - lag(known_existing, 1, 0::bigint) OVER w AS de,
           known_nonzero - lag(known_nonzero, 1, 0::bigint) OVER w AS dn,
           unknown - lag(unknown, 1, 0::bigint) OVER w AS du
    FROM samples
    WINDOW w AS (PARTITION BY account ORDER BY mc_seqno)
), daily AS (
    SELECT day, sum(de) AS de, sum(dn) AS dn, sum(du) AS du
    FROM changes GROUP BY day
), days AS (
    SELECT (to_timestamp(gen_utime) AT TIME ZONE 'UTC')::date AS day, max(seqno) AS mc_seqno
    FROM blocks
    WHERE workchain = -1
    GROUP BY (to_timestamp(gen_utime) AT TIME ZONE 'UTC')::date
), totals AS (
    SELECT d.day, d.mc_seqno,
           sum(coalesce(de, 0)) OVER w AS known_existing,
           sum(coalesce(dn, 0)) OVER w AS known_nonzero,
           sum(coalesce(du, 0)) OVER w AS unknown_count
    FROM days d LEFT JOIN daily USING (day)
    WHERE d.day >= (SELECT min(day) FROM samples)
      AND d.day < (SELECT end_day FROM horizon)
    WINDOW w AS (ORDER BY d.day ROWS UNBOUNDED PRECEDING)
)
SELECT day, mc_seqno,
       CASE WHEN unknown_count = 0 THEN known_existing::bigint END AS existing_count,
       CASE WHEN unknown_count = 0 THEN known_nonzero::bigint END AS nonzero_count,
       unknown_count::bigint AS unknown_count
FROM totals
)SQL";

inline std::string daily_view_sql() {
    return std::string("CREATE MATERIALIZED VIEW IF NOT EXISTS public.jetton_wallet_counts_daily AS ") +
        DAILY_QUERY + " WITH NO DATA;\n"
        "CREATE UNIQUE INDEX IF NOT EXISTS jetton_wallet_counts_daily_day_idx "
        "ON public.jetton_wallet_counts_daily (day);\n";
}

}  // namespace jetton_history::postgres
