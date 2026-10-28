#pragma once

#include "IndexData.h"
#include "clickhouse/block.h"

namespace jetton_history {

// Canonical nonnegative decimal strings preserve the full TVM integer range,
// matching jetton_wallets and avoiding Float64 or a narrower Decimal128 balance.
inline constexpr const char* CREATE_STATES = R"SQL(
CREATE TABLE IF NOT EXISTS jetton_wallet_states (
    account String,
    mc_seqno UInt32,
    gen_utime DateTime('UTC'),
    last_tx_lt UInt64,
    existing Nullable(Bool),
    balance Nullable(String),
    nonzero Nullable(Bool) MATERIALIZED existing AND (balance != '0'),
    CONSTRAINT valid_balance CHECK
        (ifNull(existing, 0) = 1 AND isNotNull(balance) AND match(ifNull(balance, ''), '^(0|[1-9][0-9]*)$'))
        OR (ifNull(existing, 0) = 0 AND isNull(balance))
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(gen_utime)
ORDER BY (account, mc_seqno)
)SQL";

inline constexpr const char* DAILY_QUERY = R"SQL(
WITH horizon AS (
    SELECT toDate(toDateTime(max(gen_utime), 'UTC'), 'UTC') AS end_day
    FROM blocks WHERE workchain = -1
), samples AS (
    SELECT account, mc_seqno, toDate(gen_utime, 'UTC') AS day,
           toInt64(ifNull(existing, 0)) AS known_existing,
           toInt64(ifNull(nonzero, 0)) AS known_nonzero,
           toInt64(isNull(existing)) AS unknown
    FROM jetton_wallet_states FINAL
), changes AS (
    SELECT day,
           known_existing - lagInFrame(known_existing, 1, toInt64(0)) OVER w AS de,
           known_nonzero - lagInFrame(known_nonzero, 1, toInt64(0)) OVER w AS dn,
           unknown - lagInFrame(unknown, 1, toInt64(0)) OVER w AS du
    FROM samples
    WINDOW w AS (PARTITION BY account ORDER BY mc_seqno ROWS UNBOUNDED PRECEDING)
), daily AS (
    SELECT day, sum(de) AS de, sum(dn) AS dn, sum(du) AS du
    FROM changes GROUP BY day
), days AS (
    SELECT toDate(toDateTime(gen_utime, 'UTC'), 'UTC') AS day, max(seqno) AS mc_seqno
    FROM blocks WHERE workchain = -1 GROUP BY day
), totals AS (
    SELECT d.day AS day, toUInt32(d.mc_seqno) AS mc_seqno,
           sum(ifNull(de, 0)) OVER w AS known_existing,
           sum(ifNull(dn, 0)) OVER w AS known_nonzero,
           sum(ifNull(du, 0)) OVER w AS unknown_count
    FROM days d LEFT JOIN daily USING (day)
    WHERE d.day >= (SELECT min(day) FROM samples)
      AND d.day < (SELECT end_day FROM horizon)
    WINDOW w AS (ORDER BY d.day ROWS UNBOUNDED PRECEDING)
)
SELECT day, mc_seqno,
       if(unknown_count = 0, known_existing, NULL) AS existing_count,
       if(unknown_count = 0, known_nonzero, NULL) AS nonzero_count,
       unknown_count
FROM totals
)SQL";

inline std::string daily_view_sql() {
    // EMPTY leaves the first refresh to the operator; no scan during startup.
    return std::string("CREATE MATERIALIZED VIEW IF NOT EXISTS jetton_wallet_counts_daily "
        "REFRESH EVERY 1 DAY ENGINE = MergeTree ORDER BY day EMPTY AS ") + DAILY_QUERY;
}

clickhouse::Block states_block(const std::vector<schema::JettonWalletState>& states);

}  // namespace jetton_history
