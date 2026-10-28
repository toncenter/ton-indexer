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

clickhouse::Block states_block(const std::vector<schema::JettonWalletState>& states);

}  // namespace jetton_history
