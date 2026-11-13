#pragma once

#include "IndexData.h"
#include "InsertManagerBase.h"
#include "clickhouse/block.h"
#include "clickhouse/client.h"

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

namespace nft_history {

inline constexpr const char* CREATE_STATES = R"SQL(
CREATE TABLE IF NOT EXISTS nft_item_states (
    account String,
    mc_seqno UInt32,
    gen_utime DateTime('UTC'),
    last_tx_lt UInt64,
    existing Nullable(Bool),
    owner Nullable(String),
    INDEX nft_item_states_owner_idx ifNull(owner, '') TYPE bloom_filter(0.01) GRANULARITY 1,
    CONSTRAINT valid_owner CHECK ifNull(existing, 0) = 1 OR isNull(owner)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(gen_utime)
ORDER BY (account, mc_seqno)
)SQL";

clickhouse::Block states_block(const std::vector<schema::NFTItemState>& states);

}  // namespace nft_history

namespace account_history {
void insert_states(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks);
}
