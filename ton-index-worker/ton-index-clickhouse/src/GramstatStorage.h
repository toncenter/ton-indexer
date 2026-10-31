#pragma once

#include "IndexData.h"
#include "clickhouse/block.h"

namespace gramstat {

inline constexpr const char* CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS gramstat";
inline constexpr const char* CREATE_TOTAL_SUPPLY = R"SQL(
CREATE TABLE IF NOT EXISTS gramstat.total_supply (
    mc_seqno UInt32,
    gen_utime DateTime('UTC'),
    total Decimal(38, 0),
    accounts Decimal(38, 0),
    dispatch_queue Decimal(38, 0),
    unprocessed_messages Decimal(38, 0),
    unrecovered_validator_fees Decimal(38, 0)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(gen_utime)
ORDER BY (gen_utime, mc_seqno)
)SQL";

clickhouse::Block total_supply_block(const std::vector<schema::TotalSupply>& snapshots);

inline constexpr const char* CREATE_ACCOUNT_DELTAS = R"SQL(
CREATE TABLE IF NOT EXISTS gramstat.account_deltas (
    mc_seqno UInt32,
    gen_utime DateTime('UTC'),
    delta_uninit Int64,
    delta_active Int64,
    delta_frozen Int64
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(gen_utime)
ORDER BY (gen_utime, mc_seqno)
)SQL";

clickhouse::Block account_deltas_block(const std::vector<schema::AccountDeltas>& deltas);

inline constexpr const char* CREATE_DEPLOYMENTS = R"SQL(
CREATE TABLE IF NOT EXISTS gramstat.deployments (
    mc_seqno UInt32,
    gen_utime DateTime('UTC'),
    account String,
    lt UInt64,
    tx_hash FixedString(44),
    init_code_hash Nullable(FixedString(44)),
    out_msgs_count UInt32
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(gen_utime)
ORDER BY (account, lt, tx_hash)
)SQL";

clickhouse::Block deployments_block(const std::vector<schema::Deployment>& deployments);

// Both normal resume and bounded replay must repair missing Gramstat rows even
// when the block insert succeeded before a crash. IN avoids duplicate fan-out
// while ReplacingMergeTree rows are still awaiting background merges.
// New batches write deployments BEFORE account_deltas (also when there are no
// deployments). Older deltas predate that guarantee and require explicit --force.
std::string completed_seqnos_query(bool collect_gramstat, std::int32_t from_seqno, std::int32_t to_seqno);

}  // namespace gramstat
