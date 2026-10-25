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

// Both normal resume and bounded replay must repair a missing supply row even
// when the block insert succeeded before a crash. IN avoids duplicate fan-out
// while ReplacingMergeTree rows are still awaiting background merges.
std::string completed_seqnos_query(bool collect_gramstat, std::int32_t from_seqno, std::int32_t to_seqno);

}  // namespace gramstat
