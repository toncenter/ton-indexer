#pragma once

#include "InsertManagerBase.h"
#include "clickhouse/client.h"

namespace elector::clickhouse_storage {
inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS validator_round_results (
    election_id UInt32,
    vset_hash String,
    total_stake Decimal(38,0),
    bonuses Decimal(38,0),
    unfreeze_mc_seqno UInt32,
    unfreeze_utime DateTime('UTC'),
    unfreeze_tx_lt UInt64,
    unfreeze_tx_hash FixedString(44)
) ENGINE=ReplacingMergeTree ORDER BY election_id
)SQL";

clickhouse::Block results_block(const std::vector<schema::ValidatorRoundResult>& rows);
void insert_round_results(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks);
}
