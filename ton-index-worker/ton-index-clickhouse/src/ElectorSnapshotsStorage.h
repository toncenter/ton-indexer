#pragma once
#include "InsertManagerBase.h"
#include "clickhouse/client.h"
namespace staking_history::clickhouse_storage {
inline constexpr const char* SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS elector_stake_states (
    mc_seqno UInt32, gen_utime DateTime('UTC'), complete Bool, payload String
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(gen_utime) ORDER BY mc_seqno
)SQL";
inline constexpr const char* SET_SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS validator_set_states (
    mc_seqno UInt32, gen_utime DateTime('UTC'), config_hash String, config_boc String
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(gen_utime) ORDER BY mc_seqno
)SQL";
inline constexpr const char* VIEW_SQL = R"SQL(
CREATE VIEW IF NOT EXISTS elector_frozen_positions AS
SELECT mc_seqno, gen_utime, JSONExtractUInt(e, 'election_id') AS election_id,
       JSONExtractString(e, 'vset_hash') AS vset_hash,
       JSONExtractUInt(e, 'unfreeze_at') AS unfreeze_at,
       JSONExtractUInt(e, 'stake_held') AS stake_held,
       JSONExtractString(p, 'pubkey') AS pubkey, JSONExtractString(p, 'holder') AS holder,
       toUInt64(JSONExtractString(p, 'weight')) AS weight,
       toDecimal128(JSONExtractString(p, 'stake'), 0) AS stake,
       JSONExtractBool(p, 'banned') AS banned
FROM (
  SELECT mc_seqno, gen_utime, arrayJoin(JSONExtractArrayRaw(payload,'elections')) AS e
  FROM elector_stake_states FINAL WHERE complete
) ARRAY JOIN JSONExtractArrayRaw(e,'positions') AS p
)SQL";
inline constexpr const char* HOLDER_SCHEMA_SQL = R"SQL(
CREATE TABLE IF NOT EXISTS stake_holder_states (
    account String, mc_seqno UInt32, gen_utime DateTime('UTC'), payload String
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(gen_utime) ORDER BY (account,mc_seqno)
)SQL";
void insert_states(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks);
clickhouse::Block states_block(const std::vector<schema::ElectorSnapshot>& states);
}
