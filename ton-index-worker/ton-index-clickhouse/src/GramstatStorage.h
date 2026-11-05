#pragma once

#include "IndexData.h"
#include "clickhouse/block.h"

namespace gramstat {

bool valid_database_name(const std::string& database);
std::string table_name(const std::string& database, const std::string& table);
std::vector<std::string> schema_queries(const std::string& database = "gramstat");

clickhouse::Block total_supply_block(const std::vector<schema::TotalSupply>& snapshots);
clickhouse::Block account_deltas_block(const std::vector<schema::AccountDeltas>& deltas);
clickhouse::Block deployments_block(const std::vector<schema::Deployment>& deployments);

// Both normal resume and bounded replay must repair missing Gramstat rows even
// when the block insert succeeded before a crash. IN avoids duplicate fan-out
// while ReplacingMergeTree rows are still awaiting background merges.
// Supply is required only on the fixed sampling grid; deltas are required on every MC.
// New batches write deployments BEFORE account_deltas (also when there are no
// deployments). Older deltas predate that guarantee and require explicit --force.
std::string completed_seqnos_query(bool collect_gramstat, std::int32_t from_seqno, std::int32_t to_seqno,
                                  const std::string& database = "gramstat");

}  // namespace gramstat
