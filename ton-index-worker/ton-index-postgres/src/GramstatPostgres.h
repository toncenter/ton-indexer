#pragma once

#include <pqxx/pqxx>

#include "InsertManagerBase.h"

namespace gramstat::postgres {

void ensure_schema(pqxx::work& txn);
void insert_total_supply(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks);
void insert_deployments(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks);
void insert_account_deltas(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks);

// For bounded backfill and standby acknowledgement, a legacy blocks row alone
// does not prove that the requested Gramstat metrics were written.
// Deployments commit with deltas, including the zero-event case. History written
// before deployments were introduced must be backfilled with --force.
inline const char* completed_block_condition(bool collect_gramstat) {
    return collect_gramstat
        ? " AND EXISTS (SELECT 1 FROM gramstat.total_supply WHERE mc_seqno = blocks.seqno)"
          " AND EXISTS (SELECT 1 FROM gramstat.account_deltas WHERE mc_seqno = blocks.seqno)"
        : "";
}

}  // namespace gramstat::postgres
