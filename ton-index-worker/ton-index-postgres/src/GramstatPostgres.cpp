#include "GramstatPostgres.h"
#include "GramstatSchema.h"

#include "postgresql_tools.h"

namespace gramstat::postgres {

void ensure_schema(pqxx::work& txn) {
    // Serialize first-time DDL when leader and standby start together.
    txn.exec("SELECT pg_advisory_xact_lock(hashtext('gramstat.schema'))");
    txn.exec0(SCHEMA_SQL);
}

void insert_deployments(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
    std::initializer_list<std::string_view> columns = {
        "mc_seqno", "gen_utime", "account", "lt", "tx_hash", "init_code_hash", "out_msgs_count"};
    PopulateTableStream stream(txn, "gramstat.deployments", columns, 1000);
    stream.setConflictDoUpdate({"account", "lt", "tx_hash"},
        {"mc_seqno", "gen_utime", "init_code_hash", "out_msgs_count"}, "");
    for (const auto& task : tasks) {
        const auto& events = task.parsed_block_->deployments_;
        if (!events) {
            throw std::runtime_error("Missing deployments in PostgreSQL batch");
        }
        for (const auto& event : *events) {
            if (event.mc_seqno != task.mc_seqno_) {
                throw std::runtime_error("Mismatched deployment in PostgreSQL batch");
            }
            stream.insert_row(std::make_tuple(event.mc_seqno, event.gen_utime, event.account,
                event.lt, event.tx_hash, event.init_code_hash, event.out_msgs_count));
        }
    }
    stream.finish();
}

void insert_account_deltas(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
    std::initializer_list<std::string_view> columns = {
        "mc_seqno", "gen_utime", "delta_uninit", "delta_active", "delta_frozen"};
    PopulateTableStream stream(txn, "gramstat.account_deltas", columns, 1000);
    // Recompute replaces the per-MC values; it must never add them again.
    stream.setConflictDoUpdate({"mc_seqno"},
        {"gen_utime", "delta_uninit", "delta_active", "delta_frozen"}, "");
    for (const auto& task : tasks) {
        const auto& delta = task.parsed_block_->account_deltas_;
        if (!delta || delta->mc_seqno != task.mc_seqno_) {
            throw std::runtime_error("Missing or mismatched account deltas in PostgreSQL batch");
        }
        stream.insert_row(std::make_tuple(delta->mc_seqno, delta->gen_utime,
            delta->delta_uninit, delta->delta_active, delta->delta_frozen));
    }
    stream.finish();
}

}  // namespace gramstat::postgres
