#include "GramstatPostgres.h"
#include "GramstatSchema.h"

#include "postgresql_tools.h"

namespace gramstat::postgres {

void ensure_schema(pqxx::work& txn) {
    // Serialize first-time DDL when leader and standby start together.
    txn.exec("SELECT pg_advisory_xact_lock(hashtext('gramstat.schema'))");
    txn.exec0(SCHEMA_SQL);
}

void insert_total_supply(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
    // One row per MC: a batched INSERT with conflict handling also works for
    // prewarm, historical backfill and retries of an already committed batch.
    std::initializer_list<std::string_view> columns = {
        "mc_seqno", "gen_utime", "total", "accounts", "dispatch_queue",
        "unprocessed_messages", "unrecovered_validator_fees"};
    PopulateTableStream stream(txn, "gramstat.total_supply", columns, 1000);
    stream.setConflictDoNothing();
    for (const auto& task : tasks) {
        const auto& snapshot = task.parsed_block_->total_supply_;
        if (!snapshot || snapshot->mc_seqno != task.mc_seqno_) {
            throw std::runtime_error("Missing or mismatched total supply snapshot in PostgreSQL batch");
        }
        stream.insert_row(std::make_tuple(snapshot->mc_seqno, snapshot->gen_utime, snapshot->total,
            snapshot->accounts, snapshot->dispatch_queue, snapshot->unprocessed_messages,
            snapshot->unrecovered_validator_fees));
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
        const auto& supply = task.parsed_block_->total_supply_;
        if (!delta || delta->mc_seqno != task.mc_seqno_ || !supply ||
            supply->mc_seqno != delta->mc_seqno || supply->gen_utime != delta->gen_utime) {
            throw std::runtime_error("Missing or mismatched account deltas in PostgreSQL batch");
        }
        stream.insert_row(std::make_tuple(delta->mc_seqno, delta->gen_utime,
            delta->delta_uninit, delta->delta_active, delta->delta_frozen));
    }
    stream.finish();
}

}  // namespace gramstat::postgres
