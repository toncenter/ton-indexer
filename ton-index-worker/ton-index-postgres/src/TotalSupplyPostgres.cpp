#include "TotalSupplyPostgres.h"

#include "postgresql_tools.h"

namespace gramstat::postgres {

void ensure_schema(pqxx::work& txn) {
    // Serialize first-time DDL when leader and standby start together.
    txn.exec("SELECT pg_advisory_xact_lock(hashtext('gramstat.schema'))");
    txn.exec0(R"SQL(
CREATE SCHEMA IF NOT EXISTS gramstat;
CREATE TABLE IF NOT EXISTS gramstat.total_supply (
    mc_seqno bigint PRIMARY KEY,
    gen_utime bigint NOT NULL,
    total numeric(38, 0) NOT NULL,
    accounts numeric(38, 0) NOT NULL,
    dispatch_queue numeric(38, 0) NOT NULL,
    unprocessed_messages numeric(38, 0) NOT NULL,
    unrecovered_validator_fees numeric(38, 0) NOT NULL
);
CREATE INDEX IF NOT EXISTS total_supply_gen_utime_idx
    ON gramstat.total_supply (gen_utime, mc_seqno);
)SQL");
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

}  // namespace gramstat::postgres
