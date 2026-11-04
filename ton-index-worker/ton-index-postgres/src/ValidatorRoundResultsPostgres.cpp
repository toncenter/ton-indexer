#include "ValidatorRoundResultsPostgres.h"
#include "postgresql_tools.h"

void elector::postgres::insert_round_results(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
  // No FK to validator_cycles: blocks and therefore their batches can finish out of order.
  std::map<uint32_t, const schema::ValidatorRoundResult*> rows;
  for (const auto& task : tasks) {
    for (const auto& row : task.parsed_block_->validator_round_results_) {
      if (!row.valid() || row.unfreeze_mc_seqno != task.mc_seqno_) {
        throw std::runtime_error("Invalid validator round result");
      }
      rows[row.election_id] = &row;
    }
  }
  std::initializer_list<std::string_view> columns = {
      "election_id", "vset_hash", "total_stake", "bonuses", "unfreeze_mc_seqno",
      "unfreeze_utime", "unfreeze_tx_lt", "unfreeze_tx_hash"};
  PopulateTableStream stream(txn, "public.validator_round_results", columns, 100);
  stream.setConflictDoUpdate({"election_id"},
      {"vset_hash", "total_stake", "bonuses", "unfreeze_mc_seqno", "unfreeze_utime", "unfreeze_tx_lt", "unfreeze_tx_hash"}, "");
  for (const auto& [_, row] : rows) {
    stream.insert_row(std::make_tuple(row->election_id, row->vset_hash, row->total_stake, row->bonuses,
        row->unfreeze_mc_seqno, row->unfreeze_utime, row->unfreeze_tx_lt, row->unfreeze_tx_hash));
  }
  stream.finish();
}
