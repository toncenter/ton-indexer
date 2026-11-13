#include "ValidatorRoundResultsStorage.h"

#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "td/utils/base64.h"
#include <stdexcept>

namespace elector::clickhouse_storage {

clickhouse::Block results_block(const std::vector<schema::ValidatorRoundResult>& rows) {
  using namespace clickhouse;
  auto election = std::make_shared<ColumnUInt32>();
  auto vset = std::make_shared<ColumnString>();
  auto stake = std::make_shared<ColumnDecimal>(38, 0);
  auto bonuses = std::make_shared<ColumnDecimal>(38, 0);
  auto mc = std::make_shared<ColumnUInt32>();
  auto time = std::make_shared<ColumnDateTime>("UTC");
  auto lt = std::make_shared<ColumnUInt64>();
  auto hash = std::make_shared<ColumnFixedString>(44);
  for (const auto& row : rows) {
    if (!row.valid()) throw std::runtime_error("Invalid validator round result");
    election->Append(row.election_id);
    vset->Append(row.vset_hash);
    stake->Append(row.total_stake->to_dec_string());
    bonuses->Append(row.bonuses->to_dec_string());
    mc->Append(row.unfreeze_mc_seqno);
    time->AppendRaw(row.unfreeze_utime);
    lt->Append(row.unfreeze_tx_lt);
    hash->Append(td::base64_encode(row.unfreeze_tx_hash.as_slice()));
  }
  Block block;
  block.AppendColumn("election_id", election);
  block.AppendColumn("vset_hash", vset);
  block.AppendColumn("total_stake", stake);
  block.AppendColumn("bonuses", bonuses);
  block.AppendColumn("unfreeze_mc_seqno", mc);
  block.AppendColumn("unfreeze_utime", time);
  block.AppendColumn("unfreeze_tx_lt", lt);
  block.AppendColumn("unfreeze_tx_hash", hash);
  return block;
}

void insert_round_results(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks) {
  std::vector<schema::ValidatorRoundResult> rows;
  for (const auto& task : tasks) {
    for (const auto& row : task.parsed_block_->validator_round_results_) {
      if (row.unfreeze_mc_seqno != task.mc_seqno_) throw std::runtime_error("Validator round MC mismatch");
      rows.push_back(row);
    }
  }
  if (!rows.empty()) client.Insert("validator_round_results", results_block(rows));
}

}  // namespace elector::clickhouse_storage
