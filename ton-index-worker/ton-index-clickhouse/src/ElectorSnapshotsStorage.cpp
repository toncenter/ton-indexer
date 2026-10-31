#include "ElectorSnapshotsStorage.h"
#include "ElectorSnapshots.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"

namespace staking_history::clickhouse_storage {
clickhouse::Block states_block(const std::vector<schema::ElectorSnapshot>& states) {
  auto mc = std::make_shared<clickhouse::ColumnUInt32>();
  auto time = std::make_shared<clickhouse::ColumnDateTime>("UTC");
  auto complete = std::make_shared<clickhouse::ColumnUInt8>();
  auto payload = std::make_shared<clickhouse::ColumnString>();
  for (const auto& state : states) {
    mc->Append(state.mc_seqno); time->AppendRaw(state.gen_utime);
    complete->Append(state.complete); payload->Append(serialize(state));
  }
  clickhouse::Block block;
  block.AppendColumn("mc_seqno", mc); block.AppendColumn("gen_utime", time);
  block.AppendColumn("complete", complete); block.AppendColumn("payload", payload);
  return block;
}

void insert_states(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks) {
  std::vector<schema::ElectorSnapshot> snapshots;
  auto mc = std::make_shared<clickhouse::ColumnUInt32>();
  auto time = std::make_shared<clickhouse::ColumnDateTime>("UTC");
  auto hash = std::make_shared<clickhouse::ColumnString>();
  auto boc = std::make_shared<clickhouse::ColumnString>();
  for (const auto& task : tasks) {
    if (const auto& s = task.parsed_block_->elector_snapshot_) {
      if (s->mc_seqno != task.mc_seqno_) throw std::runtime_error("Elector MC mismatch");
      snapshots.push_back(*s);
    }
    if (const auto& s = task.parsed_block_->validator_set_state_) {
      if (s->mc_seqno != task.mc_seqno_) throw std::runtime_error("Validator set MC mismatch");
      mc->Append(s->mc_seqno); time->AppendRaw(s->gen_utime); hash->Append(s->config_hash); boc->Append(s->config_boc);
    }
  }
  if (!snapshots.empty()) client.Insert("elector_stake_states", states_block(snapshots));
  if (mc->Size()) {
    clickhouse::Block block;
    block.AppendColumn("mc_seqno", mc); block.AppendColumn("gen_utime", time);
    block.AppendColumn("config_hash", hash); block.AppendColumn("config_boc", boc);
    client.Insert("validator_set_states", block);
  }
  auto account = std::make_shared<clickhouse::ColumnString>();
  auto holder_mc = std::make_shared<clickhouse::ColumnUInt32>();
  auto holder_time = std::make_shared<clickhouse::ColumnDateTime>("UTC");
  auto payload = std::make_shared<clickhouse::ColumnString>();
  for (const auto& task : tasks) {
    for (const auto& h : task.parsed_block_->stake_holder_states_) {
      if (h.mc_seqno != task.mc_seqno_) throw std::runtime_error("Holder MC mismatch");
      account->Append(h.address); holder_mc->Append(h.mc_seqno); holder_time->AppendRaw(h.gen_utime); payload->Append(serialize(h));
    }
  }
  if (account->Size()) {
    clickhouse::Block block;
    block.AppendColumn("account",account); block.AppendColumn("mc_seqno",holder_mc);
    block.AppendColumn("gen_utime",holder_time); block.AppendColumn("payload",payload);
    client.Insert("stake_holder_states",block);
  }
}
}
