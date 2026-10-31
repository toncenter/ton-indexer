#include "ElectorSnapshotsPostgres.h"
#include "ElectorSnapshots.h"
#include "postgresql_tools.h"

void staking_history::postgres::insert_states(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
  std::initializer_list<std::string_view> columns = {"mc_seqno", "gen_utime", "complete", "payload"};
  PopulateTableStream snapshots(txn, "public.elector_stake_states", columns, 100);
  snapshots.setConflictDoUpdate({"mc_seqno"}, {"gen_utime", "complete", "payload"}, "");
  for (const auto& task : tasks) {
    if (const auto& state = task.parsed_block_->elector_snapshot_) {
      if (state->mc_seqno != task.mc_seqno_) throw std::runtime_error("Elector MC mismatch");
      snapshots.insert_row(std::make_tuple(state->mc_seqno, state->gen_utime, state->complete, serialize(*state)));
    }
  }
  snapshots.finish();
  std::initializer_list<std::string_view> election_columns = {
      "mc_seqno", "election_id", "vset_hash", "unfreeze_at", "stake_held", "total_stake"};
  // Replace children even when the corrected snapshot is empty or unknown.
  std::map<uint32_t, const schema::ElectorSnapshot*> latest;
  for (const auto& task : tasks) {
    if (const auto& s = task.parsed_block_->elector_snapshot_) latest[s->mc_seqno] = &*s;
  }
  for (const auto& [mc, s] : latest) txn.exec0("DELETE FROM public.elector_frozen_elections WHERE mc_seqno=" + std::to_string(mc));
  PopulateTableStream elections(txn, "public.elector_frozen_elections", election_columns, 100);
  for (const auto& [mc, s] : latest) {
    if (!s->complete) continue;
    for (const auto& e : s->elections) elections.insert_row(std::make_tuple(mc,e.election_id,e.vset_hash,e.unfreeze_at,e.stake_held,e.total_stake));
  }
  elections.finish();
  std::initializer_list<std::string_view> set_columns = {"mc_seqno", "gen_utime", "config_hash", "config_boc"};
  PopulateTableStream sets(txn, "public.validator_set_states", set_columns, 100);
  sets.setConflictDoUpdate({"mc_seqno"}, {"gen_utime", "config_hash", "config_boc"}, "");
  for (const auto& task : tasks) {
    if (const auto& state = task.parsed_block_->validator_set_state_) {
      if (state->mc_seqno != task.mc_seqno_) throw std::runtime_error("Validator set MC mismatch");
      sets.insert_row(std::make_tuple(state->mc_seqno, state->gen_utime, state->config_hash, state->config_boc));
    }
  }
  sets.finish();
  std::initializer_list<std::string_view> holder_columns = {"account", "mc_seqno", "gen_utime", "payload"};
  PopulateTableStream holders(txn, "public.stake_holder_states", holder_columns, 100);
  holders.setConflictDoUpdate({"account", "mc_seqno"}, {"gen_utime", "payload"}, "");
  for (const auto& task : tasks) {
    for (const auto& h : task.parsed_block_->stake_holder_states_) {
      if (h.mc_seqno != task.mc_seqno_) throw std::runtime_error("Holder MC mismatch");
      holders.insert_row(std::make_tuple(h.address,h.mc_seqno,h.gen_utime,serialize(h)));
    }
  }
  holders.finish();
}
