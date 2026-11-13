#include "ElectorRoundResults.h"

#include "DataParser.h"
#include "Statistics.h"
#include "parse_contract_methods.h"
#include "common/checksum.h"
#include "emulator/transaction-emulator.h"
#include "safe-parse.h"
#include "smc-interfaces/execute-smc.h"
#include "td/utils/base64.h"
#include "vm/db/DynamicBagOfCellsDb.h"
#include <algorithm>
#include <map>
#include <set>

namespace elector {

td::Result<std::vector<vm::StackEntry>> run_get_method(
    const schema::AccountState& state, const std::shared_ptr<block::ConfigInfo>& config,
    const std::string& method, std::vector<vm::StackEntry> input, uint32_t now) {
  if (state.code.is_null() || state.data.is_null() || !config) {
    return td::Status::Error("Elector getter requires code, data and config");
  }
  ton::SmartContract smc({state.code, state.data});
  ton::SmartContract::Args args;
  if (auto libraries = config->get_libraries_root(); libraries.not_null()) {
    args.set_libraries(vm::Dictionary(libraries, 256));
  }
  args.set_config(config);
  args.set_now(now);
  args.set_address(state.account);
  args.set_stack(std::move(input));
  args.set_method_id(method);
  // list_complaints may serialize hundreds of complaints with a full voter set.
  args.set_limits(vm::GasLimits{1'000'000'000});
  auto result = smc.run_get_method(args);
  if (!result.success) {
    return td::Status::Error(PSLICE() << method << " failed, exit code " << result.code
                                    << (result.missing_library ? ", missing library" : ""));
  }
  return result.stack->extract_contents();
}

namespace {

bool proven_absent(const schema::AccountState& state, const std::shared_ptr<block::ConfigInfo>& config,
                   td::Slice method) {
  auto info = get_contract_method_info(state.code, config.get());
  // A failed inspection or custom selector says nothing about method absence.
  return info.is_ok() && info.ok()->exhaustive && !info.ok()->has((td::crc16(method) & 0xffff) | 0x10000);
}

td::Result<td::RefInt256> nonnegative(const vm::StackEntry& entry, unsigned bits) {
  auto value = entry.as_int();
  if (value.is_null() || !value->is_valid() || value->sgn() < 0 || value->bit_size(false) > bits) {
    return td::Status::Error("Invalid Elector integer");
  }
  return value;
}

td::Result<std::vector<vm::StackEntry>> list_entries(const std::vector<vm::StackEntry>& stack) {
  if (stack.size() != 1) return td::Status::Error("Elector getter must return one list");
  std::vector<vm::StackEntry> entries;
  auto list = stack[0];
  while (!list.is_null()) {
    auto pair = list.as_tuple();
    if (pair.is_null() || pair->size() != 2 || entries.size() >= 4096) {
      return td::Status::Error("Invalid Elector list");
    }
    entries.push_back(pair->at(0));
    list = pair->at(1);
  }
  return entries;
}

td::Result<std::set<uint32_t>> parse_ids(const std::vector<vm::StackEntry>& stack) {
  TRY_RESULT(entries, list_entries(stack));
  std::set<uint32_t> result;
  for (const auto& entry : entries) {
    TRY_RESULT(id, nonnegative(entry, 32));
    if (!result.insert(static_cast<uint32_t>(id->to_long())).second) {
      return td::Status::Error("Duplicate Elector election ID");
    }
  }
  return result;
}

bool successful_tick(const schema::Transaction& tx) {
  auto tick = std::get_if<schema::TransactionDescr_tick_tock>(&tx.description);
  if (!tick || tick->aborted) return false;
  auto compute = std::get_if<schema::TrComputePhase_vm>(&tick->compute_ph);
  return compute && compute->success;
}

class Collector {
  const ParsedBlock& parsed_;
  const schema::Block& block_;
  std::vector<const schema::Transaction*> txs_;
  CollectionStats& stats_;
  std::map<td::Bits256, td::Ref<vm::Cell>> cells_;
  std::map<td::Bits256, std::set<uint32_t>> ids_;
  std::shared_ptr<block::ConfigInfo> replay_config_;
  std::unique_ptr<emulator::TransactionEmulator> emulator_;

  const td::Bits256& boundary_hash(size_t i) const {
    return i == txs_.size() ? txs_.back()->account_state_hash_after : txs_[i]->account_state_hash_before;
  }

  td::Result<td::Ref<vm::Cell>> load(const td::Bits256& hash) {
    if (auto it = cells_.find(hash); it != cells_.end()) return it->second;
    if (!parsed_.cell_db_reader_) return td::Status::Error("Missing CellDB reader for Elector");
    TRY_RESULT(cell, parsed_.cell_db_reader_->load_cell(hash.as_slice()));
    if (td::Bits256(cell->get_hash().bits()) != hash) return td::Status::Error("Elector state hash mismatch");
    cells_[hash] = cell;
    return td::Ref<vm::Cell>(std::move(cell));
  }

  td::Status prepare_replay() {
    if (emulator_) return td::Status::OK();
    // Transactions in MC N execute with the configuration of MC N-1, not the
    // post-state config used by the ordinary indexer snapshots (notably at key blocks).
    for (const auto& ds : parsed_.mc_block_.shard_blocks_diff_) {
      if (ds.block_data.is_null() || !ds.block_data->block_id().is_masterchain()) continue;
      block::gen::Block::Record block;
      if (!tlb::unpack_cell(ds.block_data->root_cell(), block)) return td::Status::Error("Invalid MC block");
      vm::CellSlice update(vm::NoVm(), block.state_update);
      if (update.special_type() != vm::CellTraits::SpecialType::MerkleUpdate) {
        return td::Status::Error("Invalid MC state update");
      }
      auto hash = update.prefetch_ref(0)->get_hash(0).bits();
      TRY_RESULT(root, load(hash));
      std::vector<ton::BlockIdExt> prev;
      ton::BlockIdExt mc;
      bool split;
      TRY_STATUS(block::unpack_block_prev_blk_try(ds.block_data->root_cell(), ds.block_data->block_id(), prev, mc, split));
      if (prev.size() != 1) return td::Status::Error("Expected one previous MC block");
      TRY_RESULT(config, block::ConfigInfo::extract_config(root, prev.front(),
          block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries |
          block::ConfigInfo::needPrevBlocks | block::ConfigInfo::needSpecialSmc));
      replay_config_ = std::move(config);
      break;
    }
    if (!replay_config_) return td::Status::Error("Missing MC pre-state configuration for Elector replay");
    TRY_RESULT(seed_bytes, td::base64_decode(block_.rand_seed));
    if (seed_bytes.size() != 32) return td::Status::Error("Invalid MC random seed");
    td::Bits256 seed;
    seed.as_slice().copy_from(seed_bytes);
    emulator_ = std::make_unique<emulator::TransactionEmulator>(replay_config_);
    emulator_->set_rand_seed(seed);
    if (replay_config_->get_global_version() >= 4) {
      TRY_RESULT(prev_blocks, replay_config_->get_prev_blocks_info());
      emulator_->set_prev_blocks_info(std::move(prev_blocks));
    }
    if (auto libs = replay_config_->get_libraries_root(); libs.not_null()) emulator_->set_libs(vm::Dictionary(libs, 256));
    return td::Status::OK();
  }

  td::Result<td::Ref<vm::Cell>> state_at(size_t i) {
    auto direct = load(boundary_hash(i));
    if (direct.is_ok()) return direct.move_as_ok();
    if (i == 0 || i == txs_.size()) return direct.move_as_error();

    // A transaction stores state hashes, not intermediate Account cells. Replay
    // only a missing prefix, only after the block-level removal filter matched.
    size_t start = i;
    td::Ref<vm::Cell> root;
    while (start > 0) {
      auto found = load(boundary_hash(--start));
      if (found.is_ok()) { root = found.move_as_ok(); break; }
    }
    if (root.is_null()) return td::Status::Error("Missing initial Elector state for replay");
    TRY_STATUS(prepare_replay());
    const auto& first = *txs_[start];
    auto shard_account = vm::CellBuilder().store_ref(root).store_bits(first.prev_trans_hash.bits(), 256)
        .store_long(first.prev_trans_lt, 64).finalize();
    block::Account account(first.account.workchain, first.account.addr.bits());
    if (!account.unpack(vm::load_cell_slice_ref(shard_account), first.now,
                        replay_config_->is_special_smartcontract(first.account.addr))) {
      return td::Status::Error("Cannot unpack Elector for replay");
    }
    for (size_t j = start; j < i; ++j) {
      if (txs_[j]->raw.is_null()) return td::Status::Error("Missing original Elector transaction");
      // This overload verifies BOTH the transaction hash and resulting Account hash.
      TRY_RESULT(result, emulator_->emulate_transaction(std::move(account), txs_[j]->raw));
      ++stats_.replayed_transactions;
      account = std::move(result.account);
      if (td::Bits256(account.total_state->get_hash().bits()) != boundary_hash(j + 1)) {
        return td::Status::Error("Elector transaction chain mismatch");
      }
      cells_[boundary_hash(j + 1)] = account.total_state;
    }
    return account.total_state;
  }

  td::Result<schema::AccountState> parsed_state(size_t i, td::Ref<vm::Cell> root) {
    const auto& tx = *txs_[i == txs_.size() ? i - 1 : i];
    TRY_RESULT(state, ParseQuery::parse_account(root, tx.now,
        i == txs_.size() ? tx.hash : tx.prev_trans_hash,
        i == txs_.size() ? tx.lt : tx.prev_trans_lt));
    if (state.account != tx.account) return td::Status::Error("Elector state address mismatch");
    return state;
  }

  td::Result<std::set<uint32_t>> ids_at(size_t i) {
    if (auto it = ids_.find(boundary_hash(i)); it != ids_.end()) return it->second;
    TRY_RESULT(root, state_at(i));
    TRY_RESULT(state, parsed_state(i, root));
    if (proven_absent(state, parsed_.mc_block_.config_, "past_election_ids")) {
      // Some historical contracts expose amounts but not the later lightweight
      // ID getter. Use their existing ABI, retaining all amount validation.
      ++stats_.legacy_id_getters;
      ++stats_.amount_getters;
      g_statistics.record_count(ELECTOR_ROUND_LEGACY_IDS);
      TRY_RESULT(stack, run_get_method(state, parsed_.mc_block_.config_, "past_elections", {}, state.timestamp));
      TRY_RESULT(amounts, parse_round_amounts(stack));
      std::set<uint32_t> ids;
      for (const auto& amount : amounts) ids.insert(amount.election_id);
      ids_[boundary_hash(i)] = ids;
      return ids;
    }
    ++stats_.id_getters;
    TRY_RESULT(stack, run_get_method(state, parsed_.mc_block_.config_, "past_election_ids", {}, state.timestamp));
    TRY_RESULT(ids, parse_ids(stack));
    ids_[boundary_hash(i)] = ids;
    return ids;
  }

 public:
  Collector(const ParsedBlock& parsed, const schema::Block& block,
            std::vector<const schema::Transaction*> txs, CollectionStats& stats)
      : parsed_(parsed), block_(block), txs_(std::move(txs)), stats_(stats) {}

  td::Result<std::vector<schema::ValidatorRoundResult>> collect() {
    std::vector<schema::ValidatorRoundResult> rows;
    if (std::none_of(txs_.begin(), txs_.end(), [](auto tx) { return successful_tick(*tx); })) return rows;
    std::sort(txs_.begin(), txs_.end(), [](auto a, auto b) { return a->lt < b->lt; });
    for (size_t i = 1; i < txs_.size(); ++i) {
      if (txs_[i - 1]->account_state_hash_after != txs_[i]->account_state_hash_before) {
        return td::Status::Error("Noncontiguous Elector transactions");
      }
    }
    TRY_RESULT(first, state_at(0));
    TRY_RESULT(last, state_at(txs_.size()));
    TRY_RESULT(before, parsed_state(0, first));
    TRY_RESULT(after, parsed_state(txs_.size(), last));
    if (before.data.not_null() && after.data.not_null() && before.data->get_hash() == after.data->get_hash()) return rows;
    if (proven_absent(before, parsed_.mc_block_.config_, "past_elections") ||
        proven_absent(after, parsed_.mc_block_.config_, "past_elections")) {
      // Early Elector deployments lack the reward getter. This is unsupported
      // coverage, not an observed zero reward or a successful empty getter.
      ++stats_.unsupported_blocks;
      g_statistics.record_count(ELECTOR_ROUND_UNSUPPORTED);
      return rows;
    }
    TRY_RESULT(before_ids, ids_at(0));
    TRY_RESULT(after_ids, ids_at(txs_.size()));
    std::set<uint32_t> removed;
    std::set_difference(before_ids.begin(), before_ids.end(), after_ids.begin(), after_ids.end(),
                        std::inserter(removed, removed.end()));
    if (removed.empty()) return rows;

    for (size_t i = 0; i < txs_.size(); ++i) {
      const auto& tx = *txs_[i];
      if (!successful_tick(tx)) continue;  // Config rejection also removes elections, but is not a completed round.
      TRY_RESULT(old_ids, ids_at(i));
      TRY_RESULT(new_ids, ids_at(i + 1));
      std::set<uint32_t> released;
      for (auto id : removed) if (old_ids.count(id) && !new_ids.count(id)) released.insert(id);
      if (released.empty()) continue;
      TRY_RESULT(root, state_at(i));
      TRY_RESULT(state, parsed_state(i, root));
      ++stats_.amount_getters;
      TRY_RESULT(stack, run_get_method(state, parsed_.mc_block_.config_, "past_elections", {}, tx.now));
      TRY_RESULT(amounts, parse_round_amounts(stack));
      for (const auto& amount : amounts) {
        if (!released.erase(amount.election_id)) continue;
        if (amount.unfreeze_at > tx.now) return td::Status::Error("Elector election removed before release time");
        rows.push_back({amount.election_id, amount.vset_hash, amount.total_stake, amount.bonuses,
                        tx.mc_seqno, tx.now, tx.lt, tx.hash});
      }
      if (!released.empty()) return td::Status::Error("Missing Elector unfreeze amounts");
    }
    return rows;
  }
};

}  // namespace

td::Result<std::vector<RoundAmounts>> parse_round_amounts(const std::vector<vm::StackEntry>& stack) {
  return safe_parse::guard("Elector round amounts", [&]() -> td::Result<std::vector<RoundAmounts>> {
    TRY_RESULT(entries, list_entries(stack));
    std::vector<RoundAmounts> rows;
    std::set<uint32_t> seen;
    for (const auto& entry : entries) {
      auto tuple = entry.as_tuple();
      if (tuple.is_null() || tuple->size() < 2) return td::Status::Error("Invalid past_elections entry");
      TRY_RESULT(id, nonnegative(tuple->at(0), 32));
      size_t offset = 1;
      if (tuple->size() == 2) { tuple = tuple->at(1).as_tuple(); offset = 0; }
      if (tuple.is_null() || tuple->size() != offset + 7) return td::Status::Error("Invalid past_elections fields");
      TRY_RESULT(unfreeze, nonnegative(tuple->at(offset), 32));
      TRY_RESULT(hash, nonnegative(tuple->at(offset + 2), 256));
      TRY_RESULT(stake, nonnegative(tuple->at(offset + 4), 120));
      TRY_RESULT(bonuses, nonnegative(tuple->at(offset + 5), 120));
      auto election_id = static_cast<uint32_t>(id->to_long());
      if (!seen.insert(election_id).second) return td::Status::Error("Duplicate past_elections entry");
      rows.push_back({election_id, static_cast<uint32_t>(unfreeze->to_long()), td::hex_string(hash, true, 64), stake, bonuses});
    }
    return rows;
  });
}

td::Result<std::vector<schema::ValidatorRoundResult>> collect_round_results(const ParsedBlock& parsed, CollectionStats* stats) {
  return safe_parse::guard("Elector round results", [&]() -> td::Result<std::vector<schema::ValidatorRoundResult>> {
    if (!parsed.mc_block_.config_) return td::Status::Error("Missing Elector configuration");
    auto param = parsed.mc_block_.config_->get_config_param(1);
    if (param.is_null()) return td::Status::Error("Missing Elector address");
    auto cs = vm::load_cell_slice(param);
    td::Bits256 address;
    if (!cs.fetch_bits_to(address)) return td::Status::Error("Invalid Elector address");
    CollectionStats local_stats;
    std::vector<schema::ValidatorRoundResult> results;
    for (const auto& block : parsed.blocks_) {
      if (block.workchain != ton::masterchainId) continue;
      std::vector<const schema::Transaction*> transactions;
      for (const auto& tx : block.transactions) {
        if (tx.account == block::StdAddress(-1, address)) transactions.push_back(&tx);
      }
      Collector collector(parsed, block, std::move(transactions), stats ? *stats : local_stats);
      TRY_RESULT(rows, collector.collect());
      results.insert(results.end(), rows.begin(), rows.end());
    }
    return results;
  });
}

}  // namespace elector
