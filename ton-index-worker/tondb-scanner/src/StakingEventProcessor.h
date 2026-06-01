#pragma once

#include "common/refint.h"
#include "DataParser.h"
#include "convert-utils.h"
#include "emulator/transaction-emulator.h"
#include "smc-interfaces/NominatorPool.h"
#include "smc-interfaces/execute-smc.h"
#include "crypto/block/mc-config.h"
#include "td/utils/base64.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"
#include "vm/boc.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

class StakingEventProcessor {
private:
  ParsedBlockPtr block_;

public:
  explicit StakingEventProcessor(ParsedBlockPtr block): block_(std::move(block)) {
  }

  void process_transaction(const schema::Transaction& transaction) {
    if (!transaction.in_msg) {
      return;
    }

    auto validator_event = parse_validator_event(transaction);
    if (validator_event.is_error()) {
      LOG(DEBUG) << "Failed to parse validator event: " << validator_event.move_as_error();
    } else {
      auto event = validator_event.move_as_ok();
      if (event.has_value()) {
        block_->events_.push_back(std::move(event.value()));
      }
    }
  }

  td::Status process_block(const schema::Block& block) {
    auto pool_events = process_nominator_pool_events(block);
    if (pool_events.is_error()) {
      return pool_events.move_as_error();
    }
    for (auto& pool_event : pool_events.move_as_ok()) {
      block_->events_.push_back(std::move(pool_event));
    }
    return td::Status::OK();
  }

  td::Status process_snapshots() {
    return process_validator_snapshots();
  }

private:
  static constexpr std::uint32_t ELECTOR_NEW_STAKE = 0x4e73744b;
  static constexpr std::uint32_t ELECTOR_NEW_STAKE_OK = 0xf374484c;
  static constexpr std::uint32_t ELECTOR_NEW_STAKE_ERROR = 0xee6f454c;
  static constexpr std::uint32_t ELECTOR_RECOVER_STAKE = 0x47657424;
  static constexpr std::uint32_t ELECTOR_RECOVER_STAKE_OK = 0xf96f7324;
  static constexpr std::uint32_t ELECTOR_RECOVER_STAKE_ERROR = 0xfffffffe;
  static constexpr std::uint32_t ELECTOR_CONFIG_SET_OK = 0xee764f4b;
  static constexpr std::uint32_t ELECTOR_CONFIG_SET_ERROR = 0xee764f6f;
  static constexpr std::uint32_t ELECTOR_NEW_COMPLAINT = 0x52674370;
  static constexpr std::uint32_t ELECTOR_VOTE_COMPLAINT = 0x56744370;
  static constexpr std::int64_t ELECTOR_STAKE_FEE = 1000000000;
  static constexpr std::uint32_t NOMINATOR_POOL_VALIDATOR_DEPOSIT = 4;
  static constexpr std::uint32_t NOMINATOR_POOL_VALIDATOR_WITHDRAWAL = 5;

  td::RefInt256 zero_refint() const {
    return td::make_refint(td::BigInt256(0));
  }

  td::Result<block::StdAddress> get_elector_std_address() const {
    if (!block_->mc_block_.config_) {
      return td::Status::Error("Config is not available");
    }
    auto cell = block_->mc_block_.config_->get_config_param(1);
    if (cell.is_null()) {
      return td::Status::Error("Config param 1 is not available");
    }
    auto cs = vm::load_cell_slice(cell);
    td::Bits256 elector_addr;
    if (!cs.fetch_bits_to(elector_addr)) {
      return td::Status::Error("Failed to parse elector address from config param 1");
    }
    block::StdAddress address;
    address.workchain = ton::masterchainId;
    address.addr = elector_addr;
    return address;
  }

  td::Result<std::string> get_elector_address() const {
    TRY_RESULT(address, get_elector_std_address());
    return convert::to_raw_address(address);
  }

  bool transaction_compute_success(const schema::Transaction& transaction) const {
    auto check_compute = [](const auto& compute_ph) {
      auto* vm_compute = std::get_if<schema::TrComputePhase_vm>(&compute_ph);
      return vm_compute != nullptr && vm_compute->success;
    };
    if (auto* descr = std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
      return !descr->aborted && check_compute(descr->compute_ph);
    }
    if (auto* descr = std::get_if<schema::TransactionDescr_tick_tock>(&transaction.description)) {
      return !descr->aborted && check_compute(descr->compute_ph);
    }
    return false;
  }

  td::RefInt256 message_value(const schema::Transaction& transaction) const {
    if (!transaction.in_msg || !transaction.in_msg->value.has_value()) {
      return zero_refint();
    }
    return transaction.in_msg->value->grams;
  }

  schema::ValidatorEvent make_validator_event(
      const schema::Transaction& transaction,
      std::string event_type,
      std::string stake_holder_address,
      std::optional<std::string> validator_pubkey,
      std::optional<std::string> adnl_addr,
      std::optional<uint32_t> election_id,
      std::optional<uint64_t> query_id,
      td::RefInt256 amount,
      std::optional<int32_t> reason) const {
    schema::ValidatorEvent event;
    event.transaction_hash = transaction.hash;
    event.transaction_lt = transaction.lt;
    event.transaction_now = transaction.now;
    event.mc_seqno = transaction.mc_seqno;
    event.event_type = std::move(event_type);
    event.stake_holder_address = std::move(stake_holder_address);
    event.validator_pubkey = std::move(validator_pubkey);
    event.adnl_addr = std::move(adnl_addr);
    event.election_id = election_id;
    event.query_id = query_id;
    event.amount = amount;
    event.reason = reason;
    return event;
  }

  td::Result<std::optional<schema::ValidatorEvent>> parse_validator_event(const schema::Transaction& transaction) const {
    if (!transaction.in_msg || transaction.in_msg->body.is_null()) {
      return std::optional<schema::ValidatorEvent>{};
    }
    TRY_RESULT(elector_address, get_elector_address());
    const auto account_address = convert::to_raw_address(transaction.account);
    const auto source_address = transaction.in_msg->source;
    if (!source_address.has_value()) {
      return std::optional<schema::ValidatorEvent>{};
    }

    auto body_ref = vm::load_cell_slice_ref(transaction.in_msg->body);
    auto& body = body_ref.write();
    if (body.size() < 96) {
      return std::optional<schema::ValidatorEvent>{};
    }
    auto opcode = static_cast<std::uint32_t>(body.fetch_ulong(32));
    auto query_id = static_cast<std::uint64_t>(body.fetch_ulong(64));
    auto raw_value = message_value(transaction);

    if (!transaction_compute_success(transaction)) {
      return std::optional<schema::ValidatorEvent>{};
    }

    if (account_address == elector_address && opcode == ELECTOR_NEW_STAKE) {
      if (body.size() < 256 + 32 + 32 + 256) {
        return td::Status::Error("Invalid elector new_stake body");
      }
      td::Bits256 validator_pubkey;
      td::Bits256 adnl_addr;
      if (!body.fetch_bits_to(validator_pubkey)) {
        return td::Status::Error("Failed to parse validator pubkey");
      }
      auto stake_at = static_cast<std::uint32_t>(body.fetch_ulong(32));
      body.advance(32);  // max_factor
      if (!body.fetch_bits_to(adnl_addr)) {
        return td::Status::Error("Failed to parse validator adnl address");
      }
      auto stake_amount = raw_value - td::make_refint(td::BigInt256(ELECTOR_STAKE_FEE));
      if (td::sgn(stake_amount) < 0) {
        stake_amount = zero_refint();
      }

      for (const auto& out_msg : transaction.out_msgs) {
        if (out_msg.body.is_null()) {
          continue;
        }
        auto out_body_ref = vm::load_cell_slice_ref(out_msg.body);
        auto& out_body = out_body_ref.write();
        if (out_body.size() < 96) {
          continue;
        }
        auto out_opcode = static_cast<std::uint32_t>(out_body.fetch_ulong(32));
        if (out_opcode != ELECTOR_NEW_STAKE_OK && out_opcode != ELECTOR_NEW_STAKE_ERROR) {
          continue;
        }
        auto out_query_id = static_cast<std::uint64_t>(out_body.fetch_ulong(64));
        if (out_query_id != query_id) {
          continue;
        }

        std::optional<int32_t> reason;
        if (out_opcode == ELECTOR_NEW_STAKE_ERROR && out_body.size() >= 32) {
          reason = static_cast<int32_t>(out_body.fetch_long(32));
        }
        auto out_value = out_msg.value.has_value() ? out_msg.value->grams : zero_refint();
        const bool accepted = out_opcode == ELECTOR_NEW_STAKE_OK;
        return make_validator_event(
            transaction, accepted ? "stake_accepted" : "stake_rejected",
            source_address.value(), validator_pubkey.to_hex(), adnl_addr.to_hex(), stake_at, out_query_id,
            accepted ? stake_amount : out_value, reason);
      }

      if (query_id == 0) {
        return make_validator_event(
            transaction, "stake_accepted", source_address.value(), validator_pubkey.to_hex(), adnl_addr.to_hex(),
            stake_at, query_id, stake_amount, std::nullopt);
      }
      return std::optional<schema::ValidatorEvent>{};
    }

    if (account_address == elector_address && opcode == ELECTOR_RECOVER_STAKE) {
      return make_validator_event(
          transaction, "recover_requested", source_address.value(), std::nullopt, std::nullopt,
          std::nullopt, query_id, raw_value, std::nullopt);
    }

    if (source_address.value() != elector_address) {
      return std::optional<schema::ValidatorEvent>{};
    }

    if (opcode == ELECTOR_RECOVER_STAKE_OK) {
      return make_validator_event(
          transaction, "stake_recovered", account_address, std::nullopt, std::nullopt,
          std::nullopt, query_id, raw_value, std::nullopt);
    }

    if (opcode == ELECTOR_RECOVER_STAKE_ERROR) {
      return make_validator_event(
          transaction, "recover_failed", account_address, std::nullopt, std::nullopt,
          std::nullopt, query_id, raw_value, std::nullopt);
    }

    return std::optional<schema::ValidatorEvent>{};
  }

  uint32_t current_mc_seqno() const {
    for (const auto& block : block_->blocks_) {
      if (block.workchain == ton::masterchainId) {
        return block.seqno;
      }
    }
    if (!block_->blocks_.empty()) {
      return block_->blocks_.front().mc_block_seqno.value_or(block_->blocks_.front().seqno);
    }
    return 0;
  }

  uint32_t current_block_time() const {
    for (const auto& block : block_->blocks_) {
      if (block.workchain == ton::masterchainId) {
        return static_cast<uint32_t>(block.gen_utime);
      }
    }
    if (block_->mc_block_.config_ && block_->mc_block_.config_->utime != 0) {
      return block_->mc_block_.config_->utime;
    }
    if (!block_->blocks_.empty()) {
      return static_cast<uint32_t>(block_->blocks_.front().gen_utime);
    }
    return 0;
  }

  bool has_masterchain_key_block() const {
    for (const auto& block : block_->blocks_) {
      if (block.workchain == ton::masterchainId && block.key_block) {
        return true;
      }
    }
    return false;
  }

  std::optional<std::uint32_t> inbound_opcode(const schema::Transaction& transaction) const {
    if (!transaction.in_msg || transaction.in_msg->body.is_null()) {
      return std::nullopt;
    }
    auto body_ref = vm::load_cell_slice_ref(transaction.in_msg->body);
    auto& body = body_ref.write();
    if (body.size() < 32 + 64) {
      return std::nullopt;
    }
    return static_cast<std::uint32_t>(body.fetch_ulong(32));
  }

  std::optional<std::uint32_t> complaint_election_id_from_body(
      const schema::Transaction& transaction,
      std::uint32_t opcode) const {
    if (!transaction.in_msg || transaction.in_msg->body.is_null()) {
      return std::nullopt;
    }
    auto body_ref = vm::load_cell_slice_ref(transaction.in_msg->body);
    auto& body = body_ref.write();
    if (body.size() < 32 + 64) {
      return std::nullopt;
    }
    body.advance(32 + 64);
    if (opcode == ELECTOR_NEW_COMPLAINT) {
      if (body.size() < 32) {
        return std::nullopt;
      }
      return static_cast<std::uint32_t>(body.fetch_ulong(32));
    }
    if (opcode == ELECTOR_VOTE_COMPLAINT) {
      constexpr std::uint32_t signature_bits = 512;
      constexpr std::uint32_t sign_tag_bits = 32;
      constexpr std::uint32_t validator_index_bits = 16;
      constexpr std::uint32_t election_id_bits = 32;
      if (body.size() < signature_bits + sign_tag_bits + validator_index_bits + election_id_bits) {
        return std::nullopt;
      }
      body.advance(signature_bits + sign_tag_bits + validator_index_bits);
      return static_cast<std::uint32_t>(body.fetch_ulong(election_id_bits));
    }
    return std::nullopt;
  }

  td::Result<std::optional<vm::CellHash>> load_account_data_hash(
      const block::StdAddress& address,
      const td::Bits256& account_state_hash,
      const td::Bits256& last_trans_hash,
      uint64_t last_trans_lt,
      uint32_t now) {
    TRY_RESULT(account, load_account_from_celldb(address, account_state_hash, last_trans_hash, last_trans_lt, now));
    if (account.data.is_null()) {
      return std::optional<vm::CellHash>{};
    }
    return std::optional<vm::CellHash>{account.data->get_hash()};
  }

  td::Result<bool> transaction_data_changed(const schema::Transaction& transaction) {
    if (transaction.account_state_hash_before == transaction.account_state_hash_after) {
      return false;
    }
    TRY_RESULT(before_hash, load_account_data_hash(
        transaction.account,
        transaction.account_state_hash_before,
        transaction.prev_trans_hash,
        transaction.prev_trans_lt,
        transaction.now));
    TRY_RESULT(after_hash, load_account_data_hash(
        transaction.account,
        transaction.account_state_hash_after,
        transaction.hash,
        transaction.lt,
        transaction.now));
    return before_hash != after_hash;
  }

  struct ValidatorSnapshotTriggers {
    bool has_key_block{false};
    bool active_election_dirty{false};
    std::unordered_set<std::uint32_t> complaint_election_ids;

    bool needs_cycles() const {
      return has_key_block || !complaint_election_ids.empty();
    }

    bool has_work() const {
      return has_key_block || active_election_dirty || !complaint_election_ids.empty();
    }
  };

  ValidatorSnapshotTriggers collect_validator_snapshot_triggers() {
    ValidatorSnapshotTriggers triggers;
    triggers.has_key_block = has_masterchain_key_block();

    auto elector_address_r = get_elector_std_address();
    if (elector_address_r.is_error()) {
      LOG(DEBUG) << "Failed to resolve elector address for validator snapshot triggers: "
                 << elector_address_r.move_as_error();
      return triggers;
    }
    auto elector_address = elector_address_r.move_as_ok();

    for (const auto& block : block_->blocks_) {
      for (const auto& transaction : block.transactions) {
        if (transaction.account != elector_address) {
          continue;
        }
        auto data_changed_r = transaction_data_changed(transaction);
        bool data_changed = true;
        if (data_changed_r.is_ok()) {
          data_changed = data_changed_r.move_as_ok();
        } else {
          LOG(DEBUG) << "Failed to compare elector data before/after transaction: "
                     << data_changed_r.move_as_error();
        }
        if (!data_changed) {
          continue;
        }

        if (std::holds_alternative<schema::TransactionDescr_tick_tock>(transaction.description)) {
          triggers.active_election_dirty = true;
          continue;
        }

        auto opcode = inbound_opcode(transaction);
        if (!opcode.has_value()) {
          continue;
        }
        auto opcode_value = opcode.value();

        if (opcode_value == ELECTOR_NEW_STAKE ||
            opcode_value == ELECTOR_CONFIG_SET_OK ||
            opcode_value == ELECTOR_CONFIG_SET_ERROR) {
          triggers.active_election_dirty = true;
          continue;
        }

        if (opcode_value == ELECTOR_NEW_COMPLAINT || opcode_value == ELECTOR_VOTE_COMPLAINT) {
          auto election_id = complaint_election_id_from_body(transaction, opcode_value);
          if (election_id.has_value()) {
            triggers.complaint_election_ids.insert(election_id.value());
          }
        }
      }
    }

    return triggers;
  }

  td::Result<td::RefInt256> stack_int(const vm::StackEntry& entry, td::Slice field_name) const {
    auto value = entry.as_int();
    if (value.is_null()) {
      return td::Status::Error(PSLICE() << "Expected integer stack entry for " << field_name);
    }
    return value;
  }

  td::Result<int64_t> stack_int_to_i64(const vm::StackEntry& entry, td::Slice field_name) const {
    TRY_RESULT(value, stack_int(entry, field_name));
    return value->to_long();
  }

  td::Result<uint32_t> stack_int_to_u32(const vm::StackEntry& entry, td::Slice field_name) const {
    TRY_RESULT(value, stack_int_to_i64(entry, field_name));
    if (value < 0 || value > std::numeric_limits<uint32_t>::max()) {
      return td::Status::Error(PSLICE() << "Integer stack entry out of uint32 range for " << field_name);
    }
    return static_cast<uint32_t>(value);
  }

  td::Result<bool> stack_int_to_bool(const vm::StackEntry& entry, td::Slice field_name) const {
    TRY_RESULT(value, stack_int_to_i64(entry, field_name));
    return value != 0;
  }

  td::Result<std::string> stack_int_to_hex(const vm::StackEntry& entry, td::Slice field_name) const {
    TRY_RESULT(value, stack_int(entry, field_name));
    return td::hex_string(value, true, 64);
  }

  td::Result<std::vector<vm::StackEntry>> stack_list_to_vector(vm::StackEntry entry, td::Slice field_name) const {
    std::vector<vm::StackEntry> result;
    while (entry.type() != vm::StackEntry::Type::t_null) {
      auto tuple = entry.as_tuple();
      if (tuple.is_null() || tuple->size() != 2) {
        return td::Status::Error(PSLICE() << "Expected TVM cons list for " << field_name);
      }
      result.push_back(tuple->at(0));
      entry = tuple->at(1);
    }
    return result;
  }

  const schema::ValidatorCycleMember* find_cycle_member_by_index(
      const schema::ValidatorCycle* cycle,
      int64_t validator_index) const {
    if (cycle == nullptr) {
      return nullptr;
    }
    if (validator_index < 0 || validator_index > std::numeric_limits<uint32_t>::max()) {
      return nullptr;
    }
    auto index = static_cast<uint32_t>(validator_index);
    for (const auto& member : cycle->members) {
      if (member.validator_index == index) {
        return &member;
      }
    }
    return nullptr;
  }

  std::string voters_to_json(const std::vector<vm::StackEntry>& voters, const schema::ValidatorCycle* voting_cycle) const {
    td::JsonBuilder json;
    auto arr = json.enter_array();
    for (const auto& voter : voters) {
      auto voter_id = stack_int_to_i64(voter, "voted_validator");
      if (voter_id.is_ok()) {
        auto validator_index = voter_id.move_as_ok();
        auto value = arr.enter_value();
        auto obj = value.enter_object();
        obj("validator_index", validator_index);
        if (const auto* member = find_cycle_member_by_index(voting_cycle, validator_index)) {
          obj("validator_pubkey", member->validator_pubkey);
          obj("adnl_addr", member->adnl_addr);
          obj("weight", std::to_string(member->weight));
        }
      }
    }
    arr.leave();
    return json.string_builder().as_cslice().str();
  }

  td::Result<int64_t> complaint_required_weight(uint64_t total_weight) const {
    auto required_weight = (static_cast<unsigned __int128>(total_weight) * 2) / 3;
    if (required_weight > static_cast<unsigned __int128>(std::numeric_limits<int64_t>::max())) {
      return td::Status::Error("Complaint required weight is out of int64 range");
    }
    return static_cast<int64_t>(required_weight);
  }

  td::Result<std::string> stack_cell_to_boc(const vm::StackEntry& entry, td::Slice field_name) const {
    td::Ref<vm::Cell> cell;
    if (entry.type() == vm::StackEntry::Type::t_cell) {
      cell = entry.as_cell();
    } else if (entry.type() == vm::StackEntry::Type::t_slice) {
      cell = vm::CellBuilder().append_cellslice(entry.as_slice()).finalize();
    } else if (entry.type() == vm::StackEntry::Type::t_null) {
      return std::string{};
    } else {
      return td::Status::Error(PSLICE() << "Expected cell stack entry for " << field_name);
    }
    TRY_RESULT(serialized, convert::to_bytes(cell));
    return serialized.value_or("");
  }

  td::Result<std::vector<vm::StackEntry>> run_get_method_at_time(
      const schema::AccountState& account_state,
      const std::string& method_id,
      std::vector<vm::StackEntry> input,
      uint32_t now) const {
    if (account_state.code.is_null() || account_state.data.is_null()) {
      return td::Status::Error("Account state has no code or data");
    }
    if (!block_->mc_block_.config_) {
      return td::Status::Error("Config is not available");
    }

    ton::SmartContract smc({account_state.code, account_state.data});
    ton::SmartContract::Args args;
    auto libraries_root = block_->mc_block_.config_->get_libraries_root();
    if (libraries_root.not_null()) {
      args.set_libraries(vm::Dictionary(libraries_root, 256));
    }
    args.set_config(block_->mc_block_.config_);
    args.set_now(now);
    args.set_address(account_state.account);
    args.set_stack(std::move(input));
    args.set_method_id(method_id);

    auto result = smc.run_get_method(args);
    if (!result.success) {
      return td::Status::Error(method_id + " failed");
    }
    return result.stack->extract_contents();
  }

  td::Result<std::optional<const schema::AccountState*>> get_elector_account_state() const {
    TRY_RESULT(elector_address, get_elector_std_address());
    const schema::AccountState* latest_state = nullptr;
    for (const auto& account_state : block_->account_states_) {
      if (account_state.account != elector_address || account_state.code.is_null() || account_state.data.is_null()) {
        continue;
      }
      if (latest_state == nullptr || account_state.last_trans_lt > latest_state->last_trans_lt) {
        latest_state = &account_state;
      }
    }
    if (latest_state == nullptr) {
      return std::optional<const schema::AccountState*>{};
    }
    return std::optional<const schema::AccountState*>{latest_state};
  }

  void fill_validator_cycle_params(schema::ValidatorCycle& cycle) const {
    cycle.validators_elected_for = 0;
    cycle.elections_start_before = 0;
    cycle.elections_end_before = 0;
    cycle.stake_held_for = 0;
    cycle.max_validators = 0;
    cycle.max_main_validators = 0;
    cycle.min_validators = 0;
    cycle.min_stake = zero_refint();
    cycle.max_stake = zero_refint();
    cycle.min_total_stake = zero_refint();
    cycle.max_stake_factor = 0;
    if (!block_->mc_block_.config_) {
      return;
    }

    auto config15_cell = block_->mc_block_.config_->get_config_param(15);
    if (config15_cell.not_null()) {
      block::gen::ConfigParam::Record_cons15 config15;
      if (block::gen::ConfigParam(15).cell_unpack(config15_cell, config15)) {
        cycle.validators_elected_for = config15.validators_elected_for;
        cycle.elections_start_before = config15.elections_start_before;
        cycle.elections_end_before = config15.elections_end_before;
        cycle.stake_held_for = config15.stake_held_for;
      }
    }

    auto config16_cell = block_->mc_block_.config_->get_config_param(16);
    if (config16_cell.not_null()) {
      block::gen::ConfigParam::Record_cons16 config16;
      if (block::gen::ConfigParam(16).cell_unpack(config16_cell, config16)) {
        cycle.max_validators = config16.max_validators;
        cycle.max_main_validators = config16.max_main_validators;
        cycle.min_validators = config16.min_validators;
      }
    }

    auto config17_cell = block_->mc_block_.config_->get_config_param(17);
    if (config17_cell.not_null()) {
      block::gen::ConfigParam::Record_cons17 config17;
      if (block::gen::ConfigParam(17).cell_unpack(config17_cell, config17)) {
        auto min_stake = block::tlb::t_Grams.as_integer(config17.min_stake);
        auto max_stake = block::tlb::t_Grams.as_integer(config17.max_stake);
        auto min_total_stake = block::tlb::t_Grams.as_integer(config17.min_total_stake);
        if (min_stake.not_null()) {
          cycle.min_stake = min_stake;
        }
        if (max_stake.not_null()) {
          cycle.max_stake = max_stake;
        }
        if (min_total_stake.not_null()) {
          cycle.min_total_stake = min_total_stake;
        }
        cycle.max_stake_factor = config17.max_stake_factor;
      }
    }
  }

  struct PastElectionInfo {
    uint32_t election_id;
    td::RefInt256 total_stake;
  };

  struct ParsedValidatorCycle {
    schema::ValidatorCycle cycle;
    std::string vset_hash;
  };

  td::Result<std::unordered_map<std::string, PastElectionInfo>> get_past_elections_by_vset_hash(
      const schema::AccountState& elector_state) const {
    std::unordered_map<std::string, PastElectionInfo> result;
    TRY_RESULT(stack, run_get_method_at_time(elector_state, "past_elections", {}, current_block_time()));
    if (stack.empty()) {
      return result;
    }
    TRY_RESULT(elections, stack_list_to_vector(stack[0], "past_elections"));
    for (const auto& election_entry : elections) {
      auto election_tuple = election_entry.as_tuple();
      if (election_tuple.is_null() || election_tuple->size() < 2) {
        continue;
      }
      auto election_id = stack_int_to_u32(election_tuple->at(0), "past_election_id");
      if (election_id.is_error()) {
        continue;
      }
      auto election_id_value = election_id.move_as_ok();
      td::Result<std::string> vset_hash = td::Status::Error("past election vset hash not found");
      td::Result<td::RefInt256> total_stake = td::Status::Error("past election total stake not found");
      if (election_tuple->size() >= 6) {
        vset_hash = stack_int_to_hex(election_tuple->at(3), "past_election_vset_hash");
        total_stake = stack_int(election_tuple->at(5), "past_election_total_stake");
      } else {
        auto data_tuple = election_tuple->at(1).as_tuple();
        if (data_tuple.not_null() && data_tuple->size() >= 5) {
          vset_hash = stack_int_to_hex(data_tuple->at(2), "past_election_vset_hash");
          total_stake = stack_int(data_tuple->at(4), "past_election_total_stake");
        }
      }
      if (vset_hash.is_ok() && total_stake.is_ok()) {
        auto normalized_hash = vset_hash.move_as_ok();
        result[normalized_hash] = PastElectionInfo{
            .election_id = election_id_value,
            .total_stake = total_stake.move_as_ok(),
        };
      }
    }
    return result;
  }

  td::Result<std::optional<ParsedValidatorCycle>> parse_validator_cycle(
      int32_t config_id,
      uint32_t mc_seqno,
      const std::unordered_map<std::string, PastElectionInfo>& past_elections_by_vset_hash) const {
    if (!block_->mc_block_.config_) {
      return td::Status::Error("Config is not available");
    }
    auto validators_cell = block_->mc_block_.config_->get_config_param(config_id);
    if (validators_cell.is_null()) {
      return std::optional<ParsedValidatorCycle>{};
    }
    TRY_RESULT(vset, block::Config::unpack_validator_set(validators_cell));
    auto vset_hash = validators_cell->get_hash().to_hex();

    schema::ValidatorCycle cycle;
    cycle.utime_since = vset->utime_since;
    cycle.utime_until = vset->utime_until;
    cycle.total = static_cast<uint32_t>(vset->total);
    cycle.main = static_cast<uint32_t>(vset->main);
    cycle.total_weight = vset->total_weight;
    auto past_election_it = past_elections_by_vset_hash.find(vset_hash);
    if (past_election_it != past_elections_by_vset_hash.end()) {
      cycle.election_id = past_election_it->second.election_id;
      cycle.total_stake = past_election_it->second.total_stake;
    }
    cycle.source_mc_seqno = mc_seqno;
    fill_validator_cycle_params(cycle);

    uint32_t index = 0;
    for (const auto& validator : vset->list) {
      cycle.members.push_back(schema::ValidatorCycleMember{
          .utime_since = cycle.utime_since,
          .validator_index = index++,
          .validator_pubkey = validator.pubkey.as_bits256().to_hex(),
          .adnl_addr = validator.adnl_addr.to_hex(),
          .weight = validator.weight,
          .source_mc_seqno = mc_seqno,
      });
    }
    return std::optional<ParsedValidatorCycle>{ParsedValidatorCycle{
        .cycle = std::move(cycle),
        .vset_hash = std::move(vset_hash),
    }};
  }

  td::Result<std::optional<schema::ValidatorElection>> parse_validator_election(
      const schema::AccountState& elector_state,
      uint32_t mc_seqno) const {
    TRY_RESULT(stack, run_get_method_at_time(elector_state, "participant_list_extended", {}, current_block_time()));
    if (stack.size() < 7) {
      return td::Status::Error("participant_list_extended returned too few stack entries");
    }

    TRY_RESULT(election_id, stack_int_to_u32(stack[0], "elect_at"));
    if (election_id == 0) {
      return std::optional<schema::ValidatorElection>{};
    }

    schema::ValidatorElection election;
    election.election_id = election_id;
    TRY_RESULT_ASSIGN(election.elect_close, stack_int_to_u32(stack[1], "elect_close"));
    TRY_RESULT_ASSIGN(election.min_stake, stack_int(stack[2], "min_stake"));
    TRY_RESULT_ASSIGN(election.total_stake, stack_int(stack[3], "total_stake"));
    TRY_RESULT_ASSIGN(election.failed, stack_int_to_bool(stack[5], "failed"));
    TRY_RESULT_ASSIGN(election.finished, stack_int_to_bool(stack[6], "finished"));
    election.source_mc_seqno = mc_seqno;

    TRY_RESULT(participants, stack_list_to_vector(stack[4], "participants"));
    for (const auto& participant_entry : participants) {
      auto participant_tuple = participant_entry.as_tuple();
      if (participant_tuple.is_null() || participant_tuple->size() < 2) {
        return td::Status::Error("Invalid participant entry");
      }
      auto participant_data = participant_tuple->at(1).as_tuple();
      if (participant_data.is_null() || participant_data->size() < 4) {
        return td::Status::Error("Invalid participant data");
      }

      schema::ValidatorElectionParticipant participant;
      participant.election_id = election.election_id;
      TRY_RESULT_ASSIGN(participant.validator_pubkey, stack_int_to_hex(participant_tuple->at(0), "participant_pubkey"));
      TRY_RESULT_ASSIGN(participant.stake, stack_int(participant_data->at(0), "participant_stake"));
      TRY_RESULT_ASSIGN(participant.max_factor, stack_int_to_u32(participant_data->at(1), "participant_max_factor"));
      TRY_RESULT(addr, stack_int(participant_data->at(2), "participant_stake_holder"));
      participant.stake_holder_address = "-1:" + td::hex_string(addr, true, 64);
      TRY_RESULT_ASSIGN(participant.adnl_addr, stack_int_to_hex(participant_data->at(3), "participant_adnl"));
      participant.source_mc_seqno = mc_seqno;
      election.participants.push_back(std::move(participant));
    }

    return std::optional<schema::ValidatorElection>{std::move(election)};
  }

  std::optional<std::string> find_cycle_adnl(const schema::ValidatorCycle& cycle, const std::string& validator_pubkey) const {
    for (const auto& member : cycle.members) {
      if (member.validator_pubkey == validator_pubkey) {
        return member.adnl_addr;
      }
    }
    return std::nullopt;
  }

  td::Result<std::vector<schema::ValidatorComplaint>> parse_validator_complaints(
      const schema::AccountState& elector_state,
      const schema::ValidatorCycle& cycle,
      const std::unordered_map<std::string, const schema::ValidatorCycle*>& cycles_by_vset_hash,
      const schema::ValidatorCycle* current_voting_cycle,
      uint32_t mc_seqno) const {
    std::vector<schema::ValidatorComplaint> result;
    if (!cycle.election_id.has_value()) {
      return result;
    }
    auto election_id = cycle.election_id.value();
    TRY_RESULT(stack, run_get_method_at_time(
        elector_state, "list_complaints", {vm::StackEntry(td::make_refint(election_id))}, current_block_time()));
    if (stack.empty()) {
      return result;
    }
    TRY_RESULT(complaints, stack_list_to_vector(stack[0], "complaints"));
    for (const auto& complaint_entry : complaints) {
      auto complaint_pair = complaint_entry.as_tuple();
      if (complaint_pair.is_null() || complaint_pair->size() < 2) {
        continue;
      }
      auto subdata = complaint_pair->at(1).as_tuple();
      if (subdata.is_null() || subdata->size() < 4) {
        continue;
      }
      auto complaint_data = subdata->at(0).as_tuple();
      if (complaint_data.is_null() || complaint_data->size() < 8) {
        continue;
      }

      schema::ValidatorComplaint complaint;
      complaint.election_id = election_id;
      TRY_RESULT_ASSIGN(complaint.complaint_hash, stack_int_to_hex(complaint_pair->at(0), "complaint_hash"));
      TRY_RESULT_ASSIGN(complaint.validator_pubkey, stack_int_to_hex(complaint_data->at(0), "complaint_validator_pubkey"));
      complaint.adnl_addr = find_cycle_adnl(cycle, complaint.validator_pubkey);
      if (!complaint.adnl_addr.has_value()) {
        LOG(WARNING) << "Validator complaint " << complaint.complaint_hash
                     << " references pubkey " << complaint.validator_pubkey
                     << " that is not present in cycle " << cycle.utime_since;
      }
      TRY_RESULT_ASSIGN(complaint.description_boc, stack_cell_to_boc(complaint_data->at(1), "complaint_description"));
      TRY_RESULT_ASSIGN(complaint.created_at, stack_int_to_u32(complaint_data->at(2), "complaint_created_at"));
      TRY_RESULT_ASSIGN(complaint.severity, stack_int_to_u32(complaint_data->at(3), "complaint_severity"));
      TRY_RESULT(reward_addr, stack_int(complaint_data->at(4), "complaint_reward_addr"));
      complaint.reward_address = "-1:" + td::hex_string(reward_addr, true, 64);
      TRY_RESULT_ASSIGN(complaint.paid, stack_int(complaint_data->at(5), "complaint_paid"));
      TRY_RESULT_ASSIGN(complaint.suggested_fine, stack_int(complaint_data->at(6), "complaint_suggested_fine"));
      TRY_RESULT_ASSIGN(complaint.suggested_fine_part, stack_int_to_u32(complaint_data->at(7), "complaint_suggested_fine_part"));
      TRY_RESULT(voters, stack_list_to_vector(subdata->at(1), "complaint_voters"));
      TRY_RESULT_ASSIGN(complaint.vset_id, stack_int_to_hex(subdata->at(2), "complaint_vset_id"));
      TRY_RESULT(raw_weight_remaining, stack_int_to_i64(subdata->at(3), "complaint_weight_remaining"));

      const schema::ValidatorCycle* voting_cycle = nullptr;
      if (!std::all_of(complaint.vset_id.begin(), complaint.vset_id.end(), [](char c) { return c == '0'; })) {
        auto voting_cycle_it = cycles_by_vset_hash.find(complaint.vset_id);
        if (voting_cycle_it != cycles_by_vset_hash.end()) {
          voting_cycle = voting_cycle_it->second;
        }
      }
      if (voting_cycle == nullptr && voters.empty()) {
        voting_cycle = current_voting_cycle != nullptr ? current_voting_cycle : &cycle;
      } else if (voting_cycle == nullptr) {
        LOG(WARNING) << "Validator complaint " << complaint.complaint_hash
                     << " has votes for validator set " << complaint.vset_id
                     << ", but that set is not present in config 32/34/36";
      }
      complaint.voted_validators = voters_to_json(voters, voting_cycle);

      complaint.is_passed = raw_weight_remaining < 0;
      const auto& weight_cycle = voting_cycle != nullptr ? *voting_cycle : cycle;
      TRY_RESULT(required_weight, complaint_required_weight(weight_cycle.total_weight));
      complaint.weight_remaining = voters.empty() ? required_weight : raw_weight_remaining;
      complaint.approved_percent = weight_cycle.total_weight == 0
          ? 0
          : std::round(((static_cast<double>(required_weight) - static_cast<double>(complaint.weight_remaining)) /
                        static_cast<double>(weight_cycle.total_weight) * 100.0) * 1000.0) / 1000.0;
      complaint.source_mc_seqno = mc_seqno;
      result.push_back(std::move(complaint));
    }
    return result;
  }

  td::Status process_validator_snapshots() {
    if (!block_->mc_block_.config_) {
      // Validator snapshots are derived from masterchain config, so there is nothing reliable to read without it.
      return td::Status::OK();
    }
    const auto mc_seqno = current_mc_seqno();
    auto triggers = collect_validator_snapshot_triggers();
    if (!triggers.has_work()) {
      // Most masterchain blocks only run elector tick without changing elector data; skip expensive get-method snapshots.
      return td::Status::OK();
    }

    std::unordered_map<std::string, PastElectionInfo> past_elections_by_vset_hash;
    std::optional<const schema::AccountState*> elector_state;
    auto elector_state_r = get_elector_account_state();
    if (elector_state_r.is_ok()) {
      elector_state = elector_state_r.move_as_ok();
    } else {
      LOG(DEBUG) << "Failed to find elector state: " << elector_state_r.move_as_error();
    }

    if (triggers.needs_cycles() && elector_state.has_value()) {
      auto past_elections_r = get_past_elections_by_vset_hash(*elector_state.value());
      if (past_elections_r.is_ok()) {
        past_elections_by_vset_hash = past_elections_r.move_as_ok();
      } else {
        LOG(DEBUG) << "Failed to read past elections: " << past_elections_r.move_as_error();
      }
    }

    struct ParsedCycle {
      schema::ValidatorCycle cycle;
      std::string vset_hash;
      int32_t config_id;
      bool has_complaints;
    };

    std::vector<ParsedCycle> current_cycles;
    std::unordered_set<uint32_t> seen_utime_since;
    if (triggers.needs_cycles()) {
      for (auto config_id : {34, 36, 32}) {
        auto cycle_r = parse_validator_cycle(config_id, mc_seqno, past_elections_by_vset_hash);
        if (cycle_r.is_error()) {
          LOG(DEBUG) << "Failed to parse validator cycle config " << config_id << ": " << cycle_r.move_as_error();
          continue;
        }
        auto cycle = cycle_r.move_as_ok();
        if (!cycle.has_value()) {
          continue;
        }
        auto parsed_cycle_value = std::move(cycle.value());
        if (!seen_utime_since.insert(parsed_cycle_value.cycle.utime_since).second) {
          continue;
        }
        auto has_complaints = config_id == 32 || config_id == 34;
        if (triggers.has_key_block) {
          block_->validator_cycles_.push_back(parsed_cycle_value.cycle);
        }
        current_cycles.push_back(ParsedCycle{
            .cycle = std::move(parsed_cycle_value.cycle),
            .vset_hash = std::move(parsed_cycle_value.vset_hash),
            .config_id = config_id,
            .has_complaints = has_complaints,
        });
      }
    }

    if (!elector_state.has_value()) {
      // Elector get-method snapshots need current elector code/data; config-only cycle parsing above is all we can do here.
      return td::Status::OK();
    }

    std::unordered_map<std::string, const schema::ValidatorCycle*> cycles_by_vset_hash;
    const schema::ValidatorCycle* current_voting_cycle = nullptr;
    for (const auto& parsed_cycle : current_cycles) {
      cycles_by_vset_hash[parsed_cycle.vset_hash] = &parsed_cycle.cycle;
      if (parsed_cycle.config_id == 34) {
        current_voting_cycle = &parsed_cycle.cycle;
      }
    }

    if (triggers.active_election_dirty) {
      auto election_r = parse_validator_election(*elector_state.value(), mc_seqno);
      if (election_r.is_ok()) {
        auto election = election_r.move_as_ok();
        if (election.has_value()) {
          block_->validator_elections_.push_back(std::move(election.value()));
        }
      } else {
        LOG(DEBUG) << "Failed to parse validator election: " << election_r.move_as_error();
      }
    }

    if (!triggers.has_key_block && triggers.complaint_election_ids.empty()) {
      // No accepted complaint/vote touched elector data, so list_complaints would be unchanged for this block.
      return td::Status::OK();
    }

    for (const auto& parsed_cycle : current_cycles) {
      if (!parsed_cycle.has_complaints) {
        continue;
      }
      if (!parsed_cycle.cycle.election_id.has_value()) {
        continue;
      }
      auto election_id = parsed_cycle.cycle.election_id.value();
      if (!triggers.has_key_block &&
          triggers.complaint_election_ids.find(election_id) == triggers.complaint_election_ids.end()) {
        continue;
      }
      auto complaints_r = parse_validator_complaints(
          *elector_state.value(), parsed_cycle.cycle, cycles_by_vset_hash, current_voting_cycle, mc_seqno);
      if (complaints_r.is_error()) {
        LOG(DEBUG) << "Failed to parse validator complaints for election " << election_id
                   << ": " << complaints_r.move_as_error();
        continue;
      }
      for (auto& complaint : complaints_r.move_as_ok()) {
        block_->validator_complaints_.push_back(std::move(complaint));
      }
    }

    return td::Status::OK();
  }

  bool has_nominator_pool_interface(const block::StdAddress& address) const {
    auto interfaces_it = block_->account_interfaces_.find(address);
    if (interfaces_it == block_->account_interfaces_.end()) {
      return false;
    }
    for (const auto& interface : interfaces_it->second) {
      if (std::holds_alternative<schema::NominatorPoolData>(interface)) {
        return true;
      }
    }
    return false;
  }

  td::Result<td::Bits256> decode_rand_seed(const schema::Block& block) {
    TRY_RESULT(decoded, td::base64_decode(block.rand_seed));
    if (decoded.size() != 32) {
      return td::Status::Error("Invalid block rand_seed size");
    }
    td::Bits256 rand_seed;
    rand_seed.as_slice().copy_from(decoded);
    return rand_seed;
  }

  bool is_recover_stake_ok(const schema::Transaction& transaction) const {
    if (!transaction.in_msg || transaction.in_msg->body.is_null()) {
      return false;
    }
    auto in_msg_body_cs = vm::load_cell_slice_ref(transaction.in_msg->body);
    if (in_msg_body_cs->size() < 32 ||
        in_msg_body_cs->prefetch_ulong(32) != nominator_pool::RECOVER_STAKE_OK_OPCODE) {
      return false;
    }
    if (!transaction.in_msg->source || transaction.in_msg->source.value() != nominator_pool::ELECTOR_ADDRESS) {
      return false;
    }
    if (!transaction.in_msg->value) {
      return false;
    }
    auto* descr = std::get_if<schema::TransactionDescr_ord>(&transaction.description);
    if (descr == nullptr || descr->aborted) {
      return false;
    }
    auto* compute_ph = std::get_if<schema::TrComputePhase_vm>(&descr->compute_ph);
    return compute_ph != nullptr && compute_ph->success;
  }

  td::Result<td::Ref<vm::Cell>> make_shard_account(td::Ref<vm::Cell> account_cell,
                                                   const td::Bits256& last_trans_hash,
                                                   uint64_t last_trans_lt) {
    vm::CellBuilder cb;
    if (!cb.store_ref_bool(std::move(account_cell)) ||
        !cb.store_bits_bool(last_trans_hash) ||
        !cb.store_long_bool(last_trans_lt, 64)) {
      return td::Status::Error("Failed to build ShardAccount");
    }
    return cb.finalize();
  }

  td::Result<block::Account> load_account_from_celldb(const block::StdAddress& address,
                                                      const td::Bits256& account_state_hash,
                                                      const td::Bits256& last_trans_hash,
                                                      uint64_t last_trans_lt,
                                                      uint32_t now) {
    if (!block_->cell_db_reader_) {
      return td::Status::Error("cell_db_reader not available");
    }

    auto account_cell_r = block_->cell_db_reader_->load_cell(account_state_hash.as_slice());
    if (account_cell_r.is_error()) {
      return account_cell_r.move_as_error_prefix("Failed to load account state: ");
    }
    TRY_RESULT(shard_account_cell, make_shard_account(account_cell_r.move_as_ok(), last_trans_hash, last_trans_lt));

    bool is_special = address.workchain == ton::masterchainId &&
                      block_->mc_block_.config_->is_special_smartcontract(address.addr);
    block::Account account(address.workchain, address.addr.bits());
    if (!account.unpack(vm::load_cell_slice_ref(shard_account_cell), now, is_special)) {
      return td::Status::Error("Failed to unpack ShardAccount");
    }
    return account;
  }

  td::Result<block::Account> make_initial_account(const std::vector<const schema::Transaction*>& transactions) {
    if (transactions.empty()) {
      return td::Status::Error("No transactions for pool replay");
    }

    const auto& first_transaction = *transactions.front();
    return load_account_from_celldb(first_transaction.account,
                                    first_transaction.account_state_hash_before,
                                    first_transaction.prev_trans_hash,
                                    first_transaction.prev_trans_lt,
                                    first_transaction.now);
  }

  td::Result<nominator_pool::ParsedStorage> parse_pool_storage(const block::Account& account) {
    if (account.code.is_null() || account.code->get_hash().to_hex() != nominator_pool::CODE_HASH) {
      return td::Status::Error("Not a nominator pool account");
    }
    return nominator_pool::parse_storage(account.data);
  }

  td::Result<nominator_pool::ParsedStorage> parse_transaction_after_storage(const schema::Transaction& transaction) {
    TRY_RESULT(account, load_account_from_celldb(transaction.account,
                                                transaction.account_state_hash_after,
                                                transaction.hash,
                                                transaction.lt,
                                                transaction.now));
    return parse_pool_storage(account);
  }

  struct PoolNominatorSnapshot {
    td::RefInt256 amount;
    td::RefInt256 pending_deposit_amount;
    bool withdraw_requested{false};
  };

  td::RefInt256 abs_refint(td::RefInt256 value) const {
    if (td::sgn(value) >= 0) {
      return value;
    }
    return zero_refint() - value;
  }

  struct NominatorPoolMessageHeader {
    std::uint32_t opcode;
    std::optional<std::uint64_t> query_id;
  };

  std::optional<NominatorPoolMessageHeader> parse_nominator_pool_message_header(
      const schema::Transaction& transaction) const {
    if (!transaction.in_msg || transaction.in_msg->body.is_null()) {
      return std::nullopt;
    }
    auto body_ref = vm::load_cell_slice_ref(transaction.in_msg->body);
    auto& body = body_ref.write();
    if (body.size() < 32) {
      return std::nullopt;
    }

    NominatorPoolMessageHeader header{
        .opcode = static_cast<std::uint32_t>(body.fetch_ulong(32)),
        .query_id = std::nullopt,
    };
    if (body.size() >= 64) {
      header.query_id = static_cast<std::uint64_t>(body.fetch_ulong(64));
    }
    return header;
  }

  std::map<std::string, PoolNominatorSnapshot> build_nominator_snapshots(
      const nominator_pool::ParsedStorage& storage) const {
    std::map<std::string, PoolNominatorSnapshot> result;
    for (const auto& nominator : storage.nominators) {
      result[convert::to_raw_address(nominator.address)] = PoolNominatorSnapshot{
          .amount = nominator.amount,
          .pending_deposit_amount = nominator.pending_deposit_amount,
          .withdraw_requested = false,
      };
    }
    for (const auto& address : storage.withdraw_requests) {
      auto& snapshot = result[convert::to_raw_address(address)];
      snapshot.withdraw_requested = true;
    }
    for (auto& [_, snapshot] : result) {
      if (snapshot.amount.is_null()) {
        snapshot.amount = zero_refint();
      }
      if (snapshot.pending_deposit_amount.is_null()) {
        snapshot.pending_deposit_amount = zero_refint();
      }
    }
    return result;
  }

  PoolNominatorSnapshot get_nominator_snapshot(
      const std::map<std::string, PoolNominatorSnapshot>& snapshots,
      const std::string& address) const {
    auto it = snapshots.find(address);
    if (it != snapshots.end()) {
      return it->second;
    }
    return PoolNominatorSnapshot{
        .amount = zero_refint(),
        .pending_deposit_amount = zero_refint(),
        .withdraw_requested = false,
    };
  }

  schema::NominatorPoolEvent make_nominator_pool_event(
      const schema::Transaction& transaction,
      const std::string& nominator_address,
      uint32_t event_index,
      std::string event_type,
      td::RefInt256 amount,
      td::RefInt256 balance_delta,
      td::RefInt256 pending_balance_delta,
      const PoolNominatorSnapshot& before,
      const PoolNominatorSnapshot& after) const {
    schema::NominatorPoolEvent event;
    event.trace_id = transaction.trace_id;
    event.transaction_hash = transaction.hash;
    event.transaction_lt = transaction.lt;
    event.transaction_now = transaction.now;
    event.mc_seqno = transaction.mc_seqno;
    event.pool_address = convert::to_raw_address(transaction.account);
    event.nominator_address = nominator_address;
    event.event_index = event_index;
    event.event_type = std::move(event_type);
    event.amount = amount;
    event.balance_delta = balance_delta;
    event.pending_balance_delta = pending_balance_delta;
    event.balance_before = before.amount;
    event.balance_after = after.amount;
    event.pending_balance_before = before.pending_deposit_amount;
    event.pending_balance_after = after.pending_deposit_amount;
    event.withdraw_request_before = before.withdraw_requested;
    event.withdraw_request_after = after.withdraw_requested;
    return event;
  }

  std::optional<schema::NominatorPoolValidatorEvent> build_nominator_pool_validator_event(
      const schema::Transaction& transaction,
      const nominator_pool::ParsedStorage& before,
      const nominator_pool::ParsedStorage& after) const {
    auto balance_delta = after.validator_amount - before.validator_amount;
    if (td::sgn(balance_delta) == 0) {
      return std::nullopt;
    }

    const bool is_reward_transaction = is_recover_stake_ok(transaction);
    auto header = parse_nominator_pool_message_header(transaction);

    std::string event_type;
    if (is_reward_transaction) {
      event_type = td::sgn(balance_delta) > 0 ? "reward" : "penalty";
    } else if (header.has_value() && header->opcode == NOMINATOR_POOL_VALIDATOR_DEPOSIT) {
      event_type = "deposit";
    } else if (header.has_value() && header->opcode == NOMINATOR_POOL_VALIDATOR_WITHDRAWAL) {
      event_type = "withdrawal";
    } else {
      return std::nullopt;
    }

    schema::NominatorPoolValidatorEvent event;
    event.transaction_hash = transaction.hash;
    event.transaction_lt = transaction.lt;
    event.transaction_now = transaction.now;
    event.mc_seqno = transaction.mc_seqno;
    event.pool_address = convert::to_raw_address(transaction.account);
    event.validator_address = convert::to_raw_address(before.validator_address);
    event.event_type = std::move(event_type);
    event.amount = abs_refint(balance_delta);
    event.balance_delta = balance_delta;
    event.balance_before = before.validator_amount;
    event.balance_after = after.validator_amount;
    if (header.has_value()) {
      event.query_id = header->query_id;
    }
    if (is_reward_transaction && before.stake_at != 0) {
      event.cycle_start = before.stake_at;
    }
    event.validator_reward_share = static_cast<std::uint32_t>(before.validator_reward_share);
    return event;
  }

  std::vector<schema::NominatorPoolEvent> build_nominator_pool_events(
      const schema::Transaction& transaction,
      const nominator_pool::ParsedStorage& before,
      const nominator_pool::ParsedStorage& after) {
    auto before_snapshots = build_nominator_snapshots(before);
    auto after_snapshots = build_nominator_snapshots(after);
    for (const auto& [address, _] : after_snapshots) {
      before_snapshots.try_emplace(address, get_nominator_snapshot(before_snapshots, address));
    }

    std::vector<schema::NominatorPoolEvent> events;
    uint32_t event_index = 0;
    bool is_reward_transaction = is_recover_stake_ok(transaction);
    for (const auto& [address, before_snapshot] : before_snapshots) {
      auto after_snapshot = get_nominator_snapshot(after_snapshots, address);
      auto balance_delta = after_snapshot.amount - before_snapshot.amount;
      auto pending_balance_delta = after_snapshot.pending_deposit_amount - before_snapshot.pending_deposit_amount;
      auto total_delta = balance_delta + pending_balance_delta;

      if (is_reward_transaction) {
        auto reward_after_snapshot = before_snapshot;
        if (td::sgn(total_delta) != 0) {
          reward_after_snapshot.amount = before_snapshot.amount + total_delta;
          events.push_back(make_nominator_pool_event(
              transaction, address, event_index++, "reward", total_delta, total_delta, zero_refint(),
              before_snapshot, reward_after_snapshot));
        }
        if (td::sgn(pending_balance_delta) < 0) {
          auto activated_amount = abs_refint(pending_balance_delta);
          auto activation_before_snapshot = reward_after_snapshot;
          events.push_back(make_nominator_pool_event(
              transaction, address, event_index++, "pending_deposit_activation", activated_amount,
              activated_amount, pending_balance_delta, activation_before_snapshot, after_snapshot));
        }
      } else {
        if (td::sgn(total_delta) > 0) {
          events.push_back(make_nominator_pool_event(
              transaction, address, event_index++, "deposit", total_delta, balance_delta, pending_balance_delta,
              before_snapshot, after_snapshot));
        } else if (td::sgn(total_delta) < 0) {
          events.push_back(make_nominator_pool_event(
              transaction, address, event_index++, "withdrawal", abs_refint(total_delta), balance_delta,
              pending_balance_delta, before_snapshot, after_snapshot));
        }
        if (!before_snapshot.withdraw_requested && after_snapshot.withdraw_requested) {
          events.push_back(make_nominator_pool_event(
              transaction, address, event_index++, "withdrawal_request", zero_refint(), zero_refint(),
              zero_refint(), before_snapshot, after_snapshot));
        }
      }
    }
    return events;
  }

  std::vector<schema::BlockchainEvent> build_nominator_pool_transaction_events(
      const schema::Transaction& transaction,
      const nominator_pool::ParsedStorage& before,
      const nominator_pool::ParsedStorage& after) {
    std::vector<schema::BlockchainEvent> events;

    auto nominator_events = build_nominator_pool_events(transaction, before, after);
    for (auto& event : nominator_events) {
      events.push_back(std::move(event));
    }

    auto validator_event = build_nominator_pool_validator_event(transaction, before, after);
    if (validator_event.has_value()) {
      events.push_back(std::move(validator_event.value()));
    }

    return events;
  }

  td::Result<std::vector<schema::BlockchainEvent>> replay_nominator_pool_account(
      const schema::Block& block,
      std::vector<const schema::Transaction*> transactions) {
    std::vector<schema::BlockchainEvent> events;
    std::sort(transactions.begin(), transactions.end(), [](const auto* lhs, const auto* rhs) {
      return lhs->lt < rhs->lt;
    });

    TRY_RESULT(account, make_initial_account(transactions));
    TRY_RESULT(prev_blocks_info, block_->mc_block_.config_->get_prev_blocks_info());
    TRY_RESULT(rand_seed, decode_rand_seed(block));

    emulator::TransactionEmulator trans_emulator(block_->mc_block_.config_);
    trans_emulator.set_prev_blocks_info(std::move(prev_blocks_info));
    trans_emulator.set_rand_seed(rand_seed);
    if (auto libraries_root = block_->mc_block_.config_->get_libraries_root(); libraries_root.not_null()) {
      trans_emulator.set_libs(vm::Dictionary(libraries_root, 256));
    }

    for (const auto* transaction : transactions) {
      if (transaction->raw.is_null()) {
        return td::Status::Error("Transaction raw cell is null");
      }

      auto before_storage = parse_pool_storage(account);
      auto emulation_result_r = trans_emulator.emulate_transaction(std::move(account), transaction->raw);
      // Workaround for old blocks that may not be compatible with the current emulator:
      // if the pool had only one transaction in the block, use the canonical after-state
      // from celldb instead of dropping the reward event.
      if (emulation_result_r.is_error()) {
        auto emulation_error = emulation_result_r.move_as_error();
        if (transactions.size() == 1 && before_storage.is_ok()) {
          auto after_storage_r = parse_transaction_after_storage(*transaction);
          if (after_storage_r.is_ok()) {
            auto transaction_events = build_nominator_pool_transaction_events(
                *transaction, before_storage.move_as_ok(), after_storage_r.move_as_ok());
            for (auto& event : transaction_events) {
              events.push_back(std::move(event));
            }
            return events;
          }
        }
        return std::move(emulation_error);
      }
      auto emulation_result = emulation_result_r.move_as_ok();

      if (before_storage.is_ok()) {
        TRY_RESULT(after_storage, parse_pool_storage(emulation_result.account));
        auto transaction_events = build_nominator_pool_transaction_events(
            *transaction, before_storage.move_as_ok(), after_storage);
        for (auto& event : transaction_events) {
          events.push_back(std::move(event));
        }
      }
      account = std::move(emulation_result.account);
    }

    return events;
  }

  td::Result<std::vector<schema::BlockchainEvent>> process_nominator_pool_events(const schema::Block& block) {
    std::unordered_set<block::StdAddress> candidate_accounts;
    for (const auto& transaction : block.transactions) {
      if (has_nominator_pool_interface(transaction.account)) {
        candidate_accounts.insert(transaction.account);
      }
    }

    std::unordered_map<block::StdAddress, std::vector<const schema::Transaction*>> transactions_by_account;
    for (const auto& transaction : block.transactions) {
      if (candidate_accounts.find(transaction.account) != candidate_accounts.end()) {
        transactions_by_account[transaction.account].push_back(&transaction);
      }
    }

    std::vector<schema::BlockchainEvent> events;
    for (auto& [_, account_transactions] : transactions_by_account) {
      auto account_events = replay_nominator_pool_account(block, std::move(account_transactions));
      if (account_events.is_error()) {
        return account_events.move_as_error();
      }
      for (auto& event : account_events.move_as_ok()) {
        events.push_back(std::move(event));
      }
    }
    return events;
  }
};
