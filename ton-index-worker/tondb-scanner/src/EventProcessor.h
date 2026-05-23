#pragma once
#include "InterfaceDetectors.hpp"
#include "Statistics.h"
#include "common/refint.h"
#include "DataParser.h"
#include "emulator/transaction-emulator.h"
#include "smc-interfaces/NominatorPool.h"
#include "td/utils/base64.h"
#include "validators-tlb.h"

#include <algorithm>
#include <map>
#include <unordered_map>
#include <unordered_set>


// Detects special cases of Actions like - Jetton transfers and burns, NFT transfers
class ActionDetector: public td::actor::Actor {
private:
  ParsedBlockPtr block_;
  td::Promise<ParsedBlockPtr> promise_;
  td::Timer timer_{true};
public:
  ActionDetector(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise): block_(block), promise_(std::move(promise)) {
  }

  void start_up() override {
    timer_.resume();
    for (const auto& block : block_->blocks_) {
      for (const auto& transaction : block.transactions) {
        process_tx(transaction);
      }
      auto pool_events = process_nominator_pool_events(block);
      if (pool_events.is_error()) {
        LOG(DEBUG) << "Failed to replay nominator pool events: " << pool_events.move_as_error();
      } else {
        for (auto& pool_event : pool_events.move_as_ok()) {
          block_->events_.push_back(std::move(pool_event));
        }
      }
    }
    g_statistics.record_time(DETECT_ACTIONS_SEQNO, timer_.elapsed() * 1e6);
    promise_.set_value(std::move(block_));
    stop();
  }

  void process_tx(const schema::Transaction& transaction) {
    auto interfaces_it = block_->account_interfaces_.find(transaction.account);

    if (interfaces_it == block_->account_interfaces_.end()) {
      return;
    }
    auto interfaces = interfaces_it->second;

    if (!transaction.in_msg) {
      return;
    }
    
    auto in_msg_body_cs = vm::load_cell_slice_ref(transaction.in_msg.value().body);

    for (auto& v : interfaces) {
      if (auto jetton_wallet_ptr = std::get_if<schema::JettonWalletDataV2>(&v)) {
        if (tokens::gen::t_InternalMsgBody.check_tag(*in_msg_body_cs) == tokens::gen::InternalMsgBody::transfer_jetton) {
          auto transfer = parse_jetton_transfer(*jetton_wallet_ptr, transaction, in_msg_body_cs);
          if (transfer.is_error()) {
            LOG(DEBUG) << "Failed to parse jetton transfer: " << transfer.move_as_error();
          } else {
            block_->events_.push_back(transfer.move_as_ok());
          }
        } else if (tokens::gen::t_InternalMsgBody.check_tag(*in_msg_body_cs) == tokens::gen::InternalMsgBody::burn) {
          auto burn = parse_jetton_burn(*jetton_wallet_ptr, transaction, in_msg_body_cs);
          if (burn.is_error()) {
            LOG(DEBUG) << "Failed to parse jetton burn: " << burn.move_as_error();
          } else {
            block_->events_.push_back(burn.move_as_ok());
          }
        }
      }

      if (auto nft_item_ptr = std::get_if<schema::NFTItemDataV2>(&v)) {
        if (tokens::gen::t_InternalMsgBody.check_tag(*in_msg_body_cs) == tokens::gen::InternalMsgBody::transfer_nft) {
          auto transfer = parse_nft_transfer(*nft_item_ptr, transaction, in_msg_body_cs);
          if (transfer.is_error()) {
            LOG(DEBUG) << "Failed to parse nft transfer: " << transfer.move_as_error();
          } else {
            block_->events_.push_back(transfer.move_as_ok());
          }
        }
      }
    }
  }

  td::Result<schema::JettonTransfer> parse_jetton_transfer(const schema::JettonWalletDataV2& jetton_wallet, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs) {
    tokens::gen::InternalMsgBody::Record_transfer_jetton transfer_record;
    if (!tlb::csr_unpack_inexact(in_msg_body_cs, transfer_record)) {
      return td::Status::Error("Failed to unpack transfer");
    }

    schema::JettonTransfer transfer;
    transfer.trace_id = transaction.trace_id;
    transfer.transaction_hash = transaction.hash;
    transfer.transaction_lt = transaction.lt;
    transfer.transaction_now = transaction.now;
    if (auto* v = std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
      transfer.transaction_aborted = v->aborted;
    } else {
      return td::Status::Error("Unexpected transaction description");
    }
    transfer.mc_seqno = transaction.mc_seqno;

    transfer.query_id = transfer_record.query_id;
    transfer.amount = block::tlb::t_VarUInteger_16.as_integer(transfer_record.amount);
    if (transfer.amount.is_null()) {
      return td::Status::Error("Failed to unpack transfer amount");
    }
    if (!transaction.in_msg || !transaction.in_msg->source) {
      return td::Status::Error("Failed to unpack transfer source");
    }
    transfer.source = transaction.in_msg->source.value();
    transfer.jetton_wallet = convert::to_raw_address(transaction.account);
    transfer.jetton_master = convert::to_raw_address(jetton_wallet.jetton);
    auto destination = convert::to_raw_address(transfer_record.destination);
    if (destination.is_error()) {
      return destination.move_as_error_prefix("Failed to unpack transfer destination: ");
    }
    transfer.destination = destination.move_as_ok();
    auto response_destination = convert::to_raw_address(transfer_record.response_destination);
    if (response_destination.is_error()) {
      return response_destination.move_as_error_prefix("Failed to unpack transfer response destination: ");
    }
    transfer.response_destination = response_destination.move_as_ok();
    if (!transfer_record.custom_payload.write().fetch_maybe_ref(transfer.custom_payload)) {
      return td::Status::Error("Failed to fetch custom payload");
    }
    transfer.forward_ton_amount = block::tlb::t_VarUInteger_16.as_integer(transfer_record.forward_ton_amount);
    if (!transfer_record.forward_payload.write().fetch_maybe_ref(transfer.forward_payload)) {
      return td::Status::Error("Failed to fetch forward payload");
    }

    return transfer;
  }

  td::Result<schema::JettonBurn> parse_jetton_burn(const schema::JettonWalletDataV2& jetton_wallet, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs) {
    tokens::gen::InternalMsgBody::Record_burn burn_record;
    if (!tlb::csr_unpack_inexact(in_msg_body_cs, burn_record)) {
      return td::Status::Error("Failed to unpack burn");
    }

    schema::JettonBurn burn;
    burn.trace_id = transaction.trace_id;
    burn.transaction_hash = transaction.hash;
    burn.transaction_lt = transaction.lt;
    burn.transaction_now = transaction.now;
    if (auto* v = std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
      burn.transaction_aborted = v->aborted;
    } else {
      return td::Status::Error("Unexpected transaction description");
    }
    burn.mc_seqno = transaction.mc_seqno;

    burn.query_id = burn_record.query_id;
    if (!transaction.in_msg || !transaction.in_msg->source) {
      return td::Status::Error("Failed to unpack burn source");
    }
    burn.owner = transaction.in_msg->source.value();
    burn.jetton_wallet = convert::to_raw_address(transaction.account);
    burn.jetton_master = convert::to_raw_address(jetton_wallet.jetton);
    burn.amount = block::tlb::t_VarUInteger_16.as_integer(burn_record.amount);
    if (burn.amount.is_null()) {
      return td::Status::Error("Failed to unpack burn amount");
    }
    auto response_destination = convert::to_raw_address(burn_record.response_destination);
    if (response_destination.is_error()) {
      return response_destination.move_as_error_prefix("Failed to unpack burn response destination: ");
    }
    burn.response_destination = response_destination.move_as_ok();
    if (!burn_record.custom_payload.write().fetch_maybe_ref(burn.custom_payload)) {
      return td::Status::Error("Failed to fetch custom payload");
    }

    return burn;
  }

  td::Result<schema::NFTTransfer> parse_nft_transfer(const schema::NFTItemDataV2& nft_item, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs) {
    tokens::gen::InternalMsgBody::Record_transfer_nft transfer_record;
    if (!tlb::csr_unpack_inexact(in_msg_body_cs, transfer_record)) {
      return td::Status::Error("Failed to unpack transfer");
    }

    schema::NFTTransfer transfer;
    transfer.trace_id = transaction.trace_id;
    transfer.transaction_hash = transaction.hash;
    transfer.transaction_lt = transaction.lt;
    transfer.transaction_now = transaction.now;
    if (auto* v = std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
      transfer.transaction_aborted = v->aborted;
    } else {
      transfer.transaction_aborted = 0;
    }
    transfer.mc_seqno = transaction.mc_seqno;

    transfer.query_id = transfer_record.query_id;
    transfer.nft_item = transaction.account;
    transfer.nft_item_index = nft_item.index;
    if (nft_item.collection_address.has_value()) {
      transfer.nft_collection = convert::to_raw_address(nft_item.collection_address.value());
    }
    if (!transaction.in_msg.has_value() || !transaction.in_msg.value().source) {
      return td::Status::Error("Failed to fetch NFT old owner address");
    }
    transfer.old_owner = transaction.in_msg.value().source.value();
    auto new_owner = convert::to_raw_address(transfer_record.new_owner);
    if (new_owner.is_error()) {
      return new_owner.move_as_error_prefix("Failed to unpack new owner address: ");
    }
    transfer.new_owner = new_owner.move_as_ok();
    auto response_destination = convert::to_raw_address(transfer_record.response_destination);
    if (response_destination.is_error()) {
      return response_destination.move_as_error_prefix("Failed to unpack response destination: ");
    }
    transfer.response_destination = response_destination.move_as_ok();
    if (!transfer_record.custom_payload.write().fetch_maybe_ref(transfer.custom_payload)) {
      return td::Status::Error("Failed to fetch custom payload");
    }
    transfer.forward_amount = block::tlb::t_VarUInteger_16.as_integer(transfer_record.forward_amount);
    if (!transfer_record.forward_payload.write().fetch_maybe_ref(transfer.forward_payload)) {
      return td::Status::Error("Failed to fetch forward payload");
    }

    return transfer;
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

  bool is_recover_stake_ok(const schema::Transaction& transaction) {
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
                                                   const schema::Transaction& first_transaction) {
    vm::CellBuilder cb;
    if (!cb.store_ref_bool(std::move(account_cell)) ||
        !cb.store_bits_bool(first_transaction.prev_trans_hash) ||
        !cb.store_long_bool(first_transaction.prev_trans_lt, 64)) {
      return td::Status::Error("Failed to build ShardAccount");
    }
    return cb.finalize();
  }

  td::Result<block::Account> make_initial_account(const std::vector<const schema::Transaction*>& transactions) {
    if (transactions.empty()) {
      return td::Status::Error("No transactions for pool replay");
    }
    if (!block_->cell_db_reader_) {
      return td::Status::Error("cell_db_reader not available");
    }

    const auto& first_transaction = *transactions.front();
    auto account_cell_r = block_->cell_db_reader_->load_cell(first_transaction.account_state_hash_before.as_slice());
    if (account_cell_r.is_error()) {
      return account_cell_r.move_as_error_prefix("Failed to load initial account state: ");
    }
    TRY_RESULT(shard_account_cell, make_shard_account(account_cell_r.move_as_ok(), first_transaction));

    bool is_special = first_transaction.account.workchain == ton::masterchainId &&
                      block_->mc_block_.config_->is_special_smartcontract(first_transaction.account.addr);
    block::Account account(first_transaction.account.workchain, first_transaction.account.addr.bits());
    if (!account.unpack(vm::load_cell_slice_ref(shard_account_cell), first_transaction.now, is_special)) {
      return td::Status::Error("Failed to unpack initial ShardAccount");
    }
    return account;
  }

  td::Result<nominator_pool::ParsedStorage> parse_pool_storage(const block::Account& account) {
    if (account.code.is_null() || account.code->get_hash().to_hex() != nominator_pool::CODE_HASH) {
      return td::Status::Error("Not a nominator pool account");
    }
    return nominator_pool::parse_storage(account.data);
  }

  struct PoolNominatorSnapshot {
    td::RefInt256 amount;
    td::RefInt256 pending_deposit_amount;
    bool withdraw_requested{false};
  };

  td::RefInt256 zero_refint() const {
    return td::make_refint(td::BigInt256(0));
  }

  td::RefInt256 abs_refint(td::RefInt256 value) const {
    if (td::sgn(value) >= 0) {
      return value;
    }
    return zero_refint() - value;
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

  td::Result<std::vector<schema::NominatorPoolEvent>> replay_nominator_pool_account(
      const schema::Block& block,
      std::vector<const schema::Transaction*> transactions) {
    std::vector<schema::NominatorPoolEvent> events;
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
      TRY_RESULT(emulation_result, trans_emulator.emulate_transaction(std::move(account), transaction->raw));

      if (before_storage.is_ok()) {
        TRY_RESULT(after_storage, parse_pool_storage(emulation_result.account));
        auto transaction_events = build_nominator_pool_events(*transaction, before_storage.move_as_ok(), after_storage);
        for (auto& event : transaction_events) {
          events.push_back(std::move(event));
        }
      }
      account = std::move(emulation_result.account);
    }

    return events;
  }

  td::Result<std::vector<schema::NominatorPoolEvent>> process_nominator_pool_events(const schema::Block& block) {
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

    std::vector<schema::NominatorPoolEvent> events;
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

class EventProcessor : public td::actor::Actor {
private:
  td::actor::ActorOwn<InterfaceManager> interface_manager_;
  td::actor::ActorOwn<JettonMasterDetector> jetton_master_detector_;
  td::actor::ActorOwn<JettonWalletDetector> jetton_wallet_detector_;
  td::actor::ActorOwn<NFTCollectionDetector> nft_collection_detector_;
  td::actor::ActorOwn<NFTItemDetector> nft_item_detector_;
public:
  EventProcessor(td::actor::ActorId<InsertManagerInterface> insert_manager): 
    interface_manager_(td::actor::create_actor<InterfaceManager>("interface_manager", insert_manager)),
    jetton_master_detector_(td::actor::create_actor<JettonMasterDetector>("jetton_master_detector", interface_manager_.get(), insert_manager)), 
    jetton_wallet_detector_(td::actor::create_actor<JettonWalletDetector>("jetton_wallet_detector", jetton_master_detector_.get(), interface_manager_.get(), insert_manager)),
    nft_collection_detector_(td::actor::create_actor<NFTCollectionDetector>("nft_collection_detector", interface_manager_.get(), insert_manager)),
    nft_item_detector_(td::actor::create_actor<NFTItemDetector>("nft_item_detector", interface_manager_.get(), insert_manager, nft_collection_detector_.get())) {
  }

  void process(ParsedBlockPtr block, td::Promise<> &&promise);
  // void process(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise);

private:
  void process_states(const std::vector<schema::AccountState>& account_states, const schema::MasterchainBlockDataState& blocks_ds, td::Promise<std::vector<schema::BlockchainInterface>> &&promise);
  void process_transactions(const std::vector<schema::Transaction>& transactions, td::Promise<std::vector<schema::BlockchainEvent>> &&promise);
};
