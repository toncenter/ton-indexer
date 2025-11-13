#pragma once
#include "InterfaceDetectors.hpp"
#include "Statistics.h"
#include "common/refint.h"
#include "DataParser.h"


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
    }
    g_statistics.record_time(DETECT_ACTIONS_SEQNO, timer_.elapsed() * 1e6);
    promise_.set_value(std::move(block_));
    stop();
  }

  void process_tx(const schema::Transaction& transaction) {
    // check for recover_stake_ok from elector to nominator pool
    if (transaction.in_msg && !transaction.in_msg.value().body.is_null()) {
      auto in_msg_body_cs = vm::load_cell_slice_ref(transaction.in_msg.value().body);
      if (in_msg_body_cs->size() >= 32) {
        auto opcode = in_msg_body_cs->prefetch_ulong(32);
        if (opcode == 0xf96f7324) {  // recover_stake_ok from elector
          auto incomes = parse_nominator_pool_incomes(transaction);
          if (incomes.is_ok()) {
            for (auto& income : incomes.ok()) {
              block_->events_.push_back(std::move(income));
            }
          }
        }
      }
    }

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
      if (auto jetton_wallet_ptr = std::get_if<JettonWalletDataV2>(&v)) {
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

      if (auto nft_item_ptr = std::get_if<NFTItemDataV2>(&v)) {
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

  td::Result<JettonTransfer> parse_jetton_transfer(const JettonWalletDataV2& jetton_wallet, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs) {
    tokens::gen::InternalMsgBody::Record_transfer_jetton transfer_record;
    if (!tlb::csr_unpack_inexact(in_msg_body_cs, transfer_record)) {
      return td::Status::Error("Failed to unpack transfer");
    }

    JettonTransfer transfer;
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

  td::Result<JettonBurn> parse_jetton_burn(const JettonWalletDataV2& jetton_wallet, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs) {
    tokens::gen::InternalMsgBody::Record_burn burn_record;
    if (!tlb::csr_unpack_inexact(in_msg_body_cs, burn_record)) {
      return td::Status::Error("Failed to unpack burn");
    }

    JettonBurn burn;
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

  td::Result<NFTTransfer> parse_nft_transfer(const NFTItemDataV2& nft_item, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs) {
    tokens::gen::InternalMsgBody::Record_transfer_nft transfer_record;
    if (!tlb::csr_unpack_inexact(in_msg_body_cs, transfer_record)) {
      return td::Status::Error("Failed to unpack transfer");
    }

    NFTTransfer transfer;
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

  // find first transaction for account in current block by lt
  td::optional<schema::Transaction> find_first_tx_for_account(const block::StdAddress& account) {
    td::optional<schema::Transaction> first_tx;
    uint64_t min_lt = std::numeric_limits<uint64_t>::max();
    
    for (const auto& block : block_->blocks_) {
      for (const auto& tx : block.transactions) {
        if (tx.account == account && tx.lt < min_lt) {
          min_lt = tx.lt;
          first_tx = tx;
        }
      }
    }
    
    return first_tx;
  }

  td::Result<std::vector<NominatorPoolIncome>> parse_nominator_pool_incomes(const schema::Transaction& transaction) {
    std::vector<NominatorPoolIncome> incomes;
    
    // check code hash of pool contract: mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=
    const std::string NOMINATOR_POOL_CODE_HASH = "9A3EC14BC098F6B44064C305222CAEA2800F17DDA85EE6A8198A7095EDE10DCF";
    
    // find first transaction for account in the block because we need stake amount to calculate rewards
    auto first_tx_opt = find_first_tx_for_account(transaction.account);
    if (!first_tx_opt) {
      return td::Status::Error("Failed to find first transaction for account");
    }
    
    auto& first_tx = first_tx_opt.value();
    
    // load account state before first transaction (guaranteed in celldb)
    if (!block_->cell_db_reader_) {
      return td::Status::Error("cell_db_reader not available");
    }
    
    auto account_cell_r = block_->cell_db_reader_->load_cell(first_tx.account_state_hash_before.as_slice());
    if (account_cell_r.is_error()) {
      return td::Status::Error(PSLICE() << "Failed to load account state: " << account_cell_r.error());
    }
    
    auto account_cell = account_cell_r.move_as_ok();
    auto pool_state_r = ParseQuery::parse_account(account_cell, first_tx.now, 
                                                   first_tx.prev_trans_hash, 
                                                   first_tx.prev_trans_lt);
    if (pool_state_r.is_error()) {
      return pool_state_r.move_as_error_prefix("Failed to parse account state: ");
    }
    
    auto pool_state = pool_state_r.move_as_ok();
    
    // check code hash
    if (!pool_state.code_hash.has_value() || 
        pool_state.code_hash.value().to_hex() != NOMINATOR_POOL_CODE_HASH) {
      return td::Status::Error("Not a nominator pool contract");
    }
    
    if (pool_state.data.is_null()) {
      return td::Status::Error("Pool state data is null");
    }
    
    // parse pool data structure
    auto ds = vm::load_cell_slice(pool_state.data);
    if (!ds.is_valid()) {
      return td::Status::Error("Failed to parse pool state");
    }
    ds.advance(8);  // skip state:uint8
    ds.advance(16); // skip nominators_count:uint16
    auto stake_amount_sent = block::tlb::t_Grams.as_integer_skip(ds);  // Coins
    ds.advance(120); // skip validator_amount:Coins

    td::Ref<vm::Cell> config_ref;
    if (!ds.fetch_ref_to(config_ref)) {
      return td::Status::Error("Failed to fetch config ref");
    }
    auto config_cs = vm::load_cell_slice(config_ref);
    config_cs.advance(256); // skip validator address hash
    auto validator_reward_share = config_cs.fetch_ulong(16); // uint16
    
    td::Ref<vm::Cell> nominators_cell_ref;
    if (!ds.fetch_maybe_ref(nominators_cell_ref) || nominators_cell_ref.is_null()) {
      return incomes;  // no nominators
    }
    
    // get transaction value for reward calculation
    if (!transaction.in_msg.has_value()) {
      return incomes;
    }
    auto tx_value = transaction.in_msg.value().value;
    auto tx_value_grams = transaction.in_msg.value().value->grams;
    
    // calculate total reward: tx_value - stake_amount_sent
    td::RefInt256 tx_value_big = tx_value_grams;
    if (td::cmp(tx_value_big, stake_amount_sent) <= 0) {
      return incomes;  // no reward
    }
    td::RefInt256 reward_total = tx_value_big - stake_amount_sent;
    
    // subtract validator's share from total reward
    td::RefInt256 validator_reward = (reward_total * td::make_refint(validator_reward_share)) / td::make_refint(10000);
    td::RefInt256 nominators_reward = reward_total - validator_reward;
    
    // parse nominators dict
    vm::Dictionary nominators_dict{nominators_cell_ref, 256};
    auto iterator = nominators_dict.begin();
    
    // first pass: calculate total balance
    td::RefInt256 total_balance = td::make_refint(td::BigInt256(0));
    std::vector<std::pair<td::BitArray<256>, td::RefInt256>> nominators_list;
    
    while (!iterator.eof()) {
      auto addr_hash = td::BitArray<256>(iterator.cur_pos());
      auto nominator_cs_ref = iterator.cur_value();
      
      // nominator#_ deposit:Coins pending_deposit:Coins
      // create mutable copy of CellSlice for parsing
      vm::CellSlice nominator_cs = *nominator_cs_ref;
      auto deposit = block::tlb::t_Grams.as_integer_skip(nominator_cs);
      
      if (!deposit.is_null() && td::sgn(deposit) > 0) {
        total_balance = total_balance + deposit;
        nominators_list.push_back({addr_hash, deposit});
      }
      
      ++iterator;
    }
    
    if (total_balance.is_null() || td::sgn(total_balance) == 0) {
      return incomes;  // no balance to distribute
    }
    
    // second pass: calculate income for each nominator
    std::string pool_address = convert::to_raw_address(transaction.account);
    
    for (const auto& [addr_hash, balance] : nominators_list) {
      NominatorPoolIncome income;
      income.trace_id = transaction.trace_id;
      income.transaction_hash = transaction.hash;
      income.transaction_lt = transaction.lt;
      income.transaction_now = transaction.now;
      income.mc_seqno = transaction.mc_seqno;
      income.pool_address = pool_address;
      
      // reconstruct nominator address from hash (workchain 0)
      block::StdAddress nominator_addr;
      nominator_addr.workchain = 0;
      nominator_addr.addr = addr_hash;
      income.nominator_address = convert::to_raw_address(nominator_addr);
      
      // calculate proportional income: nominators_reward * balance / total_balance
      auto income_amount = (nominators_reward * balance) / total_balance;
      income.income_amount = income_amount;
      income.nominator_balance = balance;
      
      incomes.push_back(std::move(income));
    }
    
    return incomes;
  }
};

class EventProcessor: public td::actor::Actor {
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
  void process_states(const std::vector<schema::AccountState>& account_states, const MasterchainBlockDataState& blocks_ds, td::Promise<std::vector<BlockchainInterface>> &&promise);
  void process_transactions(const std::vector<schema::Transaction>& transactions, td::Promise<std::vector<BlockchainEvent>> &&promise);
};
