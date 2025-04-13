#pragma once
#include <map>
#include <variant>
#include "vm/cells/Cell.h"
#include "vm/stack.hpp"
#include "common/refcnt.hpp"
#include "common/checksum.h"
#include "smc-envelope/SmartContract.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "td/actor/actor.h"
#include "td/actor/MultiPromise.h"
#include "td/utils/base64.h"
#include "td/utils/Status.h"
#include "ton/ton-shard.h"

#include "parse_token_data.h"
#include "convert-utils.h"
#include "tokens-tlb.h"
#include "InsertManager.h"
#include "IndexData.h"
#include "DataParser.h"

// classes
enum SmcInterface {
  IT_JETTON_MASTER,
  IT_JETTON_WALLET,
  IT_NFT_COLLECTION,
  IT_NFT_ITEM
};

class FetchAccountFromShard: public td::actor::Actor {
private:
  MasterchainBlockDataState blocks_ds_;
  block::StdAddress address_;
  td::Promise<schema::AccountState> promise_;
public:
  FetchAccountFromShard(MasterchainBlockDataState blocks_ds, block::StdAddress address, td::Promise<schema::AccountState> promise) 
    : blocks_ds_(blocks_ds), address_(address), promise_(std::move(promise)) {
  }

  void start_up() override {
    for (auto& block_ds : blocks_ds_.shard_blocks_) {
      if (ton::shard_contains(block_ds.block_data->block_id().shard_full(), ton::extract_addr_prefix(address_.workchain, address_.addr))) {
        auto root = block_ds.block_state;
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(root, sstate)) {
          promise_.set_error(td::Status::Error("Failed to unpack ShardStateUnsplit"));
          stop();
          return;
        }
        vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};
        
        auto shard_account_csr = accounts_dict.lookup(address_.addr);
        if (shard_account_csr.is_null()) {
          promise_.set_error(td::Status::Error("Account not found in accounts_dict"));
          stop();
          return;
        } 
        
        block::gen::ShardAccount::Record acc_info;
        if(!tlb::csr_unpack(std::move(shard_account_csr), acc_info)) {
          LOG(ERROR) << "Failed to unpack ShardAccount " << address_.addr;
          stop();
          return;
        }
        int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(acc_info.account));
        switch (account_tag) {
        case block::gen::Account::account_none:
          promise_.set_error(td::Status::Error("Account is empty"));
          stop();
          return;
        case block::gen::Account::account: {
          auto account_r = ParseQuery::parse_account(acc_info.account, sstate.gen_utime, acc_info.last_trans_hash, acc_info.last_trans_lt);
          if (account_r.is_error()) {
            promise_.set_error(account_r.move_as_error());
            stop();
            return;
          }
          promise_.set_value(account_r.move_as_ok());
          stop();
          return;
        }
        default:
          promise_.set_error(td::Status::Error("Unknown account tag"));
          stop();
          return;
        }
      }
    }
    promise_.set_error(td::Status::Error("Account not found in shards"));
    stop();
  }
};

class InterfaceManager: public td::actor::Actor {
private:
  std::map<std::pair<vm::CellHash, SmcInterface>, bool> cache_{};
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
public:
  // Interfaces table will consist of 3 columns: code_hash, interface, has_interface
  InterfaceManager(td::actor::ActorId<InsertManagerInterface> insert_manager) : insert_manager_(insert_manager) {
  }

  void check_interface(vm::CellHash code_hash, SmcInterface interface, td::Promise<bool> promise) {
    if (cache_.count({code_hash, interface})) {
      promise.set_value(std::move(cache_[{code_hash, interface}]));
      return;
    }
    promise.set_error(td::Status::Error(ErrorCode::CODE_HASH_NOT_FOUND, "Unknown code hash"));
  }

  void set_interface(vm::CellHash code_hash, SmcInterface interface, bool has) {
    cache_[{code_hash, interface}] = has;
  }
};

template <typename T>
class InterfaceDetector: public td::actor::Actor {
public:
  virtual void detect(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<T> promise) = 0;
  virtual ~InterfaceDetector() = default;
};

template <class T>
class InterfaceStorage {
public:
  InterfaceStorage() {};
  InterfaceStorage(std::unordered_map<std::string, T> cache) : cache_(cache) {};
  std::unordered_map<std::string, T> cache_{};

  void check(block::StdAddress address, td::Promise<T> promise) {
    auto it = cache_.find(convert::to_raw_address(address));
    if (it != cache_.end()) {
      auto res = it->second;
      promise.set_value(std::move(res));
    } else {
      promise.set_error(td::Status::Error(ErrorCode::ENTITY_NOT_FOUND));
    }
  }

  void add(block::StdAddress address, T data, td::Promise<td::Unit> promise) {
    cache_.insert_or_assign(convert::to_raw_address(address), data);
    promise.set_result(td::Unit());
  }
};


/// @brief Detects Jetton Master according to TEP 74
/// Checks that get_jetton_data() returns (int total_supply, int mintable, slice admin_address, cell jetton_content, cell jetton_wallet_code)
class JettonMasterDetector: public InterfaceDetector<JettonMasterData> {
private:
  td::actor::ActorId<InterfaceManager> interface_manager_;
  td::actor::ActorId<InsertManagerInterface> insert_manager_;
  InterfaceStorage<JettonMasterData> storage_;
public:
  JettonMasterDetector(td::actor::ActorId<InterfaceManager> interface_manager, td::actor::ActorId<InsertManagerInterface> insert_manager) 
    : interface_manager_(interface_manager)
    , insert_manager_(insert_manager) {
  }

  JettonMasterDetector(td::actor::ActorId<InterfaceManager> interface_manager, td::actor::ActorId<InsertManagerInterface> insert_manager,
    std::unordered_map<std::string, JettonMasterData> cache)
    : interface_manager_(interface_manager)
    , insert_manager_(insert_manager)
    , storage_(cache) {
  }

  void detect(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonMasterData> promise) override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<bool> code_hash_is_master) mutable {
      if (code_hash_is_master.is_error()) {
        td::actor::send_closure(SelfId, &JettonMasterDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
        return;
      }
      if (!code_hash_is_master.move_as_ok()) {
        promise.set_error(td::Status::Error(ErrorCode::CACHED_CODE_HASH_NO_ENTITY, "Code hash is not a Jetton Master"));
        return;
      }

      td::actor::send_closure(SelfId, &JettonMasterDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    td::actor::send_closure(interface_manager_, &InterfaceManager::check_interface, code_cell->get_hash(), IT_JETTON_MASTER, std::move(P));
  }

  void detect_from_shard(const MasterchainBlockDataState& blocks_ds, block::StdAddress address, td::Promise<JettonMasterData> promise) {
    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), address, blocks_ds, promise = std::move(promise)](td::Result<schema::AccountState> account_state_r) mutable {
      if (account_state_r.is_error()) {
        promise.set_error(account_state_r.move_as_error());
        return;
      }
      auto account_state = account_state_r.move_as_ok();
      if (account_state.account_status != "active") {
        promise.set_error(td::Status::Error("Account is not active"));
        return;
      }
      td::actor::send_closure(SelfId, &JettonMasterDetector::detect_impl, address, account_state.code, account_state.data, account_state.last_trans_lt, account_state.timestamp, blocks_ds, std::move(promise));
    });
    td::actor::create_actor<FetchAccountFromShard>("fetchaccountfromshard", blocks_ds, address, std::move(R)).release();
  }

  void detect_continue(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonMasterData> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<JettonMasterData> cached_res) mutable {
      if (cached_res.is_ok()) {
        auto cached_data = cached_res.move_as_ok();
        if ((data_cell->get_hash() == cached_data.data_hash && code_cell->get_hash() == cached_data.code_hash) 
            || last_tx_lt < cached_data.last_transaction_lt) {
          promise.set_value(std::move(cached_data)); // data did not not changed from cached or is more actual than requested
          return;
        }
      }
      td::actor::send_closure(SelfId, &JettonMasterDetector::detect_impl, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });
    storage_.check(address, std::move(P));
  }

  void detect_impl(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonMasterData> promise) {
    if (code_cell.is_null() || data_cell.is_null()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "Code or data null"));
      return;
    }
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(std::move(address));

    args.set_method_id("get_jetton_data");
    auto res = smc.run_get_method(args);

    const int return_stack_size = 5;
    const vm::StackEntry::Type return_types[return_stack_size] = {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, 
      vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell, vm::StackEntry::Type::t_cell};

    if (!res.success || res.stack->depth() != return_stack_size) {
      td::actor::send_closure(interface_manager_, &InterfaceManager::set_interface, code_cell->get_hash(), IT_JETTON_MASTER, false);
      promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_jetton_data failed"));
      return;
    }

    auto stack = res.stack->as_span();
    
    for (int i = 0; i < return_stack_size; i++) {
      if (stack[i].type() != return_types[i]) {
        promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_jetton_data failed"));
        return;
      }
    }

    JettonMasterData data;
    data.address = convert::to_raw_address(address);
    data.total_supply = stack[0].as_int();
    data.mintable = stack[1].as_int() != 0;
    auto admin_address = convert::to_raw_address(stack[2].as_slice());
    if (admin_address.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_jetton_data address parsing failed"));
      return;
    }
    data.admin_address = admin_address.move_as_ok();
    data.last_transaction_lt = last_tx_lt;
    data.last_transaction_now = last_tx_now;
    data.data_hash = data_cell->get_hash();
    data.code_boc = td::base64_encode(vm::std_boc_serialize(code_cell).move_as_ok());
    data.data_boc = td::base64_encode(vm::std_boc_serialize(data_cell).move_as_ok());
    
    auto jetton_content = parse_token_data(stack[3].as_cell());
    if (jetton_content.is_ok()) {
      data.jetton_content = jetton_content.move_as_ok();
    } else {
      LOG(WARNING) << "Failed to parse jetton content for " << convert::to_raw_address(address) << ": " << jetton_content.error()
                   << " Content: " << convert::to_bytes(stack[3].as_cell()).move_as_ok().value();
    }
    data.jetton_wallet_code_hash = stack[4].as_cell()->get_hash();
    
    auto cache_promise = td::PromiseCreator::lambda([this, code_hash = code_cell->get_hash(), promise = std::move(promise), data](td::Result<td::Unit> r) mutable {
      if (r.is_error()) {
        promise.set_error(r.move_as_error());
        return;
      }
      promise.set_result(std::move(data));
    });
    storage_.add(address, std::move(data), std::move(cache_promise));
  }

  void get_wallet_address(const MasterchainBlockDataState& blocks_ds, block::StdAddress master_address, block::StdAddress owner_address, td::Promise<block::StdAddress> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), blocks_ds, master_address, owner_address, promise = std::move(promise)](td::Result<JettonMasterData> r) mutable {
      if (r.is_error()) {
        auto R = td::PromiseCreator::lambda([SelfId, promise = std::move(promise), blocks_ds, master_address, owner_address](td::Result<JettonMasterData> r) mutable {
          if (r.is_error()) {
            promise.set_error(r.move_as_error());
          } else {
            td::actor::send_closure(SelfId, &JettonMasterDetector::get_wallet_address_impl, r.move_as_ok(), master_address, owner_address, blocks_ds, std::move(promise));
          }
        });
        td::actor::send_closure(SelfId, &JettonMasterDetector::detect_from_shard, blocks_ds, master_address, std::move(R));
        return;
      }
      td::actor::send_closure(SelfId, &JettonMasterDetector::get_wallet_address_impl, r.move_as_ok(), master_address, owner_address, blocks_ds, std::move(promise));
    });
    storage_.check(master_address, std::move(P));
  }

  void get_wallet_address_impl(JettonMasterData data, block::StdAddress master_address, block::StdAddress owner_address, const MasterchainBlockDataState& blocks_ds, td::Promise<block::StdAddress> P) {
    auto code_cell = vm::std_boc_deserialize(td::base64_decode(data.code_boc).move_as_ok()).move_as_ok();
    auto data_cell = vm::std_boc_deserialize(td::base64_decode(data.data_boc).move_as_ok()).move_as_ok();
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;

    vm::CellBuilder anycast_cb;
    anycast_cb.store_bool_bool(false);
    auto anycast_cell = anycast_cb.finalize();
    td::Ref<vm::CellSlice> anycast_cs = vm::load_cell_slice_ref(anycast_cell);

    vm::CellBuilder cb;
    block::gen::t_MsgAddressInt.pack_addr_std(cb, anycast_cs, owner_address.workchain, owner_address.addr);
    auto owner_address_cell = cb.finalize();

    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(master_address);
    args.set_stack({vm::StackEntry(vm::load_cell_slice_ref(owner_address_cell))});
    
    args.set_method_id("get_wallet_address");
    auto res = smc.run_get_method(args);

    if (!res.success || res.stack->depth() != 1) {
      P.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_wallet_address failed"));
      return;
    }

    auto stack = res.stack->as_span();
    if (stack[0].type() != vm::StackEntry::Type::t_slice) {
      P.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_wallet_address failed"));
      return;
    }

    auto wallet_address = convert::to_raw_address(stack[0].as_slice());
    if (wallet_address.is_error()) {
      P.set_error(wallet_address.move_as_error());
      return;
    }
    P.set_result(block::StdAddress::parse(wallet_address.move_as_ok()));
  }
};

/// @brief Detects Jetton Wallet according to TEP 74
/// Checks that get_wallet_data() returns (int balance, slice owner, slice jetton, cell jetton_wallet_code) 
/// and corresponding jetton master recognizes this wallet
class JettonWalletDetector: public InterfaceDetector<JettonWalletData> {
private:
  td::actor::ActorId<JettonMasterDetector> jetton_master_detector_;
  td::actor::ActorId<InterfaceManager> interface_manager_;
  InterfaceStorage<JettonWalletData> storage_;
public:
  JettonWalletDetector(td::actor::ActorId<JettonMasterDetector> jetton_master_detector,
                       td::actor::ActorId<InterfaceManager> interface_manager,
                       td::actor::ActorId<InsertManagerInterface> insert_manager) 
    : jetton_master_detector_(jetton_master_detector)
    , interface_manager_(interface_manager) {
  }
    JettonWalletDetector(td::actor::ActorId<JettonMasterDetector> jetton_master_detector,
                       td::actor::ActorId<InterfaceManager> interface_manager,
                       td::actor::ActorId<InsertManagerInterface> insert_manager,
                       std::unordered_map<std::string, JettonWalletData> cache)
    : jetton_master_detector_(jetton_master_detector)
    , interface_manager_(interface_manager)
    , storage_(cache) {
  }

  void detect(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonWalletData> promise) override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<bool> code_hash_is_wallet) mutable {
      if (code_hash_is_wallet.is_error()) {
        td::actor::send_closure(SelfId, &JettonWalletDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
        return;
      }
      if (!code_hash_is_wallet.move_as_ok()) {
        promise.set_error(td::Status::Error(ErrorCode::CACHED_CODE_HASH_NO_ENTITY, "Code hash is not a Jetton Wallet"));
        return;
      }

      td::actor::send_closure(SelfId, &JettonWalletDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    td::actor::send_closure(interface_manager_, &InterfaceManager::check_interface, code_cell->get_hash(), IT_JETTON_WALLET, std::move(P));
  }

  void detect_continue(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonWalletData> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<JettonWalletData> cached_res) mutable {
      if (cached_res.is_ok()) {
        auto cached_data = cached_res.move_as_ok();
        if ((data_cell->get_hash() == cached_data.data_hash && code_cell->get_hash() == cached_data.code_hash) 
            || last_tx_lt < cached_data.last_transaction_lt) {
          promise.set_value(std::move(cached_data)); // data did not not changed from cached or is more actual than requested
          return;
        }
      }
      td::actor::send_closure(SelfId, &JettonWalletDetector::detect_impl, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    storage_.check(address, std::move(P));
  }

  void detect_impl(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonWalletData> promise) {
    if (code_cell.is_null() || data_cell.is_null()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "Code or data null"));
      return;
    }
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(std::move(address));

    args.set_method_id("get_wallet_data");
    auto res = smc.run_get_method(args);

    const int return_stack_size = 4;
    const vm::StackEntry::Type return_types[return_stack_size] = {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell};

    if (!res.success || res.stack->depth() != return_stack_size) {
      td::actor::send_closure(interface_manager_, &InterfaceManager::set_interface, code_cell->get_hash(), IT_JETTON_WALLET, false);
      promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_wallet_data failed"));
      return;
    }

    auto stack = res.stack->as_span();
    
    for (int i = 0; i < 4; i++) {
      if (stack[i].type() != return_types[i]) {
        promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_wallet_data failed"));
        return;
      }
    }

    JettonWalletData data;
    data.address = convert::to_raw_address(address);
    data.balance = stack[0].as_int();
    auto owner = convert::to_raw_address(stack[1].as_slice());
    if (owner.is_error()) {
      promise.set_error(owner.move_as_error());
      return;
    }
    data.owner = owner.move_as_ok();
    auto jetton = convert::to_raw_address(stack[2].as_slice());
    if (jetton.is_error()) {
      promise.set_error(jetton.move_as_error());
      return;
    }
    data.jetton = jetton.move_as_ok();
    data.last_transaction_lt = last_tx_lt;
    data.last_transaction_now = last_tx_now;
    data.code_hash = code_cell->get_hash();
    data.data_hash = data_cell->get_hash();

    // if (stack[3].as_cell()->get_hash() != code_cell->get_hash()) {
      // LOG(WARNING) << "Jetton Wallet code hash mismatch: " << stack[3].as_cell()->get_hash().to_hex() << " != " << code_cell->get_hash().to_hex();
    // }

    verify_belonging_to_master(std::move(data), blocks_ds, std::move(promise));
  }

  void parse_transfer(schema::Transaction transaction, td::Ref<vm::CellSlice> cs, td::Promise<JettonTransfer> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, transaction, cs = std::move(cs), promise = std::move(promise)](td::Result<JettonWalletData> R) mutable {
      if (R.is_error()) {
        if (R.error().code() == ErrorCode::ENTITY_NOT_FOUND) {
          promise.set_error(R.move_as_error_prefix("Jetton Wallet not found: "));
          return;
        }
        promise.set_error(R.move_as_error());
        return;
      }
      td::actor::send_closure(SelfId, &JettonWalletDetector::parse_transfer_impl, R.move_as_ok(), transaction, std::move(cs), std::move(promise));
    });

    storage_.check(transaction.account, std::move(P));
  }

  void parse_transfer_impl(JettonWalletData contract_data, schema::Transaction transaction, td::Ref<vm::CellSlice> cs, td::Promise<JettonTransfer> promise) {
    tokens::gen::InternalMsgBody::Record_transfer_jetton transfer_record;
    if (!tlb::csr_unpack_inexact(cs, transfer_record)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack transfer"));
      return;
    }

    JettonTransfer transfer;
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
    transfer.amount = block::tlb::t_VarUInteger_16.as_integer(transfer_record.amount);
    if (transfer.amount.is_null()) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack transfer amount"));
      return;
    }
    if (!transaction.in_msg || !transaction.in_msg->source) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack transfer source"));
      return;
    }
    transfer.source = transaction.in_msg->source.value();
    transfer.jetton_wallet = convert::to_raw_address(transaction.account);
    transfer.jetton_master = contract_data.jetton;
    auto destination = convert::to_raw_address(transfer_record.destination);
    if (destination.is_error()) {
      promise.set_error(destination.move_as_error());
      return;
    }
    transfer.destination = destination.move_as_ok();
    auto response_destination = convert::to_raw_address(transfer_record.response_destination);
    if (response_destination.is_error()) {
      promise.set_error(response_destination.move_as_error());
      return;
    }
    transfer.response_destination = response_destination.move_as_ok();
    if (!transfer_record.custom_payload.write().fetch_maybe_ref(transfer.custom_payload)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to fetch custom payload"));
      return;
    }
    transfer.forward_ton_amount = block::tlb::t_VarUInteger_16.as_integer(transfer_record.forward_ton_amount);
    if (!transfer_record.forward_payload.write().fetch_maybe_ref(transfer.forward_payload)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to fetch forward payload"));
      return;
    }

    promise.set_value(std::move(transfer));
  }

  void parse_burn(schema::Transaction transaction, td::Ref<vm::CellSlice> cs, td::Promise<JettonBurn> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, transaction, cs = std::move(cs), promise = std::move(promise)](td::Result<JettonWalletData> R) mutable {
      if (R.is_error()) {
        if (R.error().code() == ErrorCode::ENTITY_NOT_FOUND) {
          promise.set_error(R.move_as_error_prefix("Jetton Wallet not found: "));
          return;
        }
        promise.set_error(R.move_as_error());
      }
      td::actor::send_closure(SelfId, &JettonWalletDetector::parse_burn_impl, R.move_as_ok(), transaction, std::move(cs), std::move(promise));
    });

    storage_.check(transaction.account, std::move(P));
  }

  void parse_burn_impl(JettonWalletData contract_data, schema::Transaction transaction, td::Ref<vm::CellSlice> cs, td::Promise<JettonBurn> promise) {
    tokens::gen::InternalMsgBody::Record_burn burn_record;
    if (!tlb::csr_unpack_inexact(cs, burn_record)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack burn"));
      return;
    }

    JettonBurn burn;
    burn.trace_id = transaction.trace_id;
    burn.transaction_hash = transaction.hash;
    burn.transaction_lt = transaction.lt;
    burn.transaction_now = transaction.now;
    if (auto* v = std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
      burn.transaction_aborted = v->aborted;
    } else {
      burn.transaction_aborted = 0;
    }
    burn.mc_seqno = transaction.mc_seqno;

    burn.query_id = burn_record.query_id;
    if (!transaction.in_msg || !transaction.in_msg->source) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack burn source"));
      return;
    }
    burn.owner = transaction.in_msg->source.value();
    burn.jetton_wallet = convert::to_raw_address(transaction.account);
    burn.jetton_master = contract_data.jetton;
    burn.amount = block::tlb::t_VarUInteger_16.as_integer(burn_record.amount);
    if (burn.amount.is_null()) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack burn amount"));
      return;
    }
    auto response_destination = convert::to_raw_address(burn_record.response_destination);
    if (response_destination.is_error()) {
      promise.set_error(response_destination.move_as_error());
      return;
    }
    burn.response_destination = response_destination.move_as_ok();
    if (!burn_record.custom_payload.write().fetch_maybe_ref(burn.custom_payload)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to fetch custom payload"));
      return;
    }

    promise.set_value(std::move(burn));
  }

private:
  // checks belonging of address to Jetton Master by calling get_wallet_address
  void verify_belonging_to_master(JettonWalletData data, const MasterchainBlockDataState& blocks_ds, td::Promise<JettonWalletData> &&promise) {
    auto master_addr = block::StdAddress::parse(data.jetton);
    if (master_addr.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, PSLICE() << "Failed to parse jetton master address (" << data.jetton << "): "));
      return;
    }
    auto owner_addr = block::StdAddress::parse(data.owner);
    if (owner_addr.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, PSLICE() << "Failed to parse jetton owner address (" << data.owner << "): "));
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, data, promise = std::move(promise)](td::Result<block::StdAddress> R) mutable {
      if (R.is_error()) {
        LOG(WARNING) << "Jetton Master is not available, so we can't verify address " << data.address << ": " << R.error();
        promise.set_error(td::Status::Error(ErrorCode::ADDITIONAL_CHECKS_FAILED, R.error().message()));
      } else {
        auto address = R.move_as_ok();
        if (convert::to_raw_address(address) != data.address) {
          LOG(WARNING) << "Jetton Master returned wrong address: " << convert::to_raw_address(address) << " expected: " << data.address;
          promise.set_error(td::Status::Error(ErrorCode::ADDITIONAL_CHECKS_FAILED, "Couldn't verify Jetton Wallet. Possibly scam."));
        } else {
          auto cache_promise = td::PromiseCreator::lambda([this, promise = std::move(promise), data](td::Result<td::Unit> r) mutable {
            if (r.is_error()) {
              promise.set_error(r.move_as_error());
              return;
            }
            promise.set_result(std::move(data));
          });
          td::actor::send_closure(SelfId, &JettonWalletDetector::add_to_cache, address, std::move(data), std::move(cache_promise));
        }
      }
    });

    td::actor::send_closure(jetton_master_detector_, &JettonMasterDetector::get_wallet_address, blocks_ds, master_addr.move_as_ok(), owner_addr.move_as_ok(), std::move(P));
  }

  void add_to_cache(block::StdAddress address, JettonWalletData data, td::Promise<td::Unit> promise) {
    storage_.add(address, data, std::move(promise));
  }
};


/// @brief Detects NFT Collection according to your specific standard
/// Checks that get_collection_data() returns (int next_item_index, cell collection_content, slice owner_address)
class NFTCollectionDetector: public InterfaceDetector<NFTCollectionData> {
private:
  td::actor::ActorId<InterfaceManager> interface_manager_;
  InterfaceStorage<NFTCollectionData> storage_;
public:
  NFTCollectionDetector(td::actor::ActorId<InterfaceManager> interface_manager, td::actor::ActorId<InsertManagerInterface> insert_manager) 
    : interface_manager_(interface_manager) {
  }

  void get_from_cache_or_shard(block::StdAddress address, const MasterchainBlockDataState& blocks_ds, td::Promise<NFTCollectionData> promise) {
    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), address, blocks_ds, promise = std::move(promise)](td::Result<NFTCollectionData> r) mutable {
      if (r.is_error()) {
        td::actor::send_closure(SelfId, &NFTCollectionDetector::detect_from_shard, blocks_ds, address, std::move(promise));
      } else {
        promise.set_value(r.move_as_ok());
      }
    });
    storage_.check(address, std::move(R));
  }

  void detect(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<NFTCollectionData> promise) override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<bool> code_hash_is_collection) mutable {
      if (code_hash_is_collection.is_error()) {
        td::actor::send_closure(SelfId, &NFTCollectionDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
        return;
      }
      if (!code_hash_is_collection.move_as_ok()) {
        promise.set_error(td::Status::Error(ErrorCode::CACHED_CODE_HASH_NO_ENTITY, "Code hash is not a NFT Collection"));
        return;
      }

      td::actor::send_closure(SelfId, &NFTCollectionDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    td::actor::send_closure(interface_manager_, &InterfaceManager::check_interface, code_cell->get_hash(), IT_NFT_COLLECTION, std::move(P));
  }
private:
  void detect_from_shard(const MasterchainBlockDataState& blocks_ds, block::StdAddress address, td::Promise<NFTCollectionData> promise) {
    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), address, blocks_ds, promise = std::move(promise)](td::Result<schema::AccountState> account_state_r) mutable {
      if (account_state_r.is_error()) {
        promise.set_error(account_state_r.move_as_error());
        return;
      }
      auto account_state = account_state_r.move_as_ok();
      td::actor::send_closure(SelfId, &NFTCollectionDetector::detect_impl, address, account_state.code, account_state.data, account_state.last_trans_lt, account_state.timestamp, blocks_ds, std::move(promise));
    });
    td::actor::create_actor<FetchAccountFromShard>("fetchaccountfromshard", blocks_ds, address, std::move(R)).release();
  }

  void detect_continue(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<NFTCollectionData> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<NFTCollectionData> cached_res) mutable {
      if (cached_res.is_ok()) {
        auto cached_data = cached_res.move_as_ok();
        if ((data_cell->get_hash() == cached_data.data_hash && code_cell->get_hash() == cached_data.code_hash) 
            || last_tx_lt < cached_data.last_transaction_lt) {
          promise.set_value(std::move(cached_data)); // data did not not changed from cached or is more actual than requested
          return;
        }
      }
      td::actor::send_closure(SelfId, &NFTCollectionDetector::detect_impl, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    storage_.check(address, std::move(P));
  }

  void detect_impl(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<NFTCollectionData> promise) {
    if (code_cell.is_null() || data_cell.is_null()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "Code or data null"));
      return;
    }
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(std::move(address));

    args.set_method_id("get_collection_data");
    auto res = smc.run_get_method(args);

    const int return_stack_size = 3;
    const vm::StackEntry::Type return_types[return_stack_size] = {vm::StackEntry::Type::t_int, 
      vm::StackEntry::Type::t_cell, vm::StackEntry::Type::t_slice};

    if (!res.success || res.stack->depth() != return_stack_size) {
      td::actor::send_closure(interface_manager_, &InterfaceManager::set_interface, code_cell->get_hash(), IT_NFT_COLLECTION, false);
      promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_collection_data failed"));
      return;
    }

    auto stack = res.stack->as_span();
    
    for (int i = 0; i < return_stack_size; i++) {
      if (stack[i].type() != return_types[i]) {
        promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_collection_data failed"));
        return;
      }
    }

    NFTCollectionData data;
    data.address = convert::to_raw_address(address);
    data.next_item_index = stack[0].as_int();
    
    auto owner_address = convert::to_raw_address(stack[2].as_slice());
    if (owner_address.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "get_collection_data address parsing failed"));
      return;
    }
    data.owner_address = owner_address.move_as_ok();
    data.last_transaction_lt = last_tx_lt;
    data.last_transaction_now = last_tx_now;
    data.data_hash = data_cell->get_hash();
    data.code_boc = td::base64_encode(vm::std_boc_serialize(code_cell).move_as_ok());
    data.data_boc = td::base64_encode(vm::std_boc_serialize(data_cell).move_as_ok());

    auto collection_content = parse_token_data(stack[1].as_cell());
    if (collection_content.is_ok()) {
      data.collection_content = collection_content.move_as_ok();
    } else {
      LOG(WARNING) << "Failed to parse collection content for " << convert::to_raw_address(address) << ": " << collection_content.error()
                   << " Content: " << convert::to_bytes(stack[1].as_cell()).move_as_ok().value();
    }
    
    auto cache_promise = td::PromiseCreator::lambda([this, promise = std::move(promise), data](td::Result<td::Unit> r) mutable {
      if (r.is_error()) {
        promise.set_error(r.move_as_error());
        return;
      }
      promise.set_result(std::move(data));
    });
    storage_.add(address, std::move(data), std::move(cache_promise));
  }
};


/// @brief Detects NFT Item according to your specific standard
/// Checks that get_nft_data() returns (int init?, int index, slice collection_address, slice owner_address, cell individual_content)
class NFTItemDetector: public InterfaceDetector<NFTItemData> {
private:
  td::actor::ActorId<InterfaceManager> interface_manager_;
  td::actor::ActorId<NFTCollectionDetector> collection_detector_;
  InterfaceStorage<NFTItemData> storage_;
public:
  NFTItemDetector(td::actor::ActorId<InterfaceManager> interface_manager, td::actor::ActorId<InsertManagerInterface> insert_manager, td::actor::ActorId<NFTCollectionDetector> collection_detector) 
    : interface_manager_(interface_manager)
    , collection_detector_(collection_detector) {
  }

  void detect(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<NFTItemData> promise) override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<bool> code_hash_is_collection) mutable {
      if (code_hash_is_collection.is_error()) {
        td::actor::send_closure(SelfId, &NFTItemDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
        return;
      }
      if (!code_hash_is_collection.move_as_ok()) {
        promise.set_error(td::Status::Error(ErrorCode::CACHED_CODE_HASH_NO_ENTITY, "Code hash is not a NFT Item"));
        return;
      }

      td::actor::send_closure(SelfId, &NFTItemDetector::detect_continue, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    td::actor::send_closure(interface_manager_, &InterfaceManager::check_interface, code_cell->get_hash(), IT_NFT_ITEM, std::move(P));
  }

  void parse_transfer(schema::Transaction transaction, td::Ref<vm::CellSlice> cs, td::Promise<NFTTransfer> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, transaction, cs = std::move(cs), promise = std::move(promise)](td::Result<NFTItemData> R) mutable {
      if (R.is_error()) {
        if (R.error().code() == ErrorCode::ENTITY_NOT_FOUND) {
          promise.set_error(R.move_as_error_prefix("NFT item not found: "));
          return;
        }
        promise.set_error(R.move_as_error());
        return;
      }
      td::actor::send_closure(SelfId, &NFTItemDetector::parse_transfer_impl, R.move_as_ok(), transaction, std::move(cs), std::move(promise));
    });

    storage_.check(transaction.account, std::move(P));
  }

  void parse_transfer_impl(NFTItemData contract_data, schema::Transaction transaction, td::Ref<vm::CellSlice> cs, td::Promise<NFTTransfer> promise) {
    tokens::gen::InternalMsgBody::Record_transfer_nft transfer_record;
    if (!tlb::csr_unpack_inexact(cs, transfer_record)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to unpack transfer"));
      return;
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
    transfer.nft_item_index = contract_data.index;
    transfer.nft_collection = contract_data.collection_address;
    if (!transaction.in_msg.has_value() || !transaction.in_msg.value().source) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to fetch NFT old owner address"));
      return;
    }
    transfer.old_owner = transaction.in_msg.value().source.value();
    auto new_owner = convert::to_raw_address(transfer_record.new_owner);
    if (new_owner.is_error()) {
      promise.set_error(new_owner.move_as_error());
      return;
    }
    transfer.new_owner = new_owner.move_as_ok();
    auto response_destination = convert::to_raw_address(transfer_record.response_destination);
    if (response_destination.is_error()) {
      promise.set_error(response_destination.move_as_error());
      return;
    }
    transfer.response_destination = response_destination.move_as_ok();
    if (!transfer_record.custom_payload.write().fetch_maybe_ref(transfer.custom_payload)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to fetch custom payload"));
      return;
    }
    transfer.forward_amount = block::tlb::t_VarUInteger_16.as_integer(transfer_record.forward_amount);
    if (!transfer_record.forward_payload.write().fetch_maybe_ref(transfer.forward_payload)) {
      promise.set_error(td::Status::Error(ErrorCode::EVENT_PARSING_ERROR, "Failed to fetch forward payload"));
      return;
    }

    promise.set_value(std::move(transfer));
  }

private:
  void detect_continue(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now, const MasterchainBlockDataState& blocks_ds, td::Promise<NFTItemData> promise) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), this, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, promise = std::move(promise)](td::Result<NFTItemData> cached_res) mutable {
      if (cached_res.is_ok()) {
        auto cached_data = cached_res.move_as_ok();
        if ((data_cell->get_hash() == cached_data.data_hash && code_cell->get_hash() == cached_data.code_hash) 
            || last_tx_lt < cached_data.last_transaction_lt) {
          promise.set_value(std::move(cached_data)); // data did not not changed from cached or is more actual than requested
          return;
        }
      }
      td::actor::send_closure(SelfId, &NFTItemDetector::detect_impl, address, code_cell, data_cell, last_tx_lt, last_tx_now, blocks_ds, std::move(promise));
    });

    storage_.check(address, std::move(P));
  }

  void detect_impl(block::StdAddress address, td::Ref<vm::Cell> code_cell, td::Ref<vm::Cell> data_cell, uint64_t last_tx_lt, uint32_t last_tx_now,  const MasterchainBlockDataState& blocks_ds, td::Promise<NFTItemData> promise) {
    if (code_cell.is_null() || data_cell.is_null()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "Code or data null"));
      return;
    }
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(std::move(address));

    args.set_method_id("get_nft_data");
    auto res = smc.run_get_method(args);

    const int return_stack_size = 5;
    const vm::StackEntry::Type return_types[return_stack_size] = {vm::StackEntry::Type::t_int, 
      vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_cell};

    if (!res.success || res.stack->depth() != return_stack_size) {
      td::actor::send_closure(interface_manager_, &InterfaceManager::set_interface, code_cell->get_hash(), IT_NFT_ITEM, false);
      promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_nft_data failed"));
      return;
    }

    auto stack = res.stack->as_span();

    for (int i = 0; i < return_stack_size; i++) {
      if (stack[i].type() != return_types[i]) {
        promise.set_error(td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_nft_data failed"));
        return;
      }
    }

    NFTItemData data;
    data.address = convert::to_raw_address(address);
    data.init = stack[0].as_int()->to_long() != 0;
    data.index = stack[1].as_int();
    
    auto collection_address = convert::to_raw_address(stack[2].as_slice());
    if (collection_address.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "get_nft_data collection address parsing failed"));  
      return;
    }
    data.collection_address = collection_address.move_as_ok();

    auto owner_address = convert::to_raw_address(stack[3].as_slice());
    if (owner_address.is_error()) {
      promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "get_nft_data address parsing failed"));
      return;
    }
    data.owner_address = owner_address.move_as_ok();
    data.last_transaction_lt = last_tx_lt;
    data.last_transaction_now = last_tx_now;
    data.code_hash = code_cell->get_hash();
    data.data_hash = data_cell->get_hash();
    
    if (data.collection_address == "addr_none") {
      auto content = parse_token_data(stack[4].as_cell());
      if (content.is_ok()) {
        data.content = content.move_as_ok();
      } else {
        LOG(WARNING) << "Failed to parse content for " << convert::to_raw_address(address) << ": " << content.error()
                     << " Content: " << convert::to_bytes(stack[4].as_cell()).move_as_ok().value();
      }
      auto cache_promise = td::PromiseCreator::lambda([promise = std::move(promise), data](td::Result<td::Unit> r) mutable {
        if (r.is_error()) {
          promise.set_error(r.move_as_error());
          return;
        }
        promise.set_result(std::move(data));
      });
      storage_.add(address, std::move(data), std::move(cache_promise));
    } else {
      auto ind_content = stack[4].as_cell();
      auto collection_address = block::StdAddress::parse(data.collection_address);
      if (collection_address.is_error()) {
        LOG(WARNING) << "Failed to parse collection address for " << convert::to_raw_address(address) << ": " << collection_address.error();
        promise.set_error(td::Status::Error(ErrorCode::DATA_PARSING_ERROR, PSLICE() << "Failed to parse collection address for " << convert::to_raw_address(address) << ": " << collection_address.error()));
        return;
      }
      td::actor::send_closure(collection_detector_, &NFTCollectionDetector::get_from_cache_or_shard, collection_address.move_as_ok(), blocks_ds,
                              td::PromiseCreator::lambda([this, SelfId = actor_id(this), ind_content, address, data, code_cell, data_cell, last_tx_lt, blocks_ds, promise = std::move(promise)](td::Result<NFTCollectionData> collection_res) mutable {
        if (collection_res.is_error()) {
          LOG(WARNING) << "Failed to get collection for " << convert::to_raw_address(address) << ": " << collection_res.error();
          promise.set_error(collection_res.move_as_error_prefix("Failed to get collection for " + convert::to_raw_address(address) + ": "));
          return;
        }

        auto collection_data = collection_res.move_as_ok();
        auto content = get_content(data.index, ind_content, collection_data, code_cell, data_cell, blocks_ds);
        if (content.is_error()) {
          LOG(WARNING) << "Failed to parse content for " << convert::to_raw_address(address) << ": " << content.error() 
                       << " Content: " << convert::to_bytes(ind_content).move_as_ok().value();
        } else {
          data.content = content.move_as_ok();
        }

        auto verify_r = verify_belonging_to_collection(data, collection_data, blocks_ds);
        if (verify_r.is_error()) {
          LOG(WARNING) << "Failed to verify belonging to collection for " << convert::to_raw_address(address) << ": " << verify_r.error();
          promise.set_error(verify_r.move_as_error_prefix(PSLICE() << "Failed to verify belonging to collection for " << convert::to_raw_address(address) << ": " << verify_r.error()));
          return;
        }

        auto cache_promise = td::PromiseCreator::lambda([promise = std::move(promise), data](td::Result<td::Unit> r) mutable {
          if (r.is_error()) {
            promise.set_error(r.move_as_error());
            return;
          }
          promise.set_result(std::move(data));
        });

        td::actor::send_closure(SelfId, &NFTItemDetector::add_to_cache, address, std::move(data), std::move(cache_promise));
      }));
    }
  }

  void add_to_cache(block::StdAddress address, NFTItemData data, td::Promise<td::Unit> promise) {
    storage_.add(address, data, std::move(promise));
  }

  td::Status verify_belonging_to_collection(const NFTItemData& item_data, const NFTCollectionData& collection_data, const MasterchainBlockDataState& blocks_ds) {
    auto code_cell = vm::std_boc_deserialize(td::base64_decode(collection_data.code_boc).move_as_ok()).move_as_ok();
    auto data_cell = vm::std_boc_deserialize(td::base64_decode(collection_data.data_boc).move_as_ok()).move_as_ok();
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(block::StdAddress::parse(collection_data.address).move_as_ok());
    args.set_stack({vm::StackEntry(item_data.index)});
    args.set_method_id("get_nft_address_by_index");
    auto res = smc.run_get_method(args);

    if (!res.success) {
      return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_nft_address_by_index failed");
    }

    auto stack = res.stack->as_span();
    if (stack.size() != 1 || stack[0].type() != vm::StackEntry::Type::t_slice) {
      return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_nft_address_by_index failed");
    }
    auto nft_address = convert::to_raw_address(stack[0].as_slice());
    if (nft_address.is_error()) {
      return td::Status::Error(ErrorCode::DATA_PARSING_ERROR, "get_nft_address_by_index parse address failed");
    }

    return nft_address.move_as_ok() == item_data.address ? td::Status::OK() : td::Status::Error(ErrorCode::ADDITIONAL_CHECKS_FAILED, "NFT Item doesn't belong to the referred collection");
  }

  td::Result<std::map<std::string, std::string>> get_content(const td::RefInt256 index, td::Ref<vm::Cell> ind_content, const NFTCollectionData& collection_data, 
                                        td::Ref<vm::Cell> item_code, td::Ref<vm::Cell> item_data, const MasterchainBlockDataState& blocks_ds) {
    auto code_cell = vm::std_boc_deserialize(td::base64_decode(collection_data.code_boc).move_as_ok()).move_as_ok();
    auto data_cell = vm::std_boc_deserialize(td::base64_decode(collection_data.data_boc).move_as_ok()).move_as_ok();
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(block::StdAddress::parse(collection_data.address).move_as_ok());
    args.set_stack({vm::StackEntry(index), vm::StackEntry(ind_content)});
    args.set_method_id("get_nft_content");
    auto res = smc.run_get_method(args);

    if (!res.success) {
      return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_nft_content failed");
    }

    auto stack = res.stack->as_span();
    if (stack.size() != 1 || stack[0].type() != vm::StackEntry::Type::t_cell) {
      return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_nft_content failed");
    }

    const std::string ton_dns_root_addr = "0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF";

    if (collection_data.address == ton_dns_root_addr) {
      std::map<std::string, std::string> result;
      TRY_RESULT_ASSIGN(result["domain"], get_domain(item_code, item_data, blocks_ds));
      return result;
    } else {
      return parse_token_data(stack[0].as_cell());
    }
  }

  td::Result<std::string> get_domain(td::Ref<vm::Cell> code, td::Ref<vm::Cell> data, const MasterchainBlockDataState& blocks_ds) {
    ton::SmartContract smc({code, data});
    ton::SmartContract::Args args;
    args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    args.set_config(blocks_ds.config_);
    args.set_method_id("get_domain");
    auto res = smc.run_get_method(args);
    if (!res.success) {
      return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_domain failed");
    }
    auto stack = res.stack->as_span();
    if (stack.size() != 1 || stack[0].type() != vm::StackEntry::Type::t_slice) {
      return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_domain failed");
    }
    auto cs = stack[0].as_slice();

    if (cs.not_null()) {
      auto size = cs->size();
      if (size % 8 == 0) {
        auto cnt = size / 8;
        unsigned char tmp[1024];
        cs.write().fetch_bytes(tmp, cnt);
        std::string s{tmp, tmp + cnt};
        
        return s + ".ton";
      }
    }
    return td::Status::Error(ErrorCode::GET_METHOD_WRONG_RESULT, "get_domain returned unexpected result");
  }
};
