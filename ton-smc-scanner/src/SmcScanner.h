#pragma once
#include <td/actor/actor.h>
#include "DbScanner.h"
#include "smc-interfaces/InterfacesDetector.h"
#include <PostgresInserter.h>


class ShardStateScanner: public td::actor::Actor {
private:
  td::Ref<vm::Cell> shard_state_;
  MasterchainBlockDataState mc_block_ds_;

  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;

  td::Bits256 cur_addr_ = td::Bits256::zero();

  std::atomic_uint32_t in_progress_{0};

  std::unordered_map<std::string, int> no_interface_count_;
  std::unordered_set<std::string> code_hashes_to_skip_;
  std::mutex code_hashes_to_skip_mutex_;
public:
  ShardStateScanner(td::Ref<vm::Cell> shard_state, MasterchainBlockDataState mc_block_ds) : shard_state_(shard_state), mc_block_ds_(mc_block_ds) {
    // cur_addr_.from_hex("012508807D259B1F3BDD2A830CF7F4591838E0A1D1474A476B20CFB540CD465B");

    // cur_addr_.from_hex("E750CF93EAEDD2EC01B5DE8F49A334622BD630A8728806ABA65F1443EB7C8FD7");

    for (const auto &shard_ds : mc_block_ds_.shard_blocks_) {
      shard_states_.push_back(shard_ds.block_state);
    }
    auto config_r = block::ConfigInfo::extract_config(mc_block_ds_.shard_blocks_[0].block_state, block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries);
    if (config_r.is_error()) {
      LOG(ERROR) << "Failed to extract config: " << config_r.move_as_error();
      std::_Exit(2);
      return;
    }
    config_ = config_r.move_as_ok();
  };

  void schedule_next() {
    block::gen::ShardStateUnsplit::Record sstate;
    if (!tlb::unpack_cell(shard_state_, sstate)) {
      LOG(ERROR) << "Failed to unpack ShardStateUnsplit";
      stop();
      return;
    }

    vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};

    bool eof = false;
    bool allow_same = true;

    while (!eof && in_progress_ < 100) {
      auto shard_account_csr = accounts_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr_.bits(), 256, true, allow_same);
      if (shard_account_csr.is_null()) {
        eof = true;
        break;
      }
      allow_same = false;

      // if (cur_addr_.to_hex() != "E753CF93EAEDD2EC01B5DE8F49A334622BD630A8728806ABA65F1443EB7C8FD7") {
      //   continue;
      // }

      td::Ref<vm::Cell> account_root = shard_account_csr->prefetch_ref();

      int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(account_root));
      switch (account_tag) {
      case block::gen::Account::account_none:
        break;
      case block::gen::Account::account: {
        auto account_r = ParseQuery::parse_account(account_root, sstate.gen_utime);
        if (account_r.is_error()) {
          LOG(ERROR) << "Failed to parse account " << cur_addr_.to_hex() << ": " << account_r.move_as_error();
          break;
        }
        process_account_state(account_r.move_as_ok());
        break;
      }
      default:
        LOG(ERROR) << "Unknown account tag";
        break;
      }
    }

    alarm_timestamp() = td::Timestamp::in(1);
  }

  void start_up() override {
    schedule_next();
  }

  void alarm() override {
    schedule_next();
  }

  void process_account_state(schema::AccountState account) {
    // LOG(INFO) << "Processing account state " << account.account;

    if (!account.code_hash || no_interface_count_[account.code_hash.value()] > 100) {
      // LOG(INFO) << "Skipping account " << account.account;
      return;
    }
    in_progress_.fetch_add(1);
    
    td::actor::create_actor<InterfacesDetector>("InterfacesDetector", account.account, account.code, account.data, shard_states_, config_, 
                td::PromiseCreator::lambda([this, SelfId = actor_id(this), account](td::Result<std::vector<SmcInterfaceR>> R){
      if (R.is_error()) {
        LOG(ERROR) << "Failed to detect interfaces: " << R.move_as_error();
        return;
      }
      auto code_hash = account.code_hash.value();
      auto ifaces = R.move_as_ok();
      if (ifaces.empty()) {
        std::lock_guard<std::mutex> guard(code_hashes_to_skip_mutex_);
        int& count = no_interface_count_[code_hash];
        count++;
      } else {
        // Reset the count if interfaces are found
        std::lock_guard<std::mutex> guard(code_hashes_to_skip_mutex_);
        no_interface_count_[code_hash] = 0;
      }

      std::vector<PgInsertData> to_insert;
      for (auto& iface : ifaces) {
        std::visit([&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, JettonMasterDetectorR::Result>) {
            LOG(INFO) << "Detected JettonMasterDetector " << arg.address;
            LOG(INFO) << "In progress: " << in_progress_;
            PgJettonMasterData data;
            data.address = convert::to_raw_address(arg.address);
            data.total_supply = arg.total_supply->to_dec_string();
            data.mintable = arg.mintable;
            if (arg.admin_address) {
              data.admin_address = convert::to_raw_address(*arg.admin_address);
            }
            data.jetton_content = arg.jetton_content;
            data.jetton_wallet_code_hash = arg.jetton_wallet_code_hash;
            data.data_hash = account.data->get_hash();
            data.code_hash = account.code->get_hash();
            data.last_transaction_lt = account.last_trans_lt;
            data.code_boc = td::base64_encode(vm::std_boc_serialize(account.code).move_as_ok());
            data.data_boc = td::base64_encode(vm::std_boc_serialize(account.data).move_as_ok());
            to_insert.push_back(std::move(data));
          } else if constexpr (std::is_same_v<T, JettonWalletDetectorR::Result>) {
            LOG(INFO) << "Detected JettonWalletDetector " << arg.address;
            PgJettonWalletData data;
            data.address = convert::to_raw_address(arg.address);
            data.balance = arg.balance->to_dec_string();
            data.owner = convert::to_raw_address(arg.owner);
            data.jetton = convert::to_raw_address(arg.jetton);
            data.last_transaction_lt = account.last_trans_lt;
            data.code_hash = account.code_hash.value();
            data.data_hash = account.data_hash.value();
            to_insert.push_back(std::move(data));
          } else if constexpr (std::is_same_v<T, NftItemDetectorR::Result>) {
            LOG(INFO) << "Detected NftItemDetector " << arg.address;
          } else if constexpr (std::is_same_v<T, NftCollectionDetectorR::Result>) {
            LOG(INFO) << "Detected NftCollectionDetector " << arg.address;
          } else {
            LOG(ERROR) << "Unknown interface";
          }
        }, iface);
      }

      if (to_insert.size()) {
        td::MultiPromise mp;
        auto ig = mp.init_guard();
        ig.add_promise(td::PromiseCreator::lambda([this, to_insert](td::Result<td::Unit> R){
          if (R.is_error()) {
            LOG(ERROR) << "Failed to insert to PG: " << R.move_as_error();
            retry_insert(to_insert);
          } else {
            in_progress_.fetch_sub(1);
          }
        }));
        td::actor::create_actor<PostgresInserter>("PostgresInserter03", PgCredential{"127.0.0.1", 5432, "postgres", "", "ton_index"}, to_insert, ig.get_promise()).release();
      } else {
        in_progress_.fetch_sub(1);
      }

    })).release();
  }

  void retry_insert(std::vector<PgInsertData> to_insert) {
    td::MultiPromise mp2;
    auto ig2 = mp2.init_guard();
    ig2.add_promise(td::PromiseCreator::lambda([&](td::Result<td::Unit> R){
      if (R.is_ok()) {
        LOG(ERROR) << "Retry insert success";
      } else {
        LOG(ERROR) << "Retry insert also failed";
      }
      in_progress_.fetch_sub(1);
    }));
    td::actor::create_actor<PostgresInserter>("PostgresInserter03", PgCredential{"127.0.0.1", 5432, "postgres", "", "ton_index"}, to_insert, ig2.get_promise()).release();
  }
};


class SmcScanner: public td::actor::Actor {
private:
  td::actor::ActorId<DbScanner> db_scanner_;
  uint32_t seqno_;
public:
  SmcScanner(td::actor::ActorId<DbScanner> db_scanner, uint32_t seqno) :
    db_scanner_(db_scanner), seqno_(seqno) {};

  void start_up() override {
    auto P = td::PromiseCreator::lambda([=, SelfId = actor_id(this)](td::Result<MasterchainBlockDataState> R){
      if (R.is_error()) {
        LOG(ERROR) << "Failed to get seqno " << seqno_ << ": " << R.move_as_error();
        stop();
        return;
      }
      td::actor::send_closure(SelfId, &SmcScanner::got_block, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, seqno_, std::move(P));
  }

  void got_block(MasterchainBlockDataState block) {
    LOG(INFO) << "Got block data state";
    for (const auto &shard_ds : block.shard_blocks_) {
      auto& shard_state = shard_ds.block_state;
      td::actor::create_actor<ShardStateScanner>("ShardStateScanner", shard_state, block).release();
    }
  }
};