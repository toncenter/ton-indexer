#pragma once
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "smc-interfaces/InterfacesDetector.h"
#include <PostgreSQLInserter.h>


using Detector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR, 
                                    NftItemDetectorR, NftCollectionDetectorR>;

struct ShardStateData {
  AllShardStates shard_states_;
  block::gen::ShardStateUnsplit::Record sstate_;
  std::shared_ptr<block::ConfigInfo> config_;
};

using ShardStateDataPtr = std::shared_ptr<ShardStateData>;

struct Options {
  std::uint32_t seqno_;
  td::actor::ActorId<PostgreSQLInsertManager> insert_manager_;
  std::int32_t batch_size_{5000};
  bool index_interfaces_{false};
  bool from_checkpoint{true};
};

class ShardStateScanner;

class StateBatchParser: public td::actor::Actor {
private:
  std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> data_;
  ShardStateDataPtr shard_state_data_;
  td::actor::ActorId<ShardStateScanner> shard_state_scanner_;
  Options options_;
  
  std::unordered_map<block::StdAddress, std::vector<InsertData>> interfaces_;
  std::vector<InsertData> result_;
public:
  StateBatchParser(std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> data, ShardStateDataPtr shard_state_data, td::actor::ActorId<ShardStateScanner> shard_state_scanner, Options options)
    : data_(std::move(data)), shard_state_data_(std::move(shard_state_data)), shard_state_scanner_(shard_state_scanner), options_(options) {}
  void start_up() override;
  void processing_finished();
private:
  void interfaces_detected(block::StdAddress address, std::vector<typename Detector::DetectedInterface> interfaces, 
                                    td::Bits256 code_hash, td::Bits256 data_hash, uint64_t last_trans_lt, uint32_t last_trans_now, td::Promise<td::Unit> promise);
  void process_account_states(std::vector<schema::AccountState> account_states);
};

class ShardStateScanner: public td::actor::Actor {
private:
  td::Ref<vm::Cell> shard_state_;
  MasterchainBlockDataState mc_block_ds_;

  ShardStateDataPtr shard_state_data_;
  Options options_;

  td::Bits256 cur_addr_{td::Bits256::zero()};
  
  ton::ShardIdFull shard_;
  bool allow_same_{true};
  bool finished_{false};
  uint32_t in_progress_{0};
  uint32_t processed_{0};

  std::unordered_map<std::string, int> no_interface_count_;
  std::unordered_set<std::string> code_hashes_to_skip_;
  std::mutex code_hashes_to_skip_mutex_;
public:
  ShardStateScanner(td::Ref<vm::Cell> shard_state, MasterchainBlockDataState mc_block_ds, Options options);

  void schedule_next();
  void start_up() override;
  void alarm() override;
  void batch_inserted();

  void got_checkpoint(td::Bits256 cur_addr);
};

class SmcScanner: public td::actor::Actor {
private:
  td::actor::ActorId<DbScanner> db_scanner_;
  Options options_;
public:
  SmcScanner(td::actor::ActorId<DbScanner> db_scanner, Options options) :
    db_scanner_(db_scanner), options_(options) {};

  void start_up() override;
  void got_block(MasterchainBlockDataState block);
};
