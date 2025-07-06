#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"
#include "SmcScanner.h"

using ShardStateDataPtr = std::shared_ptr<ShardStateData>;

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
  void batch_inserted();

  std::string get_checkpoint_file_path();
};