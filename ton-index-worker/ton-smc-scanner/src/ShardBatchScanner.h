#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"
#include "SmcScanner.h"
#include <set>

using ShardStateDataPtr = std::shared_ptr<ShardStateData>;

class ShardStateScanner: public td::actor::Actor {
private:
  td::Ref<vm::Cell> shard_state_;
  schema::MasterchainBlockDataState mc_block_ds_;

  Options options_;
  ShardStateDataPtr shard_state_data_;

  std::unique_ptr<vm::AugmentedDictionary> accounts_dict_{nullptr};
  vm::DictIterator iterator_;

  td::Bits256 next_batch_start_;
  
  ton::ShardIdFull shard_;
  bool allow_same_{true};
  bool finished_{false};
  uint32_t in_progress_{0};
  uint32_t accounts_cnt_{0};

  std::unordered_map<std::string, int> no_interface_count_;
  std::unordered_set<std::string> code_hashes_to_skip_;
  std::mutex code_hashes_to_skip_mutex_;

  // Parallel processing members
  uint32_t max_parallel_batches_{4};
public:
  ShardStateScanner(td::Ref<vm::Cell> shard_state, schema::MasterchainBlockDataState mc_block_ds, Options options);

  void schedule_next();
  void start_up() override;
  void batch_parsed(td::Bits256 last_addr, std::vector<InsertData> results);
  void batch_inserted(td::Bits256 last_addr);

  std::string get_checkpoint_file_path();
  td::Bits256 load_from_checkpoint();
  
  // New parallel processing methods
  void update_checkpoint(td::Bits256 new_checkpoint);
};
