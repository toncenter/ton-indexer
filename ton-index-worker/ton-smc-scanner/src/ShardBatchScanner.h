#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"
#include "SmcScanner.h"
#include <queue>
#include <set>

using ShardStateDataPtr = std::shared_ptr<ShardStateData>;


class ShardRangeScanner : public td::actor::Actor {
private:
    vm::AugmentedDictionary accounts_dict_;
    td::Bits256 start_addr_;
    td::Bits256 end_addr_;
    td::Promise<std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>>> promise_;

public:
    ShardRangeScanner(vm::AugmentedDictionary accounts_dict, td::Bits256 start_addr, td::Bits256 end_addr,
                     td::Promise<std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>>> promise);
    void start_up() override;
};

using AddrRange = std::pair<td::Bits256, td::Bits256>;

class ShardStateScanner: public td::actor::Actor {
private:
  td::Ref<vm::Cell> shard_state_;
  MasterchainBlockDataState mc_block_ds_;

  ShardStateDataPtr shard_state_data_;
  Options options_;

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
  std::set<AddrRange> ranges_in_progress_;

public:
  ShardStateScanner(td::Ref<vm::Cell> shard_state, MasterchainBlockDataState mc_block_ds, Options options);

  void schedule_next();
  void start_up() override;

  std::string get_checkpoint_file_path();
  
  // New parallel processing methods
  void range_scan_completed(AddrRange range, std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> results);

  void range_parsed(AddrRange range, std::vector<InsertData> results);
  void batch_inserted(AddrRange range);
  void update_checkpoint(td::Bits256 new_checkpoint);
};
