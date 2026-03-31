#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"
#include "SmcScanner.h"
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
  td::actor::ActorId<DbScanner> db_scanner_;
  std::size_t current_shard_index_;
  ReloadShardStateContextPtr reload_context_;

  ShardStateDataPtr shard_state_data_;
  Options options_;

  td::Bits256 next_batch_start_;
  
  ton::ShardIdFull shard_;
  uint32_t accounts_cnt_{0};

  // Parallel processing members
  std::set<AddrRange> ranges_in_progress_;
  std::uint32_t completed_batches_since_reload_{0};
  bool refresh_in_progress_{false};

public:
  ShardStateScanner(td::actor::ActorId<DbScanner> db_scanner,
                    std::size_t current_shard_index,
                    ReloadShardStateContextPtr reload_context,
                    ShardStateDataPtr shard_state_data,
                    Options options);

  void schedule_next();
  void start_up() override;

  std::string get_checkpoint_file_path();
  
  // New parallel processing methods
  void range_scan_completed(AddrRange range, std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> results);

  void range_parsed(AddrRange range, std::vector<InsertData> results);
  void batch_inserted(AddrRange range);
  void fail_range(AddrRange range, td::Status error);
  void request_shard_state_reload();
  void got_reload_cell_db_reader(td::Result<std::shared_ptr<vm::CellDbReader>> result);
  void update_checkpoint(td::Bits256 new_checkpoint);
};
