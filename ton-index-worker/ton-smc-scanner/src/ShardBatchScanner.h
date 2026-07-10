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

// Looks up one chunk of listed addresses against its own copy of the shard accounts
// dictionary and returns the found (address, ShardAccount) records
class AccountListLookupScanner : public td::actor::Actor {
private:
    vm::AugmentedDictionary accounts_dict_;
    std::vector<block::StdAddress> addresses_;
    ton::ShardIdFull shard_;
    td::Promise<std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>>> promise_;

public:
    AccountListLookupScanner(vm::AugmentedDictionary accounts_dict, std::vector<block::StdAddress> addresses,
                             ton::ShardIdFull shard,
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

// Scans accounts already assigned to this shard through the normal parse-and-insert path.
// Targeted scans do not use checkpoints or reload shard state.
class AccountListShardScanner: public td::actor::Actor {
private:
  ShardStateDataPtr shard_state_data_;
  Options options_;
  std::vector<block::StdAddress> addresses_;

  ton::ShardIdFull shard_;
  std::size_t next_index_{0};
  std::uint32_t batches_in_flight_{0};

  // Per-shard counters for the final summary.
  std::size_t found_cnt_{0};
  std::size_t missing_cnt_{0};
  std::size_t inserted_batches_{0};

public:
  AccountListShardScanner(ton::ShardIdFull shard, ShardStateDataPtr shard_state_data, Options options, std::vector<block::StdAddress> addresses);

  void start_up() override;
  void schedule_next();
  void chunk_looked_up(std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> results);
  void batch_parsed(std::vector<InsertData> results);
  void batch_inserted();
  void fail_batch(td::Status error);
};
