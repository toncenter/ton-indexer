#pragma once
#include <queue>
#include "validator/manager-disk.h"
#include "validator/db/rootdb.hpp"

#include "IndexData.h"
#include "DataParser.h"
#include "EventProcessor.h"

class DbCacheWrapper;

enum ScannerMode { dbs_readonly, dbs_secondary };

class DbScanner: public td::actor::Actor {
private:
  std::string db_root_;
  ScannerMode mode_;
  td::int32 max_db_cache_size_{256};
  std::atomic<td::uint32> active_fetches;
  ton::BlockSeqno last_known_seqno_;

  td::actor::ActorOwn<ton::validator::ValidatorManagerInterface> validator_manager_;
  td::actor::ActorOwn<ton::validator::RootDb> db_;
  td::actor::ActorOwn<DbCacheWrapper> db_caching_;
public:
  DbScanner(std::string db_root, ScannerMode mode, td::int32 max_db_cache_size = 256) 
    : db_root_(db_root), mode_(mode), max_db_cache_size_(max_db_cache_size) {}

  ton::BlockSeqno get_last_known_seqno() {
    return last_known_seqno_;
  }

  void start_up() override;
  void alarm() override;

  void fetch_seqno(std::uint32_t mc_seqno, td::Promise<MasterchainBlockDataState> promise);
  void get_last_mc_seqno(td::Promise<ton::BlockSeqno> promise);
  void get_oldest_mc_seqno(td::Promise<ton::BlockSeqno> promise);
  void get_mc_block_handle(ton::BlockSeqno seqno, td::Promise<ton::validator::ConstBlockHandle> promise);
private:
  void set_last_mc_seqno(ton::BlockSeqno mc_seqno);
  void catch_up_with_primary();
  void update_last_mc_seqno();
};

struct BlockIdExtHasher {
    std::size_t operator()(const ton::BlockIdExt& k) const {
        return std::hash<std::string>()(k.to_str());
    }
};

class DbCacheWrapper: public td::actor::Actor {
private:
  td::actor::ActorId<ton::validator::RootDb> db_;
  std::list<ton::BlockIdExt> block_data_cache_order_;
  std::unordered_map<ton::BlockIdExt, td::Ref<ton::validator::BlockData>, BlockIdExtHasher> block_data_cache_;
  std::unordered_map<ton::BlockIdExt, std::vector<td::Promise<td::Ref<ton::validator::BlockData>>>, BlockIdExtHasher> block_data_pending_requests_;

  std::list<ton::BlockIdExt> block_state_cache_order_;
  std::unordered_map<ton::BlockIdExt, td::Ref<ton::validator::ShardState>, BlockIdExtHasher> block_state_cache_;
  std::unordered_map<ton::BlockIdExt, std::vector<td::Promise<td::Ref<ton::validator::ShardState>>>, BlockIdExtHasher> block_state_pending_requests_;

  size_t max_cache_size_;

public:
  DbCacheWrapper(td::actor::ActorId<ton::validator::RootDb> db, size_t max_cache_size)
    : db_(db), max_cache_size_(max_cache_size) {
  }
  
  void get_block_data(ton::validator::ConstBlockHandle handle, td::Promise<td::Ref<ton::validator::BlockData>> promise);
  void got_block_data(ton::validator::ConstBlockHandle handle, td::Result<td::Ref<ton::validator::BlockData>> res);
  void get_block_state(ton::validator::ConstBlockHandle handle, td::Promise<td::Ref<ton::validator::ShardState>> promise);
  void got_block_state(ton::validator::ConstBlockHandle handle, td::Result<td::Ref<ton::validator::ShardState>> res);
};
