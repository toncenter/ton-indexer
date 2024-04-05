#include "DbScanner.h"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "td/actor/MultiPromise.h"

using namespace ton::validator;

void DbCacheWrapper::get_block_data(ConstBlockHandle handle, td::Promise<td::Ref<BlockData>> promise) {
  auto it = block_data_cache_.find(handle->id());
  if (it != block_data_cache_.end()) {
    auto res = it->second;
    promise.set_value(std::move(res)); // Cache hit
    // Move the accessed item to the end of the list
    block_data_cache_order_.remove(handle->id());
    block_data_cache_order_.push_back(handle->id());
  } else {
    // Check if there are pending requests for this block
    auto pending_it = block_data_pending_requests_.find(handle->id());
    if (pending_it != block_data_pending_requests_.end()) {
      // If a request is pending, add the promise to the list of pending promises
      pending_it->second.push_back(std::move(promise));
    } else {
      // Cache miss - initiate a request to the database
      block_data_pending_requests_[handle->id()].push_back(std::move(promise));

      auto cache_miss_callback = [this, SelfId = actor_id(this), handle](td::Result<td::Ref<BlockData>> res) mutable {
        td::actor::send_closure(SelfId, &DbCacheWrapper::got_block_data, handle, std::move(res));
      };
      td::actor::send_closure(db_, &RootDb::get_block_data, handle, std::move(cache_miss_callback));
    }
  }
}

void DbCacheWrapper::got_block_data(ConstBlockHandle handle, td::Result<td::Ref<BlockData>> res) {
  if (res.is_ok()) {
    // Check if the cache is full
    if (block_data_cache_.size() >= max_cache_size_) {
      // Erase the least recently used item
      block_data_cache_.erase(block_data_cache_order_.front());
      block_data_cache_order_.pop_front();
    }

    // Add the item to the cache and to the end of the list
    block_data_cache_[handle->id()] = res.ok_ref();
    block_data_cache_order_.push_back(handle->id());
  }

  auto it = block_data_pending_requests_.find(handle->id());
  if (it != block_data_pending_requests_.end()) {
    for (auto& pending_promise : it->second) {
      pending_promise.set_result(res.clone());
    }
    block_data_pending_requests_.erase(handle->id());
  }
}

void DbCacheWrapper::get_block_state(ConstBlockHandle handle, td::Promise<td::Ref<ShardState>> promise) {
  auto it = block_state_cache_.find(handle->id());
  if (it != block_state_cache_.end()) {
    auto res = it->second;
    promise.set_value(std::move(res)); // Cache hit
    // Move the accessed item to the end of the list
    block_state_cache_order_.remove(handle->id());
    block_state_cache_order_.push_back(handle->id());
  } else {
    // Check if there are pending requests for this block
    auto pending_it = block_state_pending_requests_.find(handle->id());
    if (pending_it != block_state_pending_requests_.end()) {
      // If a request is pending, add the promise to the list of pending promises
      pending_it->second.push_back(std::move(promise));
    } else {
      // Cache miss - initiate a request to the database
      block_state_pending_requests_[handle->id()].push_back(std::move(promise));

      auto cache_miss_callback = [this, SelfId = actor_id(this), handle](td::Result<td::Ref<ShardState>> res) mutable {
        td::actor::send_closure(SelfId, &DbCacheWrapper::got_block_state, handle, std::move(res));
      };
      td::actor::send_closure(db_, &RootDb::get_block_state, handle, std::move(cache_miss_callback));
    }
  }
}

void DbCacheWrapper::got_block_state(ConstBlockHandle handle, td::Result<td::Ref<ShardState>> res) {
  if (res.is_ok()) {
    // Check if the cache is full
    if (block_state_cache_.size() >= max_cache_size_) {
      // Erase the least recently used item
      block_state_cache_.erase(block_state_cache_order_.front());
      block_state_cache_order_.pop_front();
    }

    // Add the item to the cache and to the end of the list
    block_state_cache_[handle->id()] = res.ok_ref();
    block_state_cache_order_.push_back(handle->id());
  }

  auto it = block_state_pending_requests_.find(handle->id());
  if (it != block_state_pending_requests_.end()) {
    for (auto& pending_promise : it->second) {
      pending_promise.set_result(res.clone());
    }
    block_state_pending_requests_.erase(handle->id());
  }
}

class GetBlockDataState: public td::actor::Actor {
private:
  td::actor::ActorId<ton::validator::RootDb> db_;
  td::actor::ActorId<DbCacheWrapper> cache_db_;
  td::Promise<BlockDataState> promise_;
  ton::BlockIdExt blk_;

  td::Ref<BlockData> block_data_;
  td::Ref<ShardState> block_state_;
public:
  GetBlockDataState(td::actor::ActorId<ton::validator::RootDb> db, td::actor::ActorId<DbCacheWrapper> cache_db, ton::BlockIdExt blk, td::Promise<BlockDataState> promise) :
    db_(db),
    cache_db_(cache_db),
    blk_(blk),
    promise_(std::move(promise)) {
  }

  void start_up() override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<BlockHandle> R) {
      td::actor::send_closure(SelfId, &GetBlockDataState::got_block_handle, std::move(R));
    });
    td::actor::send_closure(db_, &RootDb::get_block_handle, blk_, std::move(P));
  }

  void got_block_handle(td::Result<BlockHandle> handle) {
    if (handle.is_error()) {
      promise_.set_error(handle.move_as_error_prefix(PSLICE() << blk_.to_str() << ": "));
      stop();
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<BlockData>> res) {
      td::actor::send_closure(SelfId, &GetBlockDataState::got_block_data, std::move(res));
    });
    td::actor::send_closure(cache_db_, &DbCacheWrapper::get_block_data, handle.ok(), std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<ShardState>> res) {
      td::actor::send_closure(SelfId, &GetBlockDataState::got_block_state, std::move(res));
    });
    td::actor::send_closure(cache_db_, &DbCacheWrapper::get_block_state, handle.ok(), std::move(R));
  }

  void got_block_data(td::Result<td::Ref<BlockData>> block_data) {
    if (block_data.is_error()) {
      promise_.set_error(block_data.move_as_error());
      stop();
      return;
    }

    block_data_ = block_data.move_as_ok();

    check_return();
  }

  void got_block_state(td::Result<td::Ref<ShardState>> block_state) {
    if (block_state.is_error()) {
      promise_.set_error(block_state.move_as_error());
      stop();
      return;
    }

    block_state_ = block_state.move_as_ok();

    check_return();
  }

  void check_return() {
    if (block_data_.not_null() && block_state_.not_null()) {
      promise_.set_value({std::move(block_data_), std::move(block_state_)});
      stop();
    }
  }
};


//
// IndexQuery
//
class IndexQuery: public td::actor::Actor {
private:
  const int mc_seqno_;
  td::actor::ActorId<ton::validator::RootDb> db_;
  td::actor::ActorId<DbCacheWrapper> cache_db_;
  td::Promise<MasterchainBlockDataState> promise_;

  td::Ref<BlockData> mc_block_data_;
  td::Ref<MasterchainState> mc_block_state_;

  td::Ref<MasterchainState> mc_prev_block_state_;
  int pending_{0};

  std::vector<ton::BlockIdExt> shard_block_ids_;
  std::queue<ton::BlockIdExt> blocks_queue_;

  MasterchainBlockDataState result_;

public:
  IndexQuery(int mc_seqno, td::actor::ActorId<ton::validator::RootDb> db, td::actor::ActorId<DbCacheWrapper> cache_db, td::Promise<MasterchainBlockDataState> promise) : 
    db_(db), 
    cache_db_(cache_db),
    mc_seqno_(mc_seqno),
    promise_(std::move(promise)) {
  }

  void start_up() override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ConstBlockHandle> R) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_handle, std::move(R));
    });
    td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull(ton::masterchainId, ton::shardIdAll), mc_seqno_, std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ConstBlockHandle> R) {
      td::actor::send_closure(SelfId, &IndexQuery::got_prev_block_handle, std::move(R));
    });
    td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull(ton::masterchainId, ton::shardIdAll), mc_seqno_ - 1, std::move(R));
  }

  void got_mc_block_handle(td::Result<ConstBlockHandle> handle) {
    if (handle.is_error()) {
      promise_.set_error(handle.move_as_error());
      stop();
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<BlockData>> res) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_data, std::move(res));
    });
    td::actor::send_closure(cache_db_, &DbCacheWrapper::get_block_data, handle.ok(), std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<ShardState>> res) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_state, std::move(res));
    });
    td::actor::send_closure(cache_db_, &DbCacheWrapper::get_block_state, handle.ok(), std::move(R));
  }

  void got_prev_block_handle(td::Result<ConstBlockHandle> handle) {
    if (handle.is_error()) {
      promise_.set_error(handle.move_as_error());
      stop();
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<ShardState>> res) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_prev_block_state, std::move(res));
    });

    td::actor::send_closure(cache_db_, &DbCacheWrapper::get_block_state, handle.move_as_ok(), std::move(P));
  }

  void got_mc_block_data(td::Result<td::Ref<BlockData>> block_data) {
    if (block_data.is_error()) {
      promise_.set_error(block_data.move_as_error());
      stop();
      return;
    }

    mc_block_data_ = block_data.move_as_ok();
    pending_++;
    check_pending_three();
  }

  void got_mc_block_state(td::Result<td::Ref<ShardState>> state) {
    if (state.is_error()) {
      promise_.set_error(state.move_as_error());
      stop();
      return;
    }

    mc_block_state_ = td::Ref<MasterchainState>(state.move_as_ok());
    pending_++;
    check_pending_three();
  }

  void got_mc_prev_block_state(td::Result<td::Ref<ShardState>> state) {
    if (state.is_error()) {
      promise_.set_error(state.move_as_error());
      stop();
      return;
    }

    mc_prev_block_state_ = td::Ref<MasterchainState>(state.move_as_ok());
    pending_++;
    check_pending_three();
  }

  void check_pending_three() {
    if (pending_ != 3) {
      return;
    }

    result_.shard_blocks_.push_back({mc_block_data_, mc_block_state_});
    result_.shard_blocks_diff_.push_back({mc_block_data_, mc_block_state_});

    fetch_all_shard_blocks_between_current_and_prev_mc_blocks();
  }

  void fetch_all_shard_blocks_between_current_and_prev_mc_blocks() {
    auto current_shards = mc_block_state_->get_shards();
    auto prev_shards = mc_prev_block_state_->get_shards();

    for (auto& s : current_shards) {
      blocks_queue_.push(s->top_block_id());
    }
    // LOG(INFO) << "seqno: " << mc_block_state_->get_block_id().seqno()
    //           << " queue: " << blocks_queue_.size();

    process_blocks_queue(true);
  }

  void process_blocks_queue(bool to_shards) {
    if (blocks_queue_.empty()) {
      // LOG(INFO) << "mc_seqno: " << result_.shard_blocks_[0].block_state->get_seqno() 
      //           << " shards: " << result_.shard_blocks_.size() 
      //           << " diff: " << result_.shard_blocks_diff_.size();
      promise_.set_value(std::move(result_));
      stop();
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &IndexQuery::error, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &IndexQuery::process_blocks_queue, false);
      }
    });

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise(std::move(P));

    bool no_shard_blocks = true;
    // LOG(INFO) << "mc_seqno: " <<  mc_block_state_->get_seqno()
    //           << " queue size: " << blocks_queue_.size() 
    //           << " result size: " << result_.shard_blocks_diff_.size() 
    //           << " (" << result_.shard_blocks_.size() << ")";
    while (!blocks_queue_.empty()) {
      auto blk = blocks_queue_.front();
      blocks_queue_.pop();
      
      bool to_diff = true;
      for (auto& prev_shard : mc_prev_block_state_->get_shards()) {
        if (prev_shard->top_block_id() == blk) {
          to_diff = false;
          break;
        }
      }
      if (std::find(shard_block_ids_.begin(), shard_block_ids_.end(), blk) != shard_block_ids_.end()) {
        to_diff = false;
      }
      if (!(to_shards || to_diff)) {
        continue;
      }
      shard_block_ids_.push_back(blk);

      no_shard_blocks = false;
      auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this), blk, to_diff, to_shards, promise = ig.get_promise()](td::Result<BlockDataState> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix(PSTRING() << blk.to_str() << ": "));
        } else {
          td::actor::send_closure(SelfId, &IndexQuery::got_shard_block, R.move_as_ok(), to_diff, to_shards, std::move(promise));
        }
      });
      td::actor::create_actor<GetBlockDataState>("getblockdatastate", db_, cache_db_, blk, std::move(Q)).release();
    }

    if (no_shard_blocks) {
      // LOG(INFO) << "mc_seqno: " << result_.shard_blocks_[0].block_state->get_seqno() 
      //           << " shards: " << result_.shard_blocks_.size() 
      //           << " diff: " << result_.shard_blocks_diff_.size();
      promise_.set_value(std::move(result_));
      stop();
    }
  }

  void got_shard_block(BlockDataState block_data_state, bool to_diff, bool to_shards, td::Promise<td::Unit> promise) {
    std::vector<ton::BlockIdExt> prev;
    ton::BlockIdExt mc_blkid;
    bool after_split;
    auto res = block::unpack_block_prev_blk_ext(block_data_state.block_data->root_cell(), block_data_state.block_data->block_id(), prev, mc_blkid, after_split);

    if (to_diff) {
      for (auto& p: prev) {
        blocks_queue_.push(p);
      }
      result_.shard_blocks_diff_.push_back(block_data_state);  // std::move was removed here
    }
      
    if (to_shards)
      result_.shard_blocks_.push_back(block_data_state);

    promise.set_value(td::Unit());
  }

  void error(td::Status error) {
    promise_.set_error(std::move(error));
    stop();
  }
};

//
// DbScanner
//
void DbScanner::start_up() {
  auto opts = ton::validator::ValidatorManagerOptions::create(
        ton::BlockIdExt{ton::masterchainId, ton::shardIdAll, 0, ton::RootHash::zero(), ton::FileHash::zero()},
        ton::BlockIdExt{ton::masterchainId, ton::shardIdAll, 0, ton::RootHash::zero(), ton::FileHash::zero()});
  auto mode = mode_ == dbs_readonly ? td::DbOpenMode::db_readonly : td::DbOpenMode::db_secondary;
  db_ = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_, std::move(opts), mode);
  db_caching_ = td::actor::create_actor<DbCacheWrapper>("cache_db", db_.get(), max_db_cache_size_);

  td::actor::send_closure(actor_id(this), &DbScanner::update_last_mc_seqno);
  alarm_timestamp() = td::Timestamp::in(1.0);
}

void DbScanner::update_last_mc_seqno() {
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R) {
    R.ensure();
    td::actor::send_closure(SelfId, &DbScanner::set_last_mc_seqno, R.move_as_ok());
  });
  td::actor::send_closure(db_, &RootDb::get_max_masterchain_seqno, std::move(P));
}

void DbScanner::set_last_mc_seqno(ton::BlockSeqno mc_seqno) {
  if (mc_seqno > last_known_seqno_) {
    LOG(DEBUG) << "New masterchain seqno: " << mc_seqno;
  }
  last_known_seqno_ = mc_seqno;
}

void DbScanner::get_last_mc_seqno(td::Promise<ton::BlockSeqno> promise) {
  td::actor::send_closure(db_, &RootDb::get_max_masterchain_seqno, std::move(promise));
}

void DbScanner::get_oldest_mc_seqno(td::Promise<ton::BlockSeqno> promise) {
  td::actor::send_closure(db_, &RootDb::get_min_masterchain_seqno, std::move(promise));
}

void DbScanner::get_mc_block_handle(ton::BlockSeqno seqno, td::Promise<ton::validator::ConstBlockHandle> promise) {
  td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull(ton::masterchainId, ton::shardIdAll), seqno, std::move(promise));
}

void DbScanner::catch_up_with_primary() {
  auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    R.ensure();
  });
  td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
}

void DbScanner::fetch_seqno(std::uint32_t mc_seqno, td::Promise<MasterchainBlockDataState> promise) {
  td::actor::create_actor<IndexQuery>("indexquery", mc_seqno, db_.get(), db_caching_.get(), std::move(promise)).release();
}

void DbScanner::alarm() {
  if (mode_ == dbs_readonly) {
    return;
  }
  alarm_timestamp() = td::Timestamp::in(1.0);
  if (db_.empty()) {
    return;
  }
  td::actor::send_closure(actor_id(this), &DbScanner::update_last_mc_seqno);

  if (!out_of_sync_) {
    td::actor::send_closure(actor_id(this), &DbScanner::catch_up_with_primary);
  }
}
