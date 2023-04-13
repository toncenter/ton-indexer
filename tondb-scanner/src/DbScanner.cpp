#include "DbScanner.h"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "td/actor/MultiPromise.h"

using namespace ton::validator;

class GetBlockDataState: public td::actor::Actor {
private:
  td::actor::ActorId<ton::validator::RootDb> db_;
  td::Promise<BlockDataState> promise_;
  ton::BlockIdExt blk_;

  td::Ref<BlockData> block_data_;
  td::Ref<ShardState> block_state_;
public:
  GetBlockDataState(td::actor::ActorId<ton::validator::RootDb> db, ton::BlockIdExt blk, td::Promise<BlockDataState> promise) :
    db_(db),
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
    td::actor::send_closure(db_, &RootDb::get_block_data, handle.ok(), std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<ShardState>> res) {
      td::actor::send_closure(SelfId, &GetBlockDataState::got_block_state, std::move(res));
    });
    td::actor::send_closure(db_, &RootDb::get_block_state, handle.ok(), std::move(R));
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

class IndexQuery: public td::actor::Actor {
private:
  const int mc_seqno_;
  td::actor::ActorId<ton::validator::RootDb> db_;
  td::Promise<std::vector<BlockDataState>> promise_;

  td::Ref<BlockData> mc_block_data_;
  td::Ref<MasterchainState> mc_block_state_;

  td::Ref<MasterchainState> mc_prev_block_state_;
  int pending_{0};

  std::vector<ton::BlockIdExt> shard_block_ids_;
  std::queue<ton::BlockIdExt> blocks_queue_;

  std::vector<BlockDataState> result_;

public:

  IndexQuery(int mc_seqno, td::actor::ActorId<ton::validator::RootDb> db, td::Promise<std::vector<BlockDataState>> promise) : 
    db_(db), 
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
    td::actor::send_closure(db_, &RootDb::get_block_data, handle.ok(), std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<ShardState>> res) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_state, std::move(res));
    });
    td::actor::send_closure(db_, &RootDb::get_block_state, handle.ok(), std::move(R));
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

    td::actor::send_closure(db_, &RootDb::get_block_state, handle.move_as_ok(), std::move(P));
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

    result_.push_back({mc_block_data_, mc_block_state_});

    fetch_all_shard_blocks_between_current_and_prev_mc_blocks();
  }

  void fetch_all_shard_blocks_between_current_and_prev_mc_blocks() {
    auto current_shards = mc_block_state_->get_shards();
    auto prev_shards = mc_prev_block_state_->get_shards();

    for (auto& s : current_shards) {
      blocks_queue_.push(s->top_block_id());
    }

    process_blocks_queue();
  }

  void process_blocks_queue() {
    if (blocks_queue_.empty()) {
      promise_.set_value(std::move(result_));
      stop();
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &IndexQuery::error, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &IndexQuery::process_blocks_queue);
      }
    });

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise(std::move(P));

    bool no_shard_blocks = true;
    while (!blocks_queue_.empty()) {
      auto blk = blocks_queue_.front();
      blocks_queue_.pop();
      
      bool skip = false;
      for (auto& prev_shard : mc_prev_block_state_->get_shards()) {
        if (prev_shard->top_block_id() == blk) {
          skip = true;
          break;
        }
      }
      if (std::find(shard_block_ids_.begin(), shard_block_ids_.end(), blk) != shard_block_ids_.end()) {
        skip = true;
      }
      if (skip) {
        continue;
      }
      shard_block_ids_.push_back(blk);

      no_shard_blocks = false;
      
      auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this), blk, promise = ig.get_promise()](td::Result<BlockDataState> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix(PSTRING() << blk.to_str() << ": "));
        } else {
          td::actor::send_closure(SelfId, &IndexQuery::got_shard_block, R.move_as_ok());
          promise.set_value(td::Unit());
        }
      });
      td::actor::create_actor<GetBlockDataState>("getblockdatastate", db_, blk, std::move(Q)).release();
    }

    if (no_shard_blocks) {
      promise_.set_value(std::move(result_));
      stop();
    }
  }

  void got_shard_block(BlockDataState block_data_state) {
    std::vector<ton::BlockIdExt> prev;
    ton::BlockIdExt mc_blkid;
    bool after_split;
    auto res = block::unpack_block_prev_blk_ext(block_data_state.block_data->root_cell(), block_data_state.block_data->block_id(), prev, mc_blkid, after_split);
    for (auto& p: prev) {
      blocks_queue_.push(p);
    }

    result_.push_back(std::move(block_data_state));
  }

  void error(td::Status error) {
    promise_.set_error(std::move(error));
    stop();
  }
};

void DbScanner::start_up() {
  alarm_timestamp() = td::Timestamp::in(3.0);
}

void DbScanner::run() {
  db_ = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_);
}

void DbScanner::update_last_mc_seqno() {
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<int> R) {
    R.ensure();
    td::actor::send_closure(SelfId, &DbScanner::set_last_mc_seqno, R.move_as_ok());
  });

  td::actor::send_closure(db_, &RootDb::get_max_masterchain_seqno, std::move(P));
}

void DbScanner::set_last_mc_seqno(int mc_seqno) {
  if (mc_seqno > last_known_seqno_) {
    LOG(INFO) << "New masterchain seqno: " << mc_seqno;
  }
  if (last_known_seqno_ != -1) {
    for (int s = last_known_seqno_ + 1; s < mc_seqno + 1; s++) {
      seqnos_to_process_.push(s);
    }
  }
  last_known_seqno_ = mc_seqno;
}

void DbScanner::catch_up_with_primary() {
  auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    R.ensure();
  });
  td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
}

void DbScanner::schedule_for_processing() {
  while (!seqnos_to_process_.empty() && seqnos_in_progress_.size() < max_parallel_fetch_actors_) {
    auto mc_seqno = seqnos_to_process_.front();
    seqnos_to_process_.pop();

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<std::vector<BlockDataState>> res) {
    td::actor::send_closure(SelfId, &DbScanner::seqno_fetched, mc_seqno, std::move(res));
    });

    LOG(DEBUG) << "Creating IndexQuery for mc seqno " << mc_seqno;
    td::actor::create_actor<IndexQuery>("indexquery", mc_seqno, db_.get(), std::move(R)).release();
    seqnos_in_progress_.insert(mc_seqno);
  }
}

void DbScanner::seqno_fetched(int mc_seqno, td::Result<std::vector<BlockDataState>> blocks_data_state) {
  CHECK(seqnos_in_progress_.erase(mc_seqno) == 1);

  if (blocks_data_state.is_error()) {
    LOG(ERROR) << "mc_seqno " << mc_seqno << " failed to fetch BlockDataState: " << blocks_data_state.move_as_error();
    seqnos_to_process_.push(mc_seqno);
    return;
  }

  auto blks = blocks_data_state.move_as_ok();
  for (auto& blk: blks) {
    LOG(DEBUG) << "Got block data and state for " << blk.block_data->block_id().id.to_str();
  }

  auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<td::Unit> res) {
    if (res.is_ok()) {
      LOG(DEBUG) << "MC seqno " << mc_seqno << " insert success";
    } else {
      LOG(WARNING) << "MC seqno " << mc_seqno << " insert failed: " << res.move_as_error();
    }
  });

  td::actor::send_closure(insert_manager_, &InsertManagerInterface::insert, std::move(blks), std::move(R));
}

void DbScanner::alarm() {
  alarm_timestamp() = td::Timestamp::in(1.0);
  if (db_.empty()) {
    return;
  }

  td::actor::send_closure(actor_id(this), &DbScanner::update_last_mc_seqno);
  td::actor::send_closure(actor_id(this), &DbScanner::catch_up_with_primary);
  td::actor::send_closure(actor_id(this), &DbScanner::schedule_for_processing);
}