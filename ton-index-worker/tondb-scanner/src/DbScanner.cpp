#include "DbScanner.h"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "td/actor/MultiPromise.h"
#include "Statistics.h"

using namespace ton::validator;

class GetBlockDataState: public td::actor::Actor {
private:
  td::actor::ActorId<ton::validator::RootDb> db_;
  td::Promise<BlockDataState> promise_;
  ConstBlockHandle handle_;

  td::Ref<BlockData> block_data_;
  td::Ref<vm::Cell> block_state_root_;
public:
  GetBlockDataState(td::actor::ActorId<ton::validator::RootDb> db, ConstBlockHandle handle, td::Promise<BlockDataState> promise) :
    db_(db),
    handle_(handle),
    promise_(std::move(promise)) {
  }

  void start_up() override {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<BlockData>> res) {
      td::actor::send_closure(SelfId, &GetBlockDataState::got_block_data, std::move(res));
    });
    td::actor::send_closure(db_, &RootDb::get_block_data, handle_, std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<vm::Cell>> res) {
      td::actor::send_closure(SelfId, &GetBlockDataState::got_block_state, std::move(res));
    });
    td::actor::send_closure(db_, &RootDb::get_block_state_root, handle_, std::move(R));
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

  void got_block_state(td::Result<td::Ref<vm::Cell>> block_state) {
    if (block_state.is_error()) {
      promise_.set_error(block_state.move_as_error());
      stop();
      return;
    }

    block_state_root_ = block_state.move_as_ok();
    check_return();
  }

  void check_return() {
    if (block_data_.not_null() && block_state_root_.not_null()) {
      promise_.set_value({std::move(block_data_), std::move(block_state_root_), std::move(handle_)});
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
  td::Promise<MasterchainBlockDataState> promise_;
  td::Timer timer_{true};

  td::Ref<BlockData> mc_block_data_;
  td::Ref<vm::Cell> mc_block_state_;
  ConstBlockHandle mc_block_handle_;

  td::Ref<MasterchainState> mc_prev_block_state_;
  int pending_{0};

  std::queue<ton::BlockId> blocks_queue_;

  std::set<ton::BlockId> current_shard_blk_ids_;
  std::unordered_set<ConstBlockHandle> shard_block_handles_;

  MasterchainBlockDataState result_;

public:
  IndexQuery(int mc_seqno, td::actor::ActorId<ton::validator::RootDb> db, td::Promise<MasterchainBlockDataState> promise) : 
    db_(db), 
    mc_seqno_(mc_seqno),
    promise_(std::move(promise)) {
  }

  void start_up() override {
    timer_.resume();
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ConstBlockHandle> R) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_handle, std::move(R));
    });
    td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull(ton::masterchainId, ton::shardIdAll), mc_seqno_, std::move(P));
  }

  void got_mc_block_handle(td::Result<ConstBlockHandle> handle) {
    if (handle.is_error()) {
      promise_.set_error(handle.move_as_error());
      stop();
      return;
    }
    mc_block_handle_ = handle.move_as_ok();

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<BlockData>> res) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_data, std::move(res));
    });
    td::actor::send_closure(db_, &RootDb::get_block_data, mc_block_handle_, std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<vm::Cell>> res) {
      td::actor::send_closure(SelfId, &IndexQuery::got_mc_block_state, std::move(res));
    });
    td::actor::send_closure(db_, &RootDb::get_block_state_root, mc_block_handle_, std::move(R));
  }

  void got_mc_block_data(td::Result<td::Ref<BlockData>> block_data) {
    if (block_data.is_error()) {
      promise_.set_error(block_data.move_as_error());
      stop();
      return;
    }

    mc_block_data_ = block_data.move_as_ok();
    pending_++;
    check_pending();
  }

  void got_mc_block_state(td::Result<td::Ref<vm::Cell>> state) {
    if (state.is_error()) {
      promise_.set_error(state.move_as_error());
      stop();
      return;
    }

    mc_block_state_ = state.move_as_ok();
    pending_++;
    check_pending();
  }

  void check_pending() {
    if (pending_ != 2) {
      return;
    }

    result_.shard_blocks_.push_back({mc_block_data_, mc_block_state_, mc_block_handle_});
    result_.shard_blocks_diff_.push_back({mc_block_data_, mc_block_state_, mc_block_handle_});

    fetch_shard_blocks();
  }

  void fetch_shard_blocks() {
    block::gen::McStateExtra::Record mc_extra;
    block::gen::ShardStateUnsplit::Record state;
    block::gen::McStateExtra::Record extra;
    block::ShardConfig shards_config;
    if (!(tlb::unpack_cell(mc_block_state_, state) &&
          tlb::unpack_cell(state.custom->prefetch_ref(), extra) && 
          shards_config.unpack(extra.shard_hashes))) {
      return error(td::Status::Error("cannot extract shard configuration from masterchain state extra information"));
    }

    for (auto& s : shards_config.get_shard_hash_ids(true)) {
      if (s.seqno > 0) {
        blocks_queue_.push(s);
      } else {
        LOG(WARNING) << "Skipping block workchain: " << s.workchain << " shard: " << s.shard << " seqno: " << s.seqno;
      }
      current_shard_blk_ids_.insert(s);
    }
    fetch_block_handles();
  }

  void fetch_block_handles() {
    if (blocks_queue_.empty()) {
      fetch_block_data_states();
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &IndexQuery::error, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &IndexQuery::fetch_block_handles);
      }
    });

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise(std::move(P));

    while (!blocks_queue_.empty()) {
      auto blk_id = blocks_queue_.front();
      blocks_queue_.pop();

      auto P = td::PromiseCreator::lambda([&, SelfId = actor_id(this), mc_seqno = mc_seqno_, blk_id, current_shard_blk_ids = current_shard_blk_ids_, promise = ig.get_promise()](td::Result<ConstBlockHandle> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix(PSTRING() << blk_id.to_str() << ": "));
        } else {
          auto handle = R.move_as_ok();
          if (handle->masterchain_ref_block() == mc_seqno || current_shard_blk_ids.count(handle->id().id) > 0) {
            td::actor::send_closure(SelfId, &IndexQuery::add_block_handle, std::move(handle), std::move(promise));
          } else {
            promise.set_result(td::Unit());
          }
        }
      });
      td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull{blk_id.workchain, blk_id.shard}, blk_id.seqno, std::move(P));
    }
  }

  void add_block_handle(ConstBlockHandle handle, td::Promise<td::Unit> promise) {
    if (shard_block_handles_.count(handle)) {
      promise.set_result(td::Unit());
      return;
    }

    shard_block_handles_.insert(handle);
    for (const auto& prev: handle->prev()) {
      if (prev.id.seqno > 0) {
        blocks_queue_.push(prev.id);
      } else {
        LOG(WARNING) << "Skipping block workchain: " << prev.id.workchain << " shard: " << prev.id.shard << " seqno: " << prev.id.seqno;
      }
    }
    promise.set_result(td::Unit());
  }

  void fetch_block_data_states() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &IndexQuery::error, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &IndexQuery::finish);
      }
    });

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise(std::move(P));

    for (auto& handle: shard_block_handles_) {
      auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), handle, promise = ig.get_promise()](td::Result<BlockDataState> R) mutable {
        if (R.is_error()) {
          promise.set_error(R.move_as_error_prefix(PSTRING() << handle->id().to_str() << ": "));
        } else {
          td::actor::send_closure(SelfId, &IndexQuery::got_shard_block, R.move_as_ok(), handle, std::move(promise));
        }
      });
      td::actor::create_actor<GetBlockDataState>("getblockdatastate", db_, handle, std::move(P)).release();
    }
  }

  void got_shard_block(BlockDataState block_data_state, ConstBlockHandle handle, td::Promise<td::Unit> promise) {
    if (current_shard_blk_ids_.count(block_data_state.block_data->block_id().id) > 0) {
      result_.shard_blocks_.push_back(block_data_state);
    }
    if (handle->masterchain_ref_block() == mc_seqno_) {
      result_.shard_blocks_diff_.push_back(block_data_state);
    }
    promise.set_result(td::Unit());
  }

  void finish() {
    // LOG(INFO) << "For seqno " << mc_seqno_ << " got " << result_.shard_blocks_.size() << " shard blocks and " << result_.shard_blocks_diff_.size() << " shard blocks diff";
    promise_.set_value(std::move(result_));
    g_statistics.record_time(FETCH_SEQNO, timer_.elapsed() * 1e3);
    stop();
  }

  void error(td::Status error) {
    promise_.set_error(std::move(error));
    g_statistics.record_count(SEQNO_FETCH_ERROR);
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
  opts.write().set_max_open_archive_files(500);
  if (mode_ == dbs_secondary) {
    CHECK(secondary_working_dir_.has_value());
    opts.write().set_secondary_working_dir(secondary_working_dir_.value());
  }
  auto mode = mode_ == dbs_readonly ? td::DbOpenMode::db_readonly : td::DbOpenMode::db_secondary;
  db_ = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_, std::move(opts), mode);

  alarm_timestamp() = td::Timestamp::in(1.0);
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

void DbScanner::get_cell_db_reader(td::Promise<std::shared_ptr<vm::CellDbReader>> promise) {
  td::actor::send_closure(db_, &RootDb::get_cell_db_reader, std::move(promise));
}

void DbScanner::catch_up_with_primary() {
  auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), this, timer = td::Timer{}](td::Result<td::Unit> R) mutable {
    R.ensure();
    this->is_ready_ = true;
    timer.pause();
    g_statistics.record_time(CATCH_UP_WITH_PRIMARY, timer.elapsed() * 1e3);
  });
  td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
}

void DbScanner::fetch_seqno(std::uint32_t mc_seqno, td::Promise<MasterchainBlockDataState> promise) {
  td::actor::create_actor<IndexQuery>("indexquery", mc_seqno, db_.get(), std::move(promise)).release();
}

void DbScanner::alarm() {
  if (mode_ == dbs_readonly) {
    return;
  }
  alarm_timestamp() = td::Timestamp::in(catch_up_interval_);
  if (db_.empty()) {
    return;
  }

  td::actor::send_closure(actor_id(this), &DbScanner::catch_up_with_primary);
}
