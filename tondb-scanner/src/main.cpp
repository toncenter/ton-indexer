#include <iostream>
#include <queue>

#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/actor/MultiPromise.h"

#include "validator/manager-disk.h"
#include "validator/db/rootdb.hpp"

#include "crypto/vm/cp0.h"

#include "db-schema.hpp"

#include <pqxx/pqxx>

using namespace ton::validator;


class InsertOneMcSeqno: public td::actor::Actor {
private:
  std::vector<schema::Block> blocks_;
  td::Promise<td::Unit> promise_;
public:
  InsertOneMcSeqno(std::vector<schema::Block> blocks, td::Promise<td::Unit> promise): blocks_(std::move(blocks)), promise_(std::move(promise)) {
  }

  void prepare_insert_blocks(pqxx::connection &conn) {
    conn.prepare("insert_block",
              "INSERT INTO blocks (workchain, shard, seqno, root_hash, file_hash, mc_block_workchain, mc_block_shard, mc_block_seqno, global_id, version, after_merge, before_split, after_split, want_split, key_block, vert_seqno_incr, flags, gen_utime, start_lt, end_lt, validator_list_hash_short, gen_catchain_seqno, min_ref_mc_seqno, prev_key_block_seqno, vert_seqno, master_ref_seqno, rand_seed, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28) ON CONFLICT DO NOTHING;");
  }

  void insert_blocks(pqxx::work &transaction) {
    for (const auto& block : blocks_) {
      transaction.exec_prepared("insert_block" 
        ,block.workchain 
        ,block.shard 
        ,block.seqno 
        ,block.root_hash 
        ,block.file_hash 
        ,block.mc_block_workchain ? std::to_string(block.mc_block_workchain.value()).c_str() : nullptr
        ,block.mc_block_shard ? std::to_string(block.mc_block_shard.value()).c_str() : nullptr
        ,block.mc_block_seqno ? std::to_string(block.mc_block_seqno.value()).c_str() : nullptr
        ,block.global_id 
        ,block.version 
        ,block.after_merge 
        ,block.before_split 
        ,block.after_split 
        ,block.want_split 
        ,block.key_block 
        ,block.vert_seqno_incr 
        ,block.flags 
        ,block.gen_utime 
        ,block.start_lt 
        ,block.end_lt 
        ,block.validator_list_hash_short 
        ,block.gen_catchain_seqno 
        ,block.min_ref_mc_seqno 
        ,block.prev_key_block_seqno 
        ,block.vert_seqno 
        ,block.master_ref_seqno ? std::to_string(block.master_ref_seqno.value()).c_str() : nullptr
        ,block.rand_seed 
        ,block.created_by);
    }
  }

  void start_up() {
    try {
      pqxx::connection c("dbname=ton_index user=postgres password=123 hostaddr=127.0.0.1 port=54321");
      if (!c.is_open()) {
        promise_.set_error(td::Status::Error("Failed to open database"));
        stop();
        return;
      }
      prepare_insert_blocks(c);
      pqxx::work txn(c);
      insert_blocks(txn);
      txn.commit();
      promise_.set_value(td::Unit());
      stop();
    } catch (const std::exception &e) {
      promise_.set_error(td::Status::Error(PSLICE() << "Error inserting to PG: " << e.what()));
      stop();
    }
  }
};

class InsertManager: public td::actor::Actor {
private:
  std::queue<std::vector<BlockDataState>> insert_queue_;
  std::queue<td::Promise<td::Unit>> promise_queue_;
public:
  InsertManager() { // std::vector<BlockDataState> block_ds, td::Promise<td::Unit> promise): block_ds_(std::move(block_ds)), promise_(std::move(promise)) {
  }

  void start_up() override {
    alarm_timestamp() = td::Timestamp::in(1.0);
  }

   void alarm() override {
    LOG(INFO) << "insert_queue_ size: " << insert_queue_.size();

    std::vector<td::Promise<td::Unit>> promises;
    std::vector<schema::Block> schema_blocks;
    while (!insert_queue_.empty() && schema_blocks.size() < 1000) {
      auto block_ds = insert_queue_.front();
      insert_queue_.pop();

      auto promise = std::move(promise_queue_.front());
      promise_queue_.pop();

      auto schema_block = schema::BlockToSchema(std::move(block_ds));
      auto parse_res = schema_block.parse();
      if (parse_res.is_error()) {
        promise.set_error(parse_res.move_as_error_prefix("Error parsing block: "));
        continue;
      }
      promises.push_back(std::move(promise));
      auto blocks = schema_block.get_blocks();
      schema_blocks.insert(schema_blocks.end(), blocks.begin(), blocks.end());
    }
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), promises = std::move(promises)](td::Result<td::Unit> R) mutable {
      if (R.is_error()) {
        LOG(ERROR) << "Error inserting to PG: " << R.move_as_error();
        for (auto& p : promises) {
          p.set_error(R.move_as_error());
        }
        return;
      }
      
      for (auto& p : promises) {
        p.set_result(td::Unit());
      }
    });
    td::actor::create_actor<InsertOneMcSeqno>("insertonemcseqno", std::move(schema_blocks), std::move(P)).release();

    if (!insert_queue_.empty()) {
      alarm_timestamp() = td::Timestamp::in(0.1);
    } else {
      alarm_timestamp() = td::Timestamp::in(1.0);
    }
   }

  void insert(std::vector<BlockDataState> block_ds, td::Promise<td::Unit> promise) {
    insert_queue_.push(std::move(block_ds));
    promise_queue_.push(std::move(promise));
  }
};

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

class DbScanner: public td::actor::Actor {
private:
  td::actor::ActorOwn<ton::validator::ValidatorManagerInterface> validator_manager_;
  td::actor::ActorOwn<ton::validator::RootDb> db_;
  td::actor::ActorOwn<InsertManager> insert_manager_;

  std::string db_root_;
  
  std::queue<int> seqnos_to_process_;
  std::set<int> seqnos_in_progress_;
  int max_parallel_fetch_actors_{1};
  int last_known_seqno_{-1};

public:
  void set_db_root(std::string db_root) {
    db_root_ = db_root;
  }

  void set_last_known_seqno(int seqno) {
    last_known_seqno_ = seqno;
  }

  void start_up() override {
    alarm_timestamp() = td::Timestamp::in(3.0);
  }

  void run() {
    db_ = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_);
    insert_manager_ = td::actor::create_actor<InsertManager>("insertmanager");
  }

private:
  void update_last_mc_seqno() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<int> R) {
      R.ensure();
      td::actor::send_closure(SelfId, &DbScanner::set_last_mc_seqno, R.move_as_ok());
    });

    td::actor::send_closure(db_, &RootDb::get_max_masterchain_seqno, std::move(P));
  }

  void set_last_mc_seqno(int mc_seqno) {
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

  void catch_up_with_primary() {
    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      R.ensure();
    });
    td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
  }

  void schedule_for_processing() {
    while (!seqnos_to_process_.empty() && seqnos_in_progress_.size() < max_parallel_fetch_actors_) {
      auto mc_seqno = seqnos_to_process_.front();
      seqnos_to_process_.pop();

      auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<std::vector<BlockDataState>> res) {
        td::actor::send_closure(SelfId, &DbScanner::seqno_fetched, mc_seqno, std::move(res));
      });

      LOG(INFO) << "Creating IndexQuery for mc seqno " << mc_seqno;
      td::actor::create_actor<IndexQuery>("indexquery", mc_seqno, db_.get(), std::move(R)).release();
      seqnos_in_progress_.insert(mc_seqno);
    }
  }

  void seqno_fetched(int mc_seqno, td::Result<std::vector<BlockDataState>> blocks_data_state) {
    CHECK(seqnos_in_progress_.erase(mc_seqno) == 1);

    if (blocks_data_state.is_error()) {
      LOG(ERROR) << "mc_seqno " << mc_seqno << " failed to fetch BlockDataState: " << blocks_data_state.move_as_error();
      seqnos_to_process_.push(mc_seqno);
      return;
    }

    auto blks = blocks_data_state.move_as_ok();
    for (auto& blk: blks) {
      LOG(INFO) << "Got block data and state for " << blk.block_data->block_id().id.to_str();
    }

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<td::Unit> res) {
      if (res.is_ok()) {
        LOG(INFO) << "MC seqno " << mc_seqno << " insert success";
      } else {
        LOG(INFO) << "MC seqno " << mc_seqno << " insert failed: " << res.move_as_error();
      }
    });

    td::actor::send_closure(insert_manager_, &InsertManager::insert, std::move(blks), std::move(R));
  }

  void alarm() override {
    alarm_timestamp() = td::Timestamp::in(1.0);
    if (db_.empty()) {
      return;
    }

    td::actor::send_closure(actor_id(this), &DbScanner::update_last_mc_seqno);
    td::actor::send_closure(actor_id(this), &DbScanner::catch_up_with_primary);
    td::actor::send_closure(actor_id(this), &DbScanner::schedule_for_processing);
  }
};


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::OptionParser p;
  p.set_description("test db scanner");
  p.add_option('h', "help", "prints_help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });

  td::actor::ActorOwn<DbScanner> scanner;

  p.add_option('D', "db", "Path to TON DB folder",
               [&](td::Slice fname) { td::actor::send_closure(scanner, &DbScanner::set_db_root, fname.str()); });

  p.add_checked_option('F', "from", "Masterchain seqno to start indexing from",
               [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --from: not a number");
    }
    td::actor::send_closure(scanner, &DbScanner::set_last_known_seqno, v);
    return td::Status::OK();
  });

  td::actor::Scheduler scheduler({32});
  scheduler.run_in_context([&] { scanner = td::actor::create_actor<DbScanner>("scanner"); });
  scheduler.run_in_context([&] { p.run(argc, argv).ensure(); });
  scheduler.run_in_context([&] { td::actor::send_closure(scanner, &DbScanner::run); });
  scheduler.run();
}



