#include <iostream>
#include <queue>

#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "validator/manager-disk.h"
#include "validator/db/rootdb.hpp"

#include "crypto/vm/cp0.h"

using namespace ton::validator;

// class IndexQuery: public td::actor::Actor {
// private:
//   const int mc_seqno_;
//   td::actor::ActorId<ton::validator::RootDb> db_;
//   td::Promise<td::Ref<BlockData>> promise_;

// public:
//   IndexQuery(int mc_seqno, td::actor::ActorId<ton::validator::RootDb> db, td::Promise<td::Ref<BlockData>> &&promise) : 
//     db_(db), 
//     mc_seqno_(mc_seqno),
//     promise_(promise) {
//   }

//   void start_up() override {
//     auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ConstBlockHandle> R) {
//       td::actor::send_closure(SelfId, &IndexQuery::got_block_handle, std::move(R));
//     });

//     td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull(ton::masterchainId, ton::shardIdAll), mc_seqno_, std::move(P));
//   }

//   void got_block_handle(td::Result<ConstBlockHandle> handle) {
//     if (handle.is_error()) {
//       promise_.set_result(handle);
//       return;
//     }

//     auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<BlockData>> R) {
//       td::actor::send_closure(SelfId, &IndexQuery::got_block_data, std::move(R));
//     });

//     td::actor::send_closure(db_, &RootDb::get_block_data, handle.move_as_ok(), std::move(P));
//   }

//   void got_block_data(td::Result<td::Ref<BlockData>> block_data) {
//     promise_.set_result(std::move(block_data));
//   }
// };

class DbScanner: public td::actor::Actor {
private:
  td::actor::ActorOwn<ton::validator::ValidatorManagerInterface> validator_manager_;
  td::actor::ActorOwn<ton::validator::RootDb> db_;

  std::string db_root_;
  int bottom_seqno_{0};
  
  std::queue<int> seqnos_to_process_;
  std::set<int> seqnos_in_progress;
  int last_known_seqno_{0};

public:
  void set_db_root(std::string db_root) {
    db_root_ = db_root;
  }

  void set_bottom_seqno(int seqno) {
    bottom_seqno_ = seqno;
    last_known_seqno_ = seqno;
  }

  void start_up() override {
    alarm_timestamp() = td::Timestamp::in(3.0);
  }

  void run() {
    db_ = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_);
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
    for (int s = last_known_seqno_ + 1; s < mc_seqno; s++) {
      seqnos_to_process_.push(s);
    }
  }

  void catch_up_with_primary() {
    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      R.ensure();
    });
    td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
  }

  void schedule_for_processing() {
    // auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      
    // });
    // td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
  }

  void alarm() override {
    alarm_timestamp() = td::Timestamp::in(3.0);
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

  p.add_option('D', "db", "root for dbs",
               [&](td::Slice fname) { td::actor::send_closure(scanner, &DbScanner::set_db_root, fname.str()); });

  td::actor::Scheduler scheduler({1});
  scheduler.run_in_context([&] { scanner = td::actor::create_actor<DbScanner>("scanner"); });
  scheduler.run_in_context([&] { p.run(argc, argv).ensure(); });
  scheduler.run_in_context([&] { td::actor::send_closure(scanner, &DbScanner::run); });
  scheduler.run();
}



