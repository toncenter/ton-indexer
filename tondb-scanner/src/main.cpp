#include <iostream>

#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "validator/manager-disk.h"
#include "validator/db/rootdb.hpp"

#include "crypto/vm/cp0.h"

using namespace ton::validator;

class DbScanner: public td::actor::Actor {
private:
  td::actor::ActorOwn<ton::validator::ValidatorManagerInterface> validator_manager_;

  std::string db_root_;
  td::actor::ActorOwn<ton::validator::RootDb> db_;

public:

  void set_db_root(std::string db_root) {
    db_root_ = db_root;
  }

  void start_up() override {
    alarm_timestamp() = td::Timestamp::in(3.0);
  }

  void run() {
    std::cout << "Running!" << std::endl;
    
    db_ = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_);

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ConstBlockHandle> R) {
      if (R.is_error()) {
        LOG(ERROR) << R.move_as_error();
      } else {
        auto k = R.move_as_ok();
        LOG(ERROR) << "GOT HANDLE";
      }
    });

    td::actor::send_closure(db_, &RootDb::get_block_by_seqno, ton::AccountIdPrefixFull(ton::masterchainId, ton::shardIdAll), 7139009, std::move(P));
  }

  void alarm() override {
    alarm_timestamp() = td::Timestamp::in(3.0);
    if (db_.empty()) {
      return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<int> R) {
      if (R.is_error()) {
        LOG(ERROR) << R.move_as_error();
      } else {
        auto k = R.move_as_ok();
        LOG(INFO) << "Max masterchain seqno: " << k;
      }
    });

    td::actor::send_closure(db_, &RootDb::get_max_masterchain_seqno, std::move(P));

    auto R = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        LOG(ERROR) << R.move_as_error();
      } else {
        LOG(INFO) << "Catch up with primary successful";
      }
    });
    td::actor::send_closure(db_, &RootDb::try_catch_up_with_primary, std::move(R));
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



