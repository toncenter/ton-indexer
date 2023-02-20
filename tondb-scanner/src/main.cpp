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

  std::string db_root_ = "/zpool/ton-work-testnet/db";
  td::Ref<ton::validator::ValidatorManagerOptions> opts_;
  ton::BlockIdExt shard_top_block_id_;
  ton::ShardIdFull shard_{ton::masterchainId, ton::shardIdAll};

public:
  void set_db_root(std::string db_root) {
    db_root_ = db_root;
  }

  // td::Status create_validator_options() {
  //   opts_ = ton::validator::ValidatorManagerOptions::create(
  //     ton::BlockIdExt{ton::masterchainId, ton::shardIdAll, 0, ton::RootHash::zero(), ton::FileHash::zero()},
  //     ton::BlockIdExt{ton::masterchainId, ton::shardIdAll, 0, ton::RootHash::zero(), ton::FileHash::zero()});
  //   return td::Status::OK();
  // }

  void run() {
    std::cout << "Running!" << std::endl;

    auto top_block_str = td::Slice("(-1,8000000000000000,7002925):AB657E913624D4C1E3316E6CC0BBCB24CEC6868F7532314B99EE9C7DE5438A45:F2788947E2A453863BC213ACAE48DA7904C8F2610731B5BB25194494A6C09FE2");
    bool status = block::parse_block_id_ext(top_block_str, shard_top_block_id_);
    if (!status) {
      std::cout << "ERROR parsing block" << std::endl;
    }
    
    auto db = td::actor::create_actor<ton::validator::RootDb>("db", td::actor::ActorId<ton::validator::ValidatorManager>(), db_root_);

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<BlockHandle> R) {
      if (R.is_error()) {
        LOG(ERROR) << R.move_as_error();
      } else {
        auto k = R.move_as_ok();
        LOG(ERROR) << "GOT HANDLE";
      }
    });

    td::actor::send_closure(db, &RootDb::get_block_handle, shard_top_block_id_, std::move(P));
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
  // p.add_option('C', "config", "global config path",
  //              [&](td::Slice fname) { td::actor::send_closure(x, [&](std::string fname) { std::cout << fname << std::endl; }, fname.str()); });
  
  td::actor::Scheduler scheduler({1});
  scheduler.run_in_context([&] { scanner = td::actor::create_actor<DbScanner>("scanner"); });
  scheduler.run_in_context([&] { p.run(argc, argv).ensure(); });
  scheduler.run_in_context([&] { td::actor::send_closure(scanner, &DbScanner::run); });
  scheduler.run();
}



