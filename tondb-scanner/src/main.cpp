#include <iostream>

#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "validator/manager-disk.h"

#include "crypto/vm/cp0.h"

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

  td::Status create_validator_options() {
    opts_ = ton::validator::ValidatorManagerOptions::create(
      ton::BlockIdExt{ton::masterchainId, ton::shardIdAll, 0, ton::RootHash::zero(), ton::FileHash::zero()},
      ton::BlockIdExt{ton::masterchainId, ton::shardIdAll, 0, ton::RootHash::zero(), ton::FileHash::zero()});
    return td::Status::OK();
  }

  void run() {
    std::cout << "Running!" << std::endl;

    auto top_block_str = td::Slice("(-1,8000000000000000,0):B0495C92E8472EBD35F6278C1F6F89D46ADB361861BEB2F80379189CA1C5648A:2E6E0C7CA165389C4759C0331F24C7C47898DDFE62AA3CB1F74CA1E27B610D1C");
    bool status = block::parse_block_id_ext(top_block_str, shard_top_block_id_);
    if (!status) {
      std::cout << "ERROR parsing block" << std::endl;
    }

    auto Sr = create_validator_options();

    auto opts = opts_;
    opts.write().set_initial_sync_disabled(false);
    validator_manager_ = ton::validator::ValidatorManagerDiskFactory::create(ton::PublicKeyHash::zero(), opts, shard_,
                                                                             shard_top_block_id_, db_root_);
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



