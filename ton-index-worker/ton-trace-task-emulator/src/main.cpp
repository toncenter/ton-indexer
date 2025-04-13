#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "DbScanner.h"
#include "TraceTaskScheduler.h"
#include "TaskResultInserter.h"


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  // options
  std::string db_root;
  std::string working_dir;
  td::uint32 threads = 7;
  std::string redis_dsn = "tcp://127.0.0.1:6379";
  std::string redis_queue_name = "emulatorqueue";
  
  td::OptionParser p;
  p.set_description("Emulate TON traces");
  p.add_option('\0', "help", "prints_help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice fname) { 
    db_root = fname.str();
  });
  p.add_option('W', "working-dir", "Path to index working dir for secondary rocksdb logs", [&](td::Slice fname) { 
    working_dir = fname.str();
  });

  p.add_checked_option('t', "threads", "Scheduler threads (default: 7)", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --threads: not a number");
    }
    threads = v;
    return td::Status::OK();
  });

  p.add_option('\0', "redis", "Redis URI (default: 'tcp://127.0.0.1:6379')", [&](td::Slice fname) { 
    redis_dsn = fname.str();
  });

  p.add_option('\0', "redis-queue", "Redis queue name for input msgs", [&](td::Slice fname) { 
    redis_queue_name = fname.str();
  });

  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }

  if (db_root.size() == 0) {
    LOG(ERROR) << "'--db' option missing";
    std::_Exit(2);
  }

  if (working_dir.size() == 0) {
    working_dir = PSTRING() << "/tmp/index_worker_" << getpid();
    LOG(WARNING) << "Working dir not specified, using " << working_dir;
  }
  if (redis_queue_name.size() == 0) {
    LOG(ERROR) << "'--redis-queue' option missing";
    std::_Exit(2);
  }
  if (redis_dsn.size() == 0) {
    LOG(ERROR) << "'--redis' option missing";
    std::_Exit(2);
  }

  td::actor::Scheduler scheduler({threads});
  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<ITaskResultInserter> insert_manager;

  scheduler.run_in_context([&] { 
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, working_dir, 0.5);
    insert_manager = td::actor::create_actor<RedisTaskResultInsertManager>("OnDemandRedisInsertManager", redis_dsn);
    td::actor::create_actor<TraceTaskScheduler>("integritychecker", db_scanner.get(), insert_manager.get(), redis_dsn, redis_queue_name, working_dir).release();
  });
  
  scheduler.run();

  return 0;
}
