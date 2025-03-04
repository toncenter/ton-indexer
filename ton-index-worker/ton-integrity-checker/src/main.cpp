#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

// #include "InsertManagerPostgres.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "EventProcessor.h"
#include "IntegrityChecker.h"


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  // options
  td::uint32 threads = 7;
  td::int32 stats_timeout = 10;
  std::string db_root;
  td::uint32 last_known_seqno = 0;
  std::string checkpoint_path;

  std::uint32_t max_active_tasks = 7;
  size_t min_free_memory = 10;
  
  
  td::OptionParser p;
  p.set_description("Parse TON DB and insert data into Postgres");
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
  
  p.add_checked_option('f', "from", "Masterchain seqno to start indexing from", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --from: not a number");
    }
    last_known_seqno = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-active-tasks", "Max active reading tasks", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-active-tasks: not a number");
    }
    max_active_tasks = v;
    return td::Status::OK();
  });

  p.add_checked_option('\0', "min-free-memory", "Minimum percentage of free RAM left (integer). When less RAM is left the program will exit with code 100.", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-active-tasks: not a number");
    }
    min_free_memory = v;
    return td::Status::OK();
  });
  
  p.add_checked_option('\0', "stats-freq", "Pause between printing stats in seconds", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --threads: not a number");
    }
    stats_timeout = v;
    return td::Status::OK();
  });

  p.add_option('\0', "checkpoint", "Path to checkpoint file, used for saving progress.", [&](td::Slice arg) {
    checkpoint_path = arg.str();
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

  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }

  td::actor::Scheduler scheduler({threads});

  td::actor::ActorOwn<IntegrityParser> parse_manager;
  td::actor::ActorOwn<DbScanner> db_scanner;

  auto watcher = td::create_shared_destructor([] {
    td::actor::SchedulerContext::get()->stop();
  });

  scheduler.run_in_context([&, watcher = std::move(watcher)] { 
    parse_manager = td::actor::create_actor<IntegrityParser>("parsemanager");
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_readonly);
    td::actor::create_actor<IntegrityChecker>("integritychecker", 
                          db_scanner.get(), parse_manager.get(), checkpoint_path, max_active_tasks, max_active_tasks, stats_timeout, min_free_memory, watcher).release();
  });
  
  scheduler.run();

  LOG(INFO) << "TON DB integrity check finished successfully";
  
  return 0;
}
