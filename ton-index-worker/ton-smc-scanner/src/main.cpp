#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/actor/actor.h"
#include "td/utils/port/path.h"
#include "crypto/vm/cp0.h"
#include "DbScanner.h"
#include "SmcScanner.h"

#include <atomic>
#include <string>
#include <unistd.h>

#if TON_USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace {

#if TON_USE_JEMALLOC
std::atomic<bool> need_jemalloc_profile_dump{false};

void request_jemalloc_profile_dump(int) {
  need_jemalloc_profile_dump.store(true);
}

bool is_jemalloc_profiling_enabled() {
  bool enabled = false;
  size_t size = sizeof(enabled);
  return mallctl("opt.prof", &enabled, &size, nullptr, 0) == 0 && enabled;
}

void dump_jemalloc_profile(const std::string &prefix) {
  std::string filename = prefix;
  filename += ".";
  filename += std::to_string(static_cast<std::uint64_t>(td::Clocks::system()));
  filename += ".";
  filename += std::to_string(static_cast<std::uint64_t>(::getpid()));
  filename += ".heap";

  const char *filename_cstr = filename.c_str();
  if (mallctl("prof.dump", nullptr, nullptr, &filename_cstr, sizeof(filename_cstr)) == 0) {
    LOG(WARNING) << "Written jemalloc heap profile to " << filename;
  } else {
    LOG(ERROR) << "Failed to write jemalloc heap profile to " << filename
               << ". Start ton-smc-scanner with MALLOC_CONF=prof:true,...";
  }
}
#else
void request_jemalloc_profile_dump(int) {
}

bool is_jemalloc_profiling_enabled() {
  return false;
}

void dump_jemalloc_profile(const std::string &) {
}
#endif

}  // namespace

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::uint32 threads = 7;
  std::string db_root;
  std::string pg_dsn;
  Options options_;
  bool is_testnet = false;
  std::string jemalloc_profile_prefix = "/tmp/ton-smc-scanner.heap";
  
  td::OptionParser p;
  p.set_description("Scan all accounts at some seqno, detect interfaces and save them to postgres");
  p.add_option('\0', "help", "prints_help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_checked_option('S', "seqno", "Masterchain seqno on which moment all account states will be scanned", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error("bad value for --seqno: not a number");
    }
    options_.seqno_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('t', "threads", "Scheduler threads (default: 7)", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error("bad value for --threads: not a number");
    }
    threads = v;
    return td::Status::OK();
  });
  p.add_checked_option('b', "batch-size", "Insert batch size", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error("bad value for --batch-size: not a number");
    }
    options_.batch_size_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "reload-shard-state-every-batches",
                       "Reload shard-state roots and config every N completed batches (default: 16, 0 disables)",
                       [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --reload-shard-state-every-batches: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--reload-shard-state-every-batches must be non-negative");
    }
    options_.reload_shard_state_every_batches_ = static_cast<std::uint32_t>(v);
    return td::Status::OK();
  });
  p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice fname) { 
    db_root = fname.str();
  });
  p.add_option('d', "pg", "PostgreSQL connection string", [&](td::Slice value) {
    pg_dsn = value.str();
  });
  p.add_option('W', "working-dir", "Path for working directory, for storing checkpoints", [&](td::Slice fname) {
    options_.working_dir_ = fname.str();
  });
  p.add_option('\0', "interfaces", "Detect interfaces", [&] {
    options_.index_interfaces_ = true;
  });
  p.add_option('\0', "account-states", "Detect interfaces", [&] {
    options_.index_account_states_ = true;
  });
  p.add_option('\0', "testnet", "Use for testnet. It is used for correct indexing of .ton DNS entries (in testnet .ton collection has a different address)", [&]() {
    is_testnet = true;
  });
  p.add_option('\0', "jemalloc-prof-prefix", "Path prefix for jemalloc heap dumps (default: /tmp/ton-smc-scanner.heap)", [&](td::Slice value) {
    jemalloc_profile_prefix = value.str();
  });

  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }

  if (db_root.empty()) {
    LOG(ERROR) << "You must specify --db option";
    std::exit(2);
  }

  if (!options_.index_account_states_ && !options_.index_interfaces_) {
    LOG(ERROR) << "You must specify at least one of --interfaces or --account-states options";
    std::exit(2);
  }

  if (options_.seqno_ == 0) {
    LOG(ERROR) << "You must specify --seqno option";
    std::exit(2);
  }

  if (options_.working_dir_.empty()) {
    LOG(WARNING) << "You did not specify --working-dir option, checkpoints will not be saved and the process will start from scratch in case of crash";
  } else {
    auto S = td::mkdir(options_.working_dir_);
    if (S.is_error()) {
      LOG(ERROR) << "Failed to create working directory " << options_.working_dir_ << ": " << S.move_as_error();
      std::exit(2);
    }
  }

  NftItemDetectorR::is_testnet = is_testnet;

#if TON_USE_JEMALLOC
  td::set_signal_handler(td::SignalType::User, request_jemalloc_profile_dump).ensure();
  LOG(INFO) << "Jemalloc heap dumps: send SIGUSR1 or SIGUSR2 to pid " << ::getpid()
            << " to write a profile with prefix " << jemalloc_profile_prefix;
  if (!is_jemalloc_profiling_enabled()) {
    LOG(WARNING) << "Jemalloc heap profiling is disabled. Start ton-smc-scanner with MALLOC_CONF="
                 << "\"prof:true,prof_active:true,prof_prefix:" << jemalloc_profile_prefix
                 << ",lg_prof_sample:19\" to enable heap dumps";
  }
#endif

  td::actor::Scheduler scheduler({threads});
  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<PostgreSQLInsertManager> insert_manager;

  // auto watcher = td::create_shared_destructor([] {
  //   td::actor::SchedulerContext::get()->stop();
  // });
  scheduler.run_in_context([&] { 
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_readonly);
    insert_manager = td::actor::create_actor<PostgreSQLInsertManager>("insert_manager", pg_dsn, options_.batch_size_);
    options_.insert_manager_ = insert_manager.get();
    td::actor::create_actor<SmcScanner>("smcscanner", db_scanner.get(), options_).release();
  });

  while (scheduler.run(1)) {
#if TON_USE_JEMALLOC
    if (need_jemalloc_profile_dump.exchange(false)) {
      dump_jemalloc_profile(jemalloc_profile_prefix);
    }
#endif
  }
  LOG(INFO) << "TON DB integrity check finished successfully";
  return 0;
}
