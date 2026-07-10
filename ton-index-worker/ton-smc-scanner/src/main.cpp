#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/actor/actor.h"
#include "td/utils/port/path.h"
#include "td/utils/filesystem.h"
#include "crypto/vm/cp0.h"
#include "DbScanner.h"
#include "SmcScanner.h"

#include <atomic>
#include <chrono>
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
  kvrocks_state::KvrocksConfig kvrocks_config;
  Options options_;
  bool is_testnet = false;
  std::int32_t max_data_depth = 0;
  std::uint32_t max_insert_retries = 5;
  std::chrono::milliseconds insert_retry_initial_delay{500};
  std::chrono::milliseconds insert_retry_max_delay{5000};
  std::string jemalloc_profile_prefix = "/tmp/ton-smc-scanner.heap";
  std::string accounts_file;

  td::OptionParser p;
  p.set_description("Scan all accounts at some seqno, detect interfaces and save them to Postgres and/or Kvrocks");
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
  p.add_checked_option('\0', "max-parallel-batches", "Maximum parallel account range scanners per shard (default: 8)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --max-parallel-batches: not a number");
    }
    if (v <= 0) {
      return td::Status::Error("--max-parallel-batches must be positive");
    }
    options_.max_parallel_batches_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "reload-shard-state-every-batches",
                       "Reload shard-state roots and config every N completed batches (default: 64, 0 disables)",
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
  p.add_option('\0', "kvrocks", "Kvrocks direct Redis URI, e.g. tcp://127.0.0.1:6666/0", [&](td::Slice value) {
    kvrocks_config.enabled = true;
    kvrocks_config.uri = value.str();
  });
  p.add_checked_option('\0', "kvrocks-sentinels", "Comma-separated Kvrocks Sentinel nodes, e.g. 127.0.0.1:26379,127.0.0.1:26380", [&](td::Slice value) {
    try {
      kvrocks_config.enabled = true;
      kvrocks_config.sentinel_nodes = kvrocks_state::parse_kvrocks_sentinel_nodes(value.str());
    } catch (const std::exception& e) {
      return td::Status::Error(PSLICE() << "bad value for --kvrocks-sentinels: " << e.what());
    }
    return td::Status::OK();
  });
  p.add_option('\0', "kvrocks-master", "Kvrocks Sentinel master name", [&](td::Slice value) {
    kvrocks_config.sentinel_master_name = value.str();
  });
  p.add_option('\0', "kvrocks-user", "Kvrocks username", [&](td::Slice value) {
    kvrocks_config.user = value.str();
  });
  p.add_option('\0', "kvrocks-password", "Kvrocks password", [&](td::Slice value) {
    kvrocks_config.password = value.str();
  });
  p.add_checked_option('\0', "kvrocks-db", "Kvrocks logical database number", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-db: not a number");
    }
    if (v < 0) {
      return td::Status::Error("bad value for --kvrocks-db: must be non-negative");
    }
    kvrocks_config.db = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "kvrocks-pool-size", "Kvrocks connection pool size (default: 32)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-pool-size: not a number");
    }
    if (v <= 0) {
      return td::Status::Error("--kvrocks-pool-size must be positive");
    }
    kvrocks_config.pool_size = static_cast<std::size_t>(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "kvrocks-connect-timeout-ms", "Kvrocks connection timeout in milliseconds (default: 10000)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-connect-timeout-ms: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--kvrocks-connect-timeout-ms must be non-negative");
    }
    kvrocks_config.connect_timeout = std::chrono::milliseconds(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "kvrocks-socket-timeout-ms", "Kvrocks read/write timeout in milliseconds (default: 10000)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-socket-timeout-ms: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--kvrocks-socket-timeout-ms must be non-negative");
    }
    kvrocks_config.socket_timeout = std::chrono::milliseconds(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "kvrocks-pool-wait-timeout-ms", "Kvrocks connection pool wait timeout in milliseconds (default: 10000)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-pool-wait-timeout-ms: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--kvrocks-pool-wait-timeout-ms must be non-negative");
    }
    kvrocks_config.wait_timeout = std::chrono::milliseconds(v);
    return td::Status::OK();
  });
  p.add_option('\0', "kvrocks-sentinel-user", "Kvrocks Sentinel username", [&](td::Slice value) {
    kvrocks_config.sentinel_user = value.str();
  });
  p.add_option('\0', "kvrocks-sentinel-password", "Kvrocks Sentinel password", [&](td::Slice value) {
    kvrocks_config.sentinel_password = value.str();
  });
  p.add_checked_option('\0', "kvrocks-sentinel-retry-interval-ms", "Kvrocks Sentinel retry interval in milliseconds (default: 100)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-sentinel-retry-interval-ms: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--kvrocks-sentinel-retry-interval-ms must be non-negative");
    }
    kvrocks_config.sentinel_retry_interval = std::chrono::milliseconds(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "kvrocks-sentinel-max-retry", "Kvrocks Sentinel max retries (default: 2)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --kvrocks-sentinel-max-retry: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--kvrocks-sentinel-max-retry must be non-negative");
    }
    kvrocks_config.sentinel_max_retry = static_cast<std::size_t>(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-data-depth", "Max data cell depth to store in latest account states", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --max-data-depth: not a number");
    }
    max_data_depth = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "insert-retries", "Retries for transient insert errors (default: 5)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --insert-retries: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--insert-retries must be non-negative");
    }
    max_insert_retries = static_cast<std::uint32_t>(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "insert-retry-initial-delay-ms", "Initial delay before retrying transient insert errors (default: 500)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --insert-retry-initial-delay-ms: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--insert-retry-initial-delay-ms must be non-negative");
    }
    insert_retry_initial_delay = std::chrono::milliseconds(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "insert-retry-max-delay-ms", "Maximum delay before retrying transient insert errors (default: 5000)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error("bad value for --insert-retry-max-delay-ms: not a number");
    }
    if (v < 0) {
      return td::Status::Error("--insert-retry-max-delay-ms must be non-negative");
    }
    insert_retry_max_delay = std::chrono::milliseconds(v);
    return td::Status::OK();
  });
  p.add_option('W', "working-dir", "Path for working directory, for storing checkpoints", [&](td::Slice fname) {
    options_.working_dir_ = fname.str();
  });
  p.add_option('\0', "interfaces", "Detect interfaces", [&] {
    options_.index_interfaces_ = true;
  });
  p.add_option('\0', "account-states", "Index account states", [&] {
    options_.index_account_states_ = true;
  });
  p.add_option('\0', "accounts-file", "Path to a text file with raw-format addresses, one per line. "
                                      "When set, only these accounts are scanned instead of the full shard sweep. "
                                      "Blank lines, whitespace and lines starting with # are ignored", [&](td::Slice value) {
    accounts_file = value.str();
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

  if (kvrocks_config.enabled) {
    if (kvrocks_config.use_sentinel() && !kvrocks_config.uri.empty()) {
      LOG(ERROR) << "Use either --kvrocks or --kvrocks-sentinels, not both";
      std::exit(2);
    }
    if (kvrocks_config.use_sentinel() && kvrocks_config.sentinel_master_name.empty()) {
      LOG(ERROR) << "--kvrocks-master is required with --kvrocks-sentinels";
      std::exit(2);
    }
    if (kvrocks_config.use_sentinel() &&
        (kvrocks_config.connect_timeout == std::chrono::milliseconds(0) ||
         kvrocks_config.socket_timeout == std::chrono::milliseconds(0))) {
      LOG(ERROR) << "--kvrocks-connect-timeout-ms and --kvrocks-socket-timeout-ms must be positive with --kvrocks-sentinels";
      std::exit(2);
    }
  }

  if (pg_dsn.empty() && !kvrocks_config.enabled) {
    LOG(ERROR) << "You must specify at least one sink: --pg or --kvrocks/--kvrocks-sentinels";
    std::exit(2);
  }

  if (insert_retry_max_delay < insert_retry_initial_delay) {
    LOG(ERROR) << "--insert-retry-max-delay-ms must be greater than or equal to --insert-retry-initial-delay-ms";
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

  if (!accounts_file.empty()) {
    auto content_r = td::read_file(accounts_file);
    if (content_r.is_error()) {
      LOG(ERROR) << "Failed to read --accounts-file " << accounts_file << ": " << content_r.move_as_error();
      std::exit(2);
    }
    std::string content = content_r.move_as_ok().as_slice().str();

    auto addresses = std::make_shared<std::vector<block::StdAddress>>();
    std::vector<int> bad_lines;
    int line_no = 0;
    std::size_t pos = 0;
    while (pos <= content.size()) {
      auto nl = content.find('\n', pos);
      std::string line = (nl == std::string::npos) ? content.substr(pos) : content.substr(pos, nl - pos);
      pos = (nl == std::string::npos) ? content.size() + 1 : nl + 1;
      ++line_no;

      auto begin = line.find_first_not_of(" \t\r\n");
      if (begin == std::string::npos) {
        continue;
      }
      auto end = line.find_last_not_of(" \t\r\n");
      std::string trimmed = line.substr(begin, end - begin + 1);
      if (trimmed[0] == '#') {
        continue;
      }

      auto addr_r = block::StdAddress::parse(trimmed);
      if (addr_r.is_error()) {
        bad_lines.push_back(line_no);
        continue;
      }
      addresses->push_back(addr_r.move_as_ok());
    }

    if (!bad_lines.empty()) {
      std::string bad_list;
      for (std::size_t i = 0; i < bad_lines.size(); ++i) {
        if (i != 0) {
          bad_list += ", ";
        }
        bad_list += std::to_string(bad_lines[i]);
      }
      LOG(ERROR) << "--accounts-file " << accounts_file << " has unparseable address(es) on line(s): " << bad_list;
      std::exit(2);
    }
    if (addresses->empty()) {
      LOG(ERROR) << "--accounts-file " << accounts_file << " contains no addresses";
      std::exit(2);
    }
    LOG(INFO) << "Account-list mode: loaded " << addresses->size() << " addresses from " << accounts_file;
    options_.account_addresses_ = std::move(addresses);
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
  //   td::actor::SchedulerContext::get().stop();
  // });
  scheduler.run_in_context([&] { 
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_readonly);
    ScannerInsertConfig insert_config;
    insert_config.postgres_connection_string = pg_dsn;
    insert_config.postgres_enabled = !pg_dsn.empty();
    insert_config.kvrocks_config = kvrocks_config;
    insert_config.source_mc_seqno = options_.seqno_;
    insert_config.max_data_depth = max_data_depth;
    insert_config.max_insert_retries = max_insert_retries;
    insert_config.insert_retry_initial_delay = insert_retry_initial_delay;
    insert_config.insert_retry_max_delay = insert_retry_max_delay;
    insert_manager = td::actor::create_actor<PostgreSQLInsertManager>("insert_manager", insert_config, options_.batch_size_);
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
