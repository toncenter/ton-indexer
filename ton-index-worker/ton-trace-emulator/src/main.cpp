#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "DbScanner.h"
#include "TraceScheduler.h"
#include "TraceProcessor.h"
#include "RedisMaterializer.h"
#include "StatsRecorder.h"
#include "Statistics.h"
#include "GenMatchers.h"
#include "emu/EmuClassifierBridge.h"

#include <algorithm>
#include <cmath>

namespace {

td::Status parse_positive_seconds(td::Slice value,
                                  const char* option,
                                  double& destination) {
  try {
    destination = std::stod(value.str());
  } catch (...) {
    return td::Status::Error(ton::ErrorCode::error,
                             std::string("bad value for --") + option + ": not a number");
  }
  if (!std::isfinite(destination) || destination <= 0) {
    return td::Status::Error(ton::ErrorCode::error,
                             std::string("bad value for --") + option + ": must be positive");
  }
  return td::Status::OK();
}

}  // namespace


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  // options
  std::string db_root;
  std::string working_dir;
  td::uint32 threads = 7;
  std::string redis_dsn = "tcp://127.0.0.1:6379";
  std::string redis_channel = "";
  TraceRetentionConfig trace_retention;
  
  std::string global_config_path;
  std::string inet_addr;
  std::string db_event_fifo_path;
  bool mch_disable = false;
  bool mch_no_tier2 = false;
  int mch_workers = 1;
  double actor_stats_interval = 30;
  
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
  p.add_option('\0', "testnet", "Use for testnet. It is used for correct detecting of .ton DNS entries (in testnet .ton collection has a different address)", [&]() {
    NftItemDetectorR::is_testnet = true;
  });

  p.add_checked_option('\0', "actor-stats-interval",
                       "Save stats and actor stats together every N seconds (N >= 1) in working-dir/stats; keep 500 snapshots "
                       "(default: 30; 0 disables both stats and actor stats)",
                       [&](td::Slice value) {
    TRY_RESULT(interval, parse_actor_stats_interval(value));
    actor_stats_interval = interval;
    return td::Status::OK();
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

  p.add_option('\0', "redis",
               "Redis URI; the selected database is cleared on startup "
               "(default: 'tcp://127.0.0.1:6379')",
               [&](td::Slice fname) {
    redis_dsn = fname.str();
  });

  p.add_option('\0', "redis-channel", "Redis channel name for input msgs", [&](td::Slice fname) { 
    redis_channel = fname.str();
  });

  p.add_checked_option('\0', "trace-root-pending-ttl",
                       "Seconds to retain a trace whose canonical root is pending (default: 30)",
                       [&](td::Slice value) {
    return parse_positive_seconds(
        value, "trace-root-pending-ttl", trace_retention.root_pending_seconds);
  });

  p.add_checked_option(
      '\0',
      "trace-root-replaced-confirmed-ttl",
      "Seconds to wait for a confirmed root replaced by a finalized fork "
      "(default: 30)",
      [&](td::Slice value) {
        return parse_positive_seconds(
            value,
            "trace-root-replaced-confirmed-ttl",
            trace_retention.root_replaced_confirmed_seconds);
      });

  p.add_checked_option('\0', "trace-open-ttl",
                       "Seconds to retain a real trace with a pending tail (default: 300)",
                       [&](td::Slice value) {
    return parse_positive_seconds(value, "trace-open-ttl", trace_retention.open_seconds);
  });

  p.add_checked_option('\0', "trace-completed-ttl",
                       "Seconds to retain a completed trace in Redis (default: 30)",
                       [&](td::Slice value) {
    return parse_positive_seconds(
        value, "trace-completed-ttl", trace_retention.completed_seconds);
  });

  p.add_option('\0', "global-config", "Path to global config json file (for listening overlay)", [&](td::Slice fname) { 
    global_config_path = fname.str();
  });

  p.add_option('\0', "addr", "ip:port of this machine (for listening overlay)", [&](td::Slice fname) { 
    inet_addr = fname.str();
  });

  p.add_option('\0', "db-event-fifo", "Path to FIFO pipe for DB events", [&](td::Slice fname) { 
    db_event_fifo_path = fname.str();
  });

  // This option bypasses in-process classification and inserts empty payloads.
  p.add_option('\0', "mch-disable", "Disable in-process MCH classification", [&]() {
    mch_disable = true;
  });

  // Tier-2 lookups are enabled by default.
  p.add_option('\0', "mch-no-tier2", "Disable celldb tier-2 lookups (tier-1-only classification)", [&]() {
    mch_no_tier2 = true;
  });

  p.add_checked_option('\0', "mch-workers", "MCH classifier workers (default: 1)", [&](td::Slice value) {
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --mch-workers: not a number");
    }
    mch_workers = std::clamp(v, 1, 64);
    return td::Status::OK();
  });


  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }

  if (db_root.size() == 0) {
    std::cerr << "'--db' option missing" << std::endl;
    std::_Exit(2);
  }

  if (working_dir.size() == 0) {
    working_dir = PSTRING() << "/tmp/index_worker_" << getpid();
    LOG(WARNING) << "Working dir not specified, using " << working_dir;
  }

  if (global_config_path.empty() ^ inet_addr.empty()) {
    std::cerr << "'--global-config' must be present with '--addr'" << std::endl;
    std::_Exit(2);
  }

  mch::EmuClassifierConfig mch_classifier_config;
  mch_classifier_config.workers = mch_workers;
  if (mch_disable) {
    LOG(WARNING) << "MCH classification DISABLED (--mch-disable): traces are inserted unclassified";
  } else {
    // Compiled matcher-table preparation must succeed when classification is enabled.
    auto r_prep = mch::make_engine_prep();
    if (r_prep.is_error()) {
      LOG(FATAL) << "MCH engine prep failed: " << r_prep.move_as_error();
    }
    mch_classifier_config.prep = r_prep.move_as_ok();
    mch_classifier_config.tier2 = !mch_no_tier2;
    LOG(INFO) << "MCH classification ENABLED (artifact sha " << mch::gen_matchers_ir_source_sha()
              << "), inserts wait for classification, celldb tier-2 "
              << (mch_classifier_config.tier2 ? "ON" : "OFF")
              << " workers=" << mch_classifier_config.workers;
  }

  // Resolve once before the scheduler starts. Redis reconnects must not block
  // actor workers in getaddrinfo(). Validate before the startup FLUSHDB too.
  auto redis_options = parse_redis_connection_options(redis_dsn);
  if (redis_options.is_error()) {
    LOG(ERROR) << redis_options.move_as_error();
    return 1;
  }
  // Keep the writer alive until after scheduler destruction, so its file I/O
  // and final drain never block an actor worker during shutdown.
  std::shared_ptr<StatsFileWriter> stats_writer;
  if (actor_stats_interval > 0) {
    auto writer = StatsFileWriter::create(working_dir);
    if (writer.is_error()) {
      LOG(ERROR) << writer.move_as_error();
      return 1;
    }
    stats_writer = writer.move_as_ok();
    td::actor::set_debug(true);
    LOG(INFO) << "Stats enabled: interval=" << actor_stats_interval
              << "s, directory=" << working_dir << "/stats, max_snapshots=" << StatsSnapshotStore::kMaxSnapshots;
  } else {
    LOG(INFO) << "Stats and actor stats disabled";
  }

  // This must happen before any actor can subscribe to events or write a trace.
  LOG(WARNING) << "Clearing pending Redis database before startup";
  auto flush_status = flush_pending_redis_database(redis_dsn);
  if (flush_status.is_error()) {
    LOG(ERROR) << flush_status.move_as_error();
    return 1;
  }
  LOG(INFO) << "Pending Redis database cleared";

  td::actor::Scheduler scheduler({threads});
  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<ITraceProcessor> trace_processor;

  scheduler.run_in_context([&] { 
    if (stats_writer) {
      td::actor::create_actor<StatsRecorder>("StatsRecorder", actor_stats_interval, true, stats_writer,
                                            [] { return g_statistics.generate_report_and_reset(); }).release();
    }
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, working_dir, 0.05f);
    trace_processor = td::actor::create_actor<TraceProcessor>(
        "TraceProcessor", redis_options.move_as_ok(), trace_retention,
        mch_classifier_config);
    td::actor::create_actor<TraceEmulatorScheduler>("integritychecker", db_scanner.get(), trace_processor.get(),
      global_config_path, inet_addr, redis_dsn, redis_channel,
      db_event_fifo_path).release();
  });
  
  scheduler.run();

  return 0;
}
