#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/utils/misc.h"

#include "crypto/vm/cp0.h"

#include "DbScanner.h"
#include "FinalizedTraceScheduler.h"
#include "TraceProcessor.h"
#include "RedisMaterializer.h"
#include "StatsRecorder.h"
#include "Statistics.h"
#include "emu/EmuClassifierBridge.h"

#include <algorithm>
#include <cmath>

namespace {

td::Status parse_finalized_seqno(td::Slice text, ton::BlockSeqno& target) {
  auto value = td::to_integer_safe<std::uint32_t>(text);
  if (value.is_error() || value.ok() == 0) return td::Status::Error("seqno must be a positive uint32");
  target = value.move_as_ok();
  return td::Status::OK();
}

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
  TraceRetentionConfig trace_retention;

  std::string db_event_fifo_path;
  int mch_workers = 1;
  double actor_stats_interval = 30;
  ton::BlockSeqno from_seqno = 0, to_seqno = 0;

  td::OptionParser p;
  p.set_description("Stream finalized TON traces without emulation (active-active by mc seqno)");
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
  p.add_option('W', "working-dir", "Working directory for secondary DB and statistics", [&](td::Slice fname) {
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
    if (v < 1) return td::Status::Error("--threads must be positive");
    threads = v;
    return td::Status::OK();
  });

  p.add_option('\0', "redis",
               "Redis URI; use a dedicated DB shared only by finalized producers (never flushed)",
               [&](td::Slice fname) {
    redis_dsn = fname.str();
  });

  p.add_checked_option('\0', "trace-completed-ttl",
                       "Replay TTL and completed trace retention in seconds (default: 30)",
                       [&](td::Slice value) {
    return parse_positive_seconds(
        value, "trace-completed-ttl", trace_retention.completed_seconds);
  });

  p.add_option('\0', "db-event-fifo", "TON node DB events FIFO (optional; polling fallback)", [&](td::Slice fname) {
    db_event_fifo_path = fname.str();
  });

  p.add_checked_option('\0', "from-mc-seqno", "Bootstrap block (default: current node head on first initialization)",
                       [&](td::Slice value) { return parse_finalized_seqno(value, from_seqno); });
  p.add_checked_option('\0', "to-mc-seqno", "Stop after processing this block (for replay and validation)",
                       [&](td::Slice value) { return parse_finalized_seqno(value, to_seqno); });

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

  if (to_seqno && from_seqno && to_seqno < from_seqno) {
    std::cerr << "--to-mc-seqno precedes --from-mc-seqno" << std::endl;
    return 2;
  }
  if (trace_retention.completed_seconds < 1 || trace_retention.completed_seconds > 86400) {
    std::cerr << "Finalized replay TTL must be between 1 and 86400 seconds" << std::endl;
    return 2;
  }
  mch::EmuClassifierConfig mch_classifier_config;
  mch_classifier_config.workers = mch_workers;
  auto prep = mch::make_engine_prep();
  if (prep.is_error()) {
    LOG(ERROR) << "MCH engine prep failed: " << prep.move_as_error();
    return 1;
  }
  mch_classifier_config.prep = prep.move_as_ok();
  mch_classifier_config.tier2 = true;

  // Resolve once before the scheduler starts. Redis reconnects must not block
  // actor workers in getaddrinfo().
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

  td::actor::Scheduler scheduler({threads});
  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<TraceProcessor> trace_processor;

  scheduler.run_in_context([&] {
    if (stats_writer) {
      td::actor::create_actor<StatsRecorder>("StatsRecorder", actor_stats_interval, true, stats_writer,
                                            [] { return g_statistics.generate_report_and_reset(); }).release();
    }
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, working_dir, 0.05f);
    trace_processor = td::actor::create_actor<TraceProcessor>(
        "TraceProcessor", redis_options.ok(), trace_retention, mch_classifier_config, true);
    td::actor::create_actor<FinalizedTraceScheduler>("FinalizedTraceScheduler", db_scanner.get(), trace_processor.get(),
        redis_options.move_as_ok(), from_seqno, to_seqno, db_event_fifo_path).release();
  });

  scheduler.run();

  return 0;
}
