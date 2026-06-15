#include "BenchmarkInsertManager.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "Statistics.h"
#include "../../index-engine/src/IndexScheduler.h"

#include "crypto/vm/cp0.h"
#include "td/utils/OptionParser.h"
#include "td/utils/check.h"
#include "td/utils/filesystem.h"
#include "td/utils/logging.h"
#include "td/utils/port/path.h"
#include "td/utils/port/signals.h"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>

namespace {

td::Status parse_int32_option(td::Slice value, const char* name, std::int32_t& target, bool allow_zero = true) {
  std::int32_t parsed_value;
  try {
    parsed_value = std::stoi(value.str());
  } catch (...) {
    return td::Status::Error(ton::ErrorCode::error, PSLICE() << "bad value for --" << name << ": not a number");
  }
  if ((!allow_zero && parsed_value <= 0) || (allow_zero && parsed_value < 0)) {
    return td::Status::Error(ton::ErrorCode::error,
                             PSLICE() << "bad value for --" << name
                                      << (allow_zero ? ": must be non-negative" : ": must be positive"));
  }
  target = parsed_value;
  return td::Status::OK();
}

td::Status parse_uint32_option(td::Slice value, const char* name, std::uint32_t& target, bool allow_zero = true) {
  std::uint64_t parsed_value;
  std::size_t parsed = 0;
  auto raw = value.str();
  if (raw.empty() || raw[0] == '-') {
    return td::Status::Error(ton::ErrorCode::error,
                             PSLICE() << "bad value for --" << name
                                      << (allow_zero ? ": must be non-negative" : ": must be positive"));
  }
  try {
    parsed_value = std::stoull(raw, &parsed);
  } catch (...) {
    return td::Status::Error(ton::ErrorCode::error, PSLICE() << "bad value for --" << name << ": not a number");
  }
  if (parsed != raw.size()) {
    return td::Status::Error(ton::ErrorCode::error, PSLICE() << "bad value for --" << name << ": not a number");
  }
  if ((!allow_zero && parsed_value == 0) || parsed_value > std::numeric_limits<std::uint32_t>::max()) {
    return td::Status::Error(ton::ErrorCode::error,
                             PSLICE() << "bad value for --" << name
                                      << (allow_zero ? ": must fit uint32" : ": must be between 1 and uint32 max"));
  }
  target = static_cast<std::uint32_t>(parsed_value);
  return td::Status::OK();
}

void print_help_and_exit(td::OptionParser& parser) {
  char buffer[12000];
  td::StringBuilder sb(td::MutableSlice{buffer, sizeof(buffer)});
  sb << parser;
  std::cout << sb.as_cslice().c_str();
  std::cout.flush();
  std::exit(2);
}

}  // namespace

int main(int argc, char* argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();
  CHECK(vm::init_op_cp0());

  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<ParseManager> parse_manager;
  td::actor::ActorOwn<BenchmarkInsertManager> insert_manager;
  td::actor::ActorOwn<IndexScheduler> index_scheduler;

  std::string db_root;
  std::string working_dir;
  std::uint32_t from_seqno = 0;
  std::uint32_t to_seqno = 0;
  std::uint32_t prewarm_count = 50;
  std::uint32_t threads = 7;
  std::uint32_t io_workers = 1;
  std::uint32_t max_active_tasks = 7;
  std::uint32_t max_insert_actors = 4;
  std::int32_t stats_timeout = 10;
  std::int32_t max_data_depth = 0;
  std::int32_t latest_states_prepare_parallelism = 4;
  std::int32_t latest_states_prepare_chunk_size = 128;
  std::int32_t max_queue_size = -1;
  std::int32_t max_batch_size = -1;
  bool testnet = false;

  QueueState max_queue{10000, 100000, 100000, 100000, 100000};
  QueueState batch_size{2000, 2000, 10000, 10000, 10000};

  td::OptionParser parser;
  parser.set_description("Benchmark TON DB CellDb reads and latest account state BOC serialization without writes");
  parser.add_option('\0', "help", "prints help", [&]() {
    print_help_and_exit(parser);
  });
  parser.add_option('D', "db", "Path to TON DB folder", [&](td::Slice value) {
    db_root = value.str();
  });
  parser.add_option('W', "working-dir", "Path for secondary RocksDB logs and benchmark output", [&](td::Slice value) {
    working_dir = value.str();
  });
  parser.add_checked_option('f', "from", "First masterchain seqno to benchmark", [&](td::Slice value) {
    return parse_uint32_option(value, "from", from_seqno, false);
  });
  parser.add_checked_option('\0', "to", "Last masterchain seqno to benchmark", [&](td::Slice value) {
    return parse_uint32_option(value, "to", to_seqno, false);
  });
  parser.add_checked_option('\0', "prewarm", "Masterchain blocks used for TraceAssembler warmup/backtrack",
                            [&](td::Slice value) {
                              return parse_uint32_option(value, "prewarm", prewarm_count, true);
                            });
  parser.add_checked_option('\0', "max-data-depth", "Max data cell depth to serialize; 0 means no depth limit",
                            [&](td::Slice value) {
                              try {
                                max_data_depth = std::stoi(value.str());
                              } catch (...) {
                                return td::Status::Error(ton::ErrorCode::error,
                                                         "bad value for --max-data-depth: not a number");
                              }
                              return td::Status::OK();
                            });
  parser.add_checked_option('\0', "latest-states-prepare-parallelism",
                            "Parallel latest account state prepare actors", [&](td::Slice value) {
                              return parse_int32_option(value, "latest-states-prepare-parallelism",
                                                        latest_states_prepare_parallelism, false);
                            });
  parser.add_checked_option('\0', "latest-states-prepare-chunk-size", "Rows per latest account state prepare chunk",
                            [&](td::Slice value) {
                              return parse_int32_option(value, "latest-states-prepare-chunk-size",
                                                        latest_states_prepare_chunk_size, false);
                            });
  parser.add_checked_option('\0', "max-active-tasks", "Max active reading tasks", [&](td::Slice value) {
    return parse_uint32_option(value, "max-active-tasks", max_active_tasks, false);
  });
  parser.add_checked_option('\0', "max-insert-actors", "Parallel benchmark batch actors", [&](td::Slice value) {
    return parse_uint32_option(value, "max-insert-actors", max_insert_actors, false);
  });
  parser.add_checked_option('\0', "max-queue-blocks", "Max masterchain/block count in insert queue",
                            [&](td::Slice value) {
                              std::int32_t v;
                              auto status = parse_int32_option(value, "max-queue-blocks", v, false);
                              if (status.is_ok()) {
                                max_queue.mc_blocks_ = v;
                                max_queue.blocks_ = v;
                              }
                              return status;
                            });
  parser.add_checked_option('\0', "max-queue-txs", "Max transactions in insert queue", [&](td::Slice value) {
    return parse_int32_option(value, "max-queue-txs", max_queue.txs_, false);
  });
  parser.add_checked_option('\0', "max-queue-msgs", "Max messages in insert queue", [&](td::Slice value) {
    return parse_int32_option(value, "max-queue-msgs", max_queue.msgs_, false);
  });
  parser.add_checked_option('\0', "max-batch-blocks", "Max masterchain/block count in benchmark batch",
                            [&](td::Slice value) {
                              std::int32_t v;
                              auto status = parse_int32_option(value, "max-batch-blocks", v, false);
                              if (status.is_ok()) {
                                batch_size.mc_blocks_ = v;
                                batch_size.blocks_ = v;
                              }
                              return status;
                            });
  parser.add_checked_option('\0', "max-batch-txs", "Max transactions in benchmark batch", [&](td::Slice value) {
    return parse_int32_option(value, "max-batch-txs", batch_size.txs_, false);
  });
  parser.add_checked_option('\0', "max-batch-msgs", "Max messages in benchmark batch", [&](td::Slice value) {
    return parse_int32_option(value, "max-batch-msgs", batch_size.msgs_, false);
  });
  parser.add_checked_option('\0', "max-queue-size", "Set all queue limits to this value", [&](td::Slice value) {
    return parse_int32_option(value, "max-queue-size", max_queue_size, false);
  });
  parser.add_checked_option('\0', "max-batch-size", "Set all batch limits to this value", [&](td::Slice value) {
    return parse_int32_option(value, "max-batch-size", max_batch_size, false);
  });
  parser.add_checked_option('t', "threads", "Scheduler threads", [&](td::Slice value) {
    return parse_uint32_option(value, "threads", threads, false);
  });
  parser.add_checked_option('\0', "io-workers", "Scheduler IO workers", [&](td::Slice value) {
    return parse_uint32_option(value, "io-workers", io_workers, false);
  });
  parser.add_checked_option('\0', "stats-freq", "Pause between indexer stats logs in seconds", [&](td::Slice value) {
    return parse_int32_option(value, "stats-freq", stats_timeout, false);
  });
  parser.add_option('\0', "testnet", "Use testnet-specific interface detection rules", [&]() {
    testnet = true;
  });

  auto parse_status = parser.run(argc, argv);
  if (parse_status.is_error()) {
    LOG(ERROR) << "failed to parse options: " << parse_status.move_as_error();
    std::_Exit(2);
  }
  if (db_root.empty()) {
    LOG(ERROR) << "Please specify TON DB folder with -D or --db";
    std::_Exit(2);
  }
  if (working_dir.empty()) {
    LOG(ERROR) << "Please specify working directory with -W or --working-dir";
    std::_Exit(2);
  }
  if (from_seqno == 0 || to_seqno == 0) {
    LOG(ERROR) << "--from and --to are required for ton-celldb-bench";
    std::_Exit(2);
  }
  if (from_seqno > static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max()) ||
      to_seqno > static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max())) {
    LOG(ERROR) << "--from and --to must fit int32";
    std::_Exit(2);
  }
  if (from_seqno > to_seqno) {
    LOG(ERROR) << "--from must be less than or equal to --to";
    std::_Exit(2);
  }

  if (max_queue_size > 0) {
    max_queue.mc_blocks_ = max_queue_size;
    max_queue.blocks_ = max_queue_size;
    max_queue.txs_ = max_queue_size;
    max_queue.msgs_ = max_queue_size;
    max_queue.traces_ = max_queue_size;
  }
  if (max_batch_size > 0) {
    batch_size.mc_blocks_ = max_batch_size;
    batch_size.blocks_ = max_batch_size;
    batch_size.txs_ = max_batch_size;
    batch_size.msgs_ = max_batch_size;
    batch_size.traces_ = max_batch_size;
  }

  td::mkpath(working_dir + "/").ensure();
  NftItemDetectorR::is_testnet = testnet;

  auto stats = std::make_shared<BenchmarkStats>();
  auto finish_requested = std::make_shared<std::atomic_bool>(false);
  auto watcher = td::create_shared_destructor([finish_requested] {
    finish_requested->store(true);
  });

  constexpr float db_catch_up_interval = 60.0f;
  constexpr bool force_index = true;

  td::actor::Scheduler scheduler({td::actor::Scheduler::NodeInfo{threads, io_workers}});
  scheduler.run_in_context([&] {
    insert_manager = td::actor::create_actor<BenchmarkInsertManager>("benchmark_insert_manager", stats, from_seqno);
  });
  scheduler.run_in_context([&] {
    parse_manager = td::actor::create_actor<ParseManager>("parsemanager");
  });
  scheduler.run_in_context([&] {
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary,
                                                    working_dir + "/secondary_logs", db_catch_up_interval, false);
  });
  scheduler.run_in_context([&, watcher = std::move(watcher)] {
    index_scheduler = td::actor::create_actor<IndexScheduler>(
        "indexscheduler", db_scanner.get(), insert_manager.get(), parse_manager.get(), working_dir,
        static_cast<std::int32_t>(from_seqno), static_cast<std::int32_t>(to_seqno), force_index, max_active_tasks,
        max_queue, stats_timeout, watcher, prewarm_count);
  });
  scheduler.run_in_context([&] {
    td::actor::send_closure(insert_manager, &BenchmarkInsertManager::set_parallel_inserts_actors,
                            static_cast<int>(max_insert_actors));
    td::actor::send_closure(insert_manager, &BenchmarkInsertManager::set_insert_batch_size, batch_size);
    td::actor::send_closure(insert_manager, &BenchmarkInsertManager::set_max_data_depth, max_data_depth);
    td::actor::send_closure(insert_manager, &BenchmarkInsertManager::set_latest_states_prepare_parallelism,
                            latest_states_prepare_parallelism);
    td::actor::send_closure(insert_manager, &BenchmarkInsertManager::set_latest_states_prepare_chunk_size,
                            latest_states_prepare_chunk_size);
    td::actor::send_closure(insert_manager, &BenchmarkInsertManager::print_info);
  });
  scheduler.run_in_context([&] {
    td::actor::send_closure(index_scheduler, &IndexScheduler::run);
  });

  while (!finish_requested->load() && scheduler.run(1)) {
  }

  if (finish_requested->load()) {
    scheduler.run_in_context([&] {
      index_scheduler.reset();
      insert_manager.reset();
      parse_manager.reset();
      db_scanner.reset();
      td::actor::SchedulerContext::get().stop();
    });
    scheduler.run();
  }

  auto summary = stats->summary();
  summary += "\nindexer stage statistics\n";
  summary += g_statistics.generate_report_and_reset();
  auto stats_path = working_dir + "/bench_stats.txt";
  auto write_status = td::atomic_write_file(stats_path, summary);
  if (write_status.is_error()) {
    LOG(ERROR) << "Failed to write benchmark stats to " << stats_path << ": " << write_status.move_as_error();
  } else {
    LOG(INFO) << "Benchmark stats written to " << stats_path;
  }
  LOG(INFO) << "\n" << summary;
  LOG(INFO) << "Done!";
  return 0;
}
