#include <atomic>
#include <iostream>
#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/utils/port/path.h"

#include "crypto/vm/cp0.h"

#include "InsertManagerFirehose.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "IndexScheduler.h"

namespace {

// Set by SIGTERM/SIGINT: firecore's reader-node stops its child with SIGTERM
// and treats any exit but a clean one as a crash.
std::atomic_bool g_stop_requested{false};

void request_stop(int) {
  g_stop_requested.store(true);
}

td::Result<std::uint32_t> parse_uint(td::Slice value, td::Slice name) {
  auto r = td::to_integer_safe<std::uint32_t>(value);
  if (r.is_error()) {
    return td::Status::Error(PSLICE() << "bad value for --" << name << ": not a number");
  }
  return r.move_as_ok();
}

}  // namespace

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::actor::ActorOwn<DbScanner> db_scanner_;
  td::actor::ActorOwn<ParseManager> parse_manager_;
  td::actor::ActorOwn<InsertManagerFirehose> insert_manager_;
  td::actor::ActorOwn<IndexScheduler> index_scheduler_;

  td::uint32 threads = 7;
  td::uint64 celldb_cache_size = td::uint64{1024} << 20;  // RocksDB's default is far too small for cell reads
  td::uint32 io_workers = 1;
  td::int32 stats_timeout = 10;
  std::string db_root;
  std::string working_dir;
  td::uint32 from_seqno = 0;
  td::uint32 to_seqno = 0;
  td::uint32 max_active_tasks = 7;
  td::uint32 prewarm_count = 50;
  std::string db_event_fifo_path;
  std::string merged_blocks_dir;
  std::string json_dir;
  bool testnet = false;
  QueueState max_queue{10000, 100000, 1000000, 1000000};

  td::OptionParser p;
  p.set_description(
      "Read the TON node database and produce Firehose blocks of type sf.ton.type.v1.Block, one per masterchain seqno.\n"
      "By default prints the Firehose protocol to stdout for firecore's reader-node and resumes from its cursor.\n"
      "With --merged-blocks-dir, writes merged-blocks bundles of the range [--from, --to] instead.");
  p.add_option('\0', "help", "prints help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cerr << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice value) { db_root = value.str(); });
  p.add_option('W', "working-dir", "Path to working dir for secondary rocksdb logs, TraceAssembler state and the cursor",
               [&](td::Slice value) { working_dir = value.str(); });
  p.add_checked_option('f', "from", "Masterchain seqno to start from", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(from_seqno, parse_uint(value, "from"));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "to", "Last masterchain seqno of a bounded range", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(to_seqno, parse_uint(value, "to"));
    return td::Status::OK();
  });
  p.add_option('\0', "merged-blocks-dir", "Write merged-blocks bundles of [--from, --to] to this directory",
               [&](td::Slice value) { merged_blocks_dir = value.str(); });
  p.add_option('\0', "json-dir", "Also write every block as <mc_seqno>.json to this directory",
               [&](td::Slice value) { json_dir = value.str(); });
  p.add_option('\0', "db-event-fifo", "Path to the node db event FIFO for event-driven reading",
               [&](td::Slice value) { db_event_fifo_path = value.str(); });
  p.add_checked_option('\0', "prewarm",
                       "Masterchain blocks used for TraceAssembler warmup before the start; every trace that crosses "
                       "the start must begin within them (default: 50)",
                       [&](td::Slice value) {
                         TRY_RESULT_ASSIGN(prewarm_count, parse_uint(value, "prewarm"));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "max-active-tasks", "Max active reading tasks (default: 7)", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(max_active_tasks, parse_uint(value, "max-active-tasks"));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-queue-blocks", "Max masterchain blocks waiting for output", [&](td::Slice value) {
    TRY_RESULT(v, parse_uint(value, "max-queue-blocks"));
    max_queue.mc_blocks_ = v;
    max_queue.blocks_ = v * 10;
    return td::Status::OK();
  });
  p.add_checked_option('t', "threads", "Scheduler threads (default: 7)", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(threads, parse_uint(value, "threads"));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "stats-freq", "Pause between printing stats in seconds", [&](td::Slice value) {
    TRY_RESULT(v, parse_uint(value, "stats-freq"));
    stats_timeout = static_cast<td::int32>(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "celldb-cache-size", "Block cache of the node cell db in MiB (default: 1024)",
                       [&](td::Slice value) {
                         TRY_RESULT(v, parse_uint(value, "celldb-cache-size"));
                         celldb_cache_size = static_cast<td::uint64>(v) << 20;
                         return td::Status::OK();
                       });
  p.add_option('\0', "testnet", "Use for testnet", [&]() { testnet = true; });
  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }
  if (db_root.empty() || working_dir.empty()) {
    LOG(ERROR) << "--db and --working-dir are required";
    std::_Exit(2);
  }
  const bool bounded_range = from_seqno > 0 && to_seqno > 0;
  if (to_seqno > 0 && !bounded_range) {
    LOG(ERROR) << "--to requires --from";
    std::_Exit(2);
  }
  if (bounded_range && from_seqno > to_seqno) {
    LOG(ERROR) << "--from must be less than or equal to --to";
    std::_Exit(2);
  }
  if (!merged_blocks_dir.empty()) {
    // Bundles hold 100 blocks; the first one may start at the chain's first block.
    if (!bounded_range || (from_seqno % 100 != 0 && from_seqno != 1) || (to_seqno + 1) % 100 != 0) {
      LOG(ERROR) << "--merged-blocks-dir needs --from at a multiple of 100 (or 1) and --to just below one";
      std::_Exit(2);
    }
    td::mkpath(merged_blocks_dir + "/").ensure();
  }
  if (bounded_range && !db_event_fifo_path.empty()) {
    LOG(WARNING) << "--db-event-fifo is ignored with a bounded range (--from and --to)";
    db_event_fifo_path.clear();
  }
  td::mkpath(working_dir + "/").ensure();
  if (!json_dir.empty()) {
    td::mkpath(json_dir + "/").ensure();
  }

  std::vector<std::unique_ptr<FirehoseOutput>> outputs;
  if (merged_blocks_dir.empty()) {
    outputs.push_back(make_fire_protocol_output());
  } else {
    outputs.push_back(make_merged_blocks_output(merged_blocks_dir));
  }
  if (!json_dir.empty()) {
    outputs.push_back(make_json_output(json_dir));
  }
  std::string cursor_path = bounded_range ? std::string() : working_dir + "/firehose_cursor";

  const float db_catch_up_interval = bounded_range ? 60.0f : 1.0f;
  NftItemDetectorR::is_testnet = testnet;

  auto finish_requested = std::make_shared<std::atomic_bool>(false);
  auto watcher = td::create_shared_destructor([finish_requested] {
    finish_requested->store(true);
  });

  td::actor::Scheduler scheduler({td::actor::Scheduler::NodeInfo{threads, io_workers}});
  scheduler.run_in_context([&] {
    insert_manager_ = td::actor::create_actor<InsertManagerFirehose>("insertmanager", std::move(outputs), cursor_path);
  });
  scheduler.run_in_context([&] { parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager"); });
  scheduler.run_in_context([&] {
    db_scanner_ = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary,
                                                     working_dir + "/secondary_logs", db_catch_up_interval,
                                                     !bounded_range, celldb_cache_size);
  });
  scheduler.run_in_context([&, watcher = std::move(watcher)] {
    index_scheduler_ = td::actor::create_actor<IndexScheduler>("indexscheduler", db_scanner_.get(),
        insert_manager_.get(), parse_manager_.get(), working_dir, from_seqno, to_seqno, false, max_active_tasks,
        max_queue, stats_timeout, watcher, prewarm_count, db_event_fifo_path);
    td::actor::send_closure(index_scheduler_, &IndexScheduler::set_detect_interfaces_and_actions, false);
  });
  scheduler.run_in_context([&] { td::actor::send_closure(index_scheduler_, &IndexScheduler::run); });

  td::set_signal_handler(td::SignalType::Quit, request_stop).ensure();
  while (!finish_requested->load() && !g_stop_requested.load() && scheduler.run(1)) {
  }
  if (finish_requested->load() || g_stop_requested.load()) {
    scheduler.run_in_context([&] {
      index_scheduler_.reset();
      insert_manager_.reset();
      parse_manager_.reset();
      db_scanner_.reset();
      td::actor::SchedulerContext::get().stop();
    });
    scheduler.run();
  }
  LOG(INFO) << "Done!";
  return 0;
}
