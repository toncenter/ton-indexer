#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/utils/port/path.h"

#include "crypto/vm/cp0.h"

#include "../../index-engine/src/IndexScheduler.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "InsertManagerPostgres.h"
#include "TraceAssembler.h"

#include <atomic>
#include <limits>

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::actor::ActorOwn<DbScanner> db_scanner_;
  td::actor::ActorOwn<ParseManager> parse_manager_;
  td::actor::ActorOwn<InsertManagerPostgres> insert_manager_;
  td::actor::ActorOwn<IndexScheduler> index_scheduler_;

  // options
  td::uint32 threads = 7;
  td::uint32 io_workers = 1;
  td::int32 stats_timeout = 1;
  std::string db_root;
  std::string working_dir;
  td::uint32 last_known_seqno = 0;
  td::uint32 from_seqno = 0;
  td::uint32 to_seqno = 0;
  td::uint32 prewarm_count = 50;
  bool force_index = false;
  bool no_leader = false;
  bool custom_types = false;
  bool create_indexes = true;
  bool run_migrations = true;
  InsertManagerPostgres::Credential credential;
  KvrocksConfig kvrocks_config;
  PartitionManagerConfig partition_config;
  bool testnet = false;

  std::uint32_t max_active_tasks = 7;
  std::uint32_t max_insert_actors = 12;
  std::int32_t max_data_depth = 0;
  
  std::int32_t max_queue_size{-1};
  std::int32_t max_batch_size{-1};
  QueueState max_queue{10000, 100000, 100000, 100000};
  QueueState batch_size{2000, 2000, 10000, 10000};
  
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
  p.add_option('W', "working-dir", "Path to index working dir for secondary rocksdb logs", [&](td::Slice fname) { 
    working_dir = fname.str();
  });
  p.add_option('\0', "pg", "PostgreSQL connection string", [&](td::Slice value) { 
    credential.conn_str = value.str();
  });
  p.add_option('h', "host", "PostgreSQL host address", [&](td::Slice value) { 
    LOG(WARNING) << "Using --host option is deprecated, use --pg with connection string instead";
    credential.host = value.str();
  });
  p.add_checked_option('p', "port", "PostgreSQL port", [&](td::Slice value) {
    int port;
    try{
      port = std::stoi(value.str());
      if (!(port >= 0 && port < 65536))
        return td::Status::Error("Port must be a number between 0 and 65535");
    } catch(...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --port: not a number");
    }
    LOG(WARNING) << "Using --port option is deprecated, use --pg with connection string instead";
    credential.port = port;
    return td::Status::OK();
  });
  p.add_option('u', "user", "PostgreSQL username", [&](td::Slice value) { 
    LOG(WARNING) << "Using --user option is deprecated, use --pg with connection string instead";
    credential.user = value.str();
  });
  p.add_option('P', "password", "PostgreSQL password", [&](td::Slice value) {
    LOG(WARNING) << "Using --password option is deprecated, use --pg with connection string instead";
    credential.password = value.str();
  });
  p.add_option('d', "dbname", "PostgreSQL database name", [&](td::Slice value) {
    LOG(WARNING) << "Using --dbname option is deprecated, use --pg with connection string instead";
    credential.dbname = value.str();
  });

  p.add_option('\0', "kvrocks", "Kvrocks direct Redis URI, e.g. tcp://127.0.0.1:6666/0", [&](td::Slice value) {
    kvrocks_config.enabled = true;
    kvrocks_config.uri = value.str();
  });
  p.add_checked_option('\0', "kvrocks-sentinels", "Comma-separated Kvrocks Sentinel nodes, e.g. 127.0.0.1:26379,127.0.0.1:26380", [&](td::Slice value) {
    try {
      kvrocks_config.enabled = true;
      kvrocks_config.sentinel_nodes = parse_kvrocks_sentinel_nodes(value.str());
    } catch (const std::exception &e) {
      return td::Status::Error(ton::ErrorCode::error, PSLICE() << "bad value for --kvrocks-sentinels: " << e.what());
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
      return td::Status::Error(ton::ErrorCode::error, "bad value for --kvrocks-db: not a number");
    }
    if (v < 0) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --kvrocks-db: must be non-negative");
    }
    kvrocks_config.db = v;
    return td::Status::OK();
  });
  p.add_option('\0', "kvrocks-sentinel-user", "Kvrocks Sentinel username", [&](td::Slice value) {
    kvrocks_config.sentinel_user = value.str();
  });
  p.add_option('\0', "kvrocks-sentinel-password", "Kvrocks Sentinel password", [&](td::Slice value) {
    kvrocks_config.sentinel_password = value.str();
  });

  p.add_option('\0', "pg-manage-partitions", "Manage hot PostgreSQL range partitions for historical tables", [&]() {
    partition_config.enabled = true;
  });
  p.add_checked_option('\0', "pg-partition-size-mc-seqnos", "PostgreSQL partition size in masterchain seqnos", [&](td::Slice value) {
    std::uint64_t v;
    std::size_t parsed = 0;
    auto raw = value.str();
    if (raw.empty() || raw[0] == '-') {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-size-mc-seqnos: must be positive");
    }
    try {
      v = std::stoull(raw, &parsed);
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-size-mc-seqnos: not a number");
    }
    if (parsed != raw.size()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-size-mc-seqnos: not a number");
    }
    if (v == 0 || v > std::numeric_limits<std::uint32_t>::max()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-size-mc-seqnos: must be between 1 and uint32 max");
    }
    partition_config.partition_size_mc_seqnos = static_cast<std::uint32_t>(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "pg-partition-retention-mc-seqnos", "PostgreSQL hot partition retention in masterchain seqnos; 0 disables dropping", [&](td::Slice value) {
    std::uint64_t v;
    std::size_t parsed = 0;
    auto raw = value.str();
    if (raw.empty() || raw[0] == '-') {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-retention-mc-seqnos: must be non-negative");
    }
    try {
      v = std::stoull(raw, &parsed);
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-retention-mc-seqnos: not a number");
    }
    if (parsed != raw.size()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-retention-mc-seqnos: not a number");
    }
    if (v > std::numeric_limits<std::uint32_t>::max()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-retention-mc-seqnos: must fit uint32");
    }
    partition_config.retention_mc_seqnos = static_cast<std::uint32_t>(v);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "pg-partition-precreate-count", "Number of future PostgreSQL partitions to precreate", [&](td::Slice value) {
    std::uint64_t v;
    std::size_t parsed = 0;
    auto raw = value.str();
    if (raw.empty() || raw[0] == '-') {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-precreate-count: must be non-negative");
    }
    try {
      v = std::stoull(raw, &parsed);
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-precreate-count: not a number");
    }
    if (parsed != raw.size()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-precreate-count: not a number");
    }
    if (v > std::numeric_limits<std::uint32_t>::max()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --pg-partition-precreate-count: must fit uint32");
    }
    partition_config.precreate_count = static_cast<std::uint32_t>(v);
    return td::Status::OK();
  });

  p.add_option('\0', "testnet", "Use for testnet. It is used for correct indexing of .ton DNS entries (in testnet .ton collection has a different address)", [&]() {
    testnet = true;
  });

  p.add_checked_option('f', "from", "Masterchain seqno to start indexing from", [&](td::Slice value) { 
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --from: not a number");
    }
    if (v < 0) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --from: must be non-negative");
    }
    from_seqno = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "to", "Masterchain seqno to end indexing", [&](td::Slice value) { 
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --to: not a number");
    }
    if (v < 0) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --to: must be non-negative");
    }
    to_seqno = v;
    return td::Status::OK();
  });
  p.add_option('\0', "force", "Ignore existing seqnos and force reindex", [&]() {
    force_index = true;
    LOG(WARNING) << "Force reindexing enabled";
  });
  p.add_option('\0', "no-leader", "Disable Postgres leader/standby coordination; only valid with --from and --to", [&]() {
    no_leader = true;
  });
  p.add_checked_option('\0', "prewarm", "Number of masterchain blocks used for TraceAssembler warmup/backtrack", [&](td::Slice value) {
    std::uint64_t v;
    std::size_t parsed = 0;
    auto raw = value.str();
    if (raw.empty() || raw[0] == '-') {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --prewarm: must be non-negative");
    }
    try {
      v = std::stoull(raw, &parsed);
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --prewarm: not a number");
    }
    if (parsed != raw.size()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --prewarm: not a number");
    }
    if (v > std::numeric_limits<std::uint32_t>::max()) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --prewarm: must fit uint32");
    }
    prewarm_count = static_cast<td::uint32>(v);
    return td::Status::OK();
  });

  p.add_checked_option('\0', "max-data-depth", "Max data cell depth to store in latest account states", [&](td::Slice value) { 
    int v;
    try {
      v = std::stoi(value.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-data-depth: not a number");
    }
    max_data_depth = v;
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

  // queue settings
  p.add_checked_option('\0', "max-queue-blocks", "Max blocks in insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-blocks: not a number");
    }
    max_queue.mc_blocks_ = v;
    max_queue.blocks_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-queue-txs", "Max transactions in insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-txs: not a number");
    }
    max_queue.txs_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-queue-msgs", "Max messages in insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-msgs: not a number");
    }
    max_queue.msgs_ = v;
    return td::Status::OK();
  });

  // insert manager settings
  p.add_checked_option('\0', "max-insert-actors", "Number of parallel insert actors (default: 3)", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-insert-actors: not a number");
    }
    max_insert_actors = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-batch-blocks", "Max blocks in batch", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-batch-blocks: not a number");
    }
    batch_size.mc_blocks_ = v;
    batch_size.blocks_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-batch-txs", "Max transactions in batch", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-batch-txs: not a number");
    }
    batch_size.txs_ = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-batch-msgs", "Max messages in batch", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-batch-msgs: not a number");
    }
    batch_size.msgs_ = v;
    return td::Status::OK();
  });

  p.add_checked_option('\0', "max-queue-size", "Max size of insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-size: not a number");
    }
    max_queue_size = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-batch-size", "Max size of batch", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-size: not a number");
    }
    max_batch_size = v;
    return td::Status::OK();
  });
  
  // scheduler settings
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
  p.add_checked_option('\0', "io-workers", "Scheduler IO workers (default: 1)", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --io-workers: not a number");
    }
    io_workers = v;
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
  
  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }
  if (kvrocks_config.enabled) {
    if (kvrocks_config.use_sentinel() && kvrocks_config.uri.size() > 0) {
      LOG(ERROR) << "Use either --kvrocks or --kvrocks-sentinels, not both";
      std::_Exit(2);
    }
    if (kvrocks_config.use_sentinel() && kvrocks_config.sentinel_master_name.empty()) {
      LOG(ERROR) << "--kvrocks-master is required with --kvrocks-sentinels";
      std::_Exit(2);
    }
  }
  if (working_dir.size() == 0) {
    LOG(ERROR) << "Please specify working directory with -W or --working-dir";
    std::_Exit(2);
  }
  const bool bounded_archive_range = from_seqno > 0 && to_seqno > 0;
  if (from_seqno > static_cast<td::uint32>(std::numeric_limits<td::int32>::max()) ||
      to_seqno > static_cast<td::uint32>(std::numeric_limits<td::int32>::max())) {
    LOG(ERROR) << "--from and --to must fit int32";
    std::_Exit(2);
  }
  if (bounded_archive_range && from_seqno > to_seqno) {
    LOG(ERROR) << "--from must be less than or equal to --to";
    std::_Exit(2);
  }
  if (no_leader && !bounded_archive_range) {
    LOG(ERROR) << "--no-leader is only valid when both --from and --to are specified";
    std::_Exit(2);
  }
  if (bounded_archive_range && partition_config.enabled) {
    LOG(ERROR) << "--pg-manage-partitions cannot be used with bounded archive indexing (--from and --to)";
    std::_Exit(2);
  }
  td::mkpath(working_dir + "/").ensure();

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

  NftItemDetectorR::is_testnet = testnet;

  auto finish_requested = std::make_shared<std::atomic_bool>(false);
  auto watcher = td::create_shared_destructor([finish_requested] {
    finish_requested->store(true);
  });

  td::actor::Scheduler scheduler({td::actor::Scheduler::NodeInfo{threads, io_workers}});
  scheduler.run_in_context([&] {
    insert_manager_ = td::actor::create_actor<InsertManagerPostgres>(
        "insertmanager", credential, kvrocks_config, partition_config, no_leader, bounded_archive_range);
  });
  scheduler.run_in_context([&] { parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager"); });
  scheduler.run_in_context([&] { db_scanner_ = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, working_dir + "/secondary_logs"); });

  scheduler.run_in_context([&, watcher = std::move(watcher)] { index_scheduler_ = td::actor::create_actor<IndexScheduler>("indexscheduler", db_scanner_.get(), 
    insert_manager_.get(), parse_manager_.get(), working_dir, from_seqno, to_seqno, force_index, max_active_tasks, max_queue, stats_timeout, watcher, prewarm_count);
  });
  scheduler.run_in_context([&] { 
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_parallel_inserts_actors, max_insert_actors);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_batch_size, batch_size);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_max_data_depth, max_data_depth);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::print_info);
  });
  scheduler.run_in_context([&] { td::actor::send_closure(index_scheduler_, &IndexScheduler::run); });
  
  while(!finish_requested->load() && scheduler.run(1)) {
    // do something
  }
  if (finish_requested->load()) {
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
