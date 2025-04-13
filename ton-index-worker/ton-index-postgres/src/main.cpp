#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/utils/port/path.h"

#include "crypto/vm/cp0.h"

#include "InsertManagerPostgres.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "TraceAssembler.h"
#include "EventProcessor.h"
#include "IndexScheduler.h"


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
  td::int32 stats_timeout = 10;
  std::string db_root;
  std::string working_dir;
  td::uint32 last_known_seqno = 0;
  td::uint32 from_seqno = 0;
  td::uint32 to_seqno = 0;
  bool force_index = false;
  bool custom_types = false;
  bool create_indexes = true;
  bool run_migrations = true;
  InsertManagerPostgres::Credential credential;
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
  p.add_option('h', "host", "PostgreSQL host address",  [&](td::Slice value) { 
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
    credential.port = port;
    return td::Status::OK();
  });
  p.add_option('u', "user", "PostgreSQL username", [&](td::Slice value) { 
    credential.user = value.str();
  });
  p.add_option('P', "password", "PostgreSQL password", [&](td::Slice value) { 
    credential.password = value.str();
  });
  p.add_option('d', "dbname", "PostgreSQL database name", [&](td::Slice value) { 
    credential.dbname = value.str();
  });
  p.add_option('\0', "custom-types", "Use pgton extension with custom types", [&]() {
    custom_types = true;
    LOG(WARNING) << "Using pgton extension!";
  });
  
  p.add_option('\0', "no-create-indexes", "Do not create indexes in database", [&]() {
    create_indexes = false;
    LOG(WARNING) << "Indexes will not be created on launch.";
  });

  p.add_option('\0', "no-migrations", "Do not run migrations", [&]() {
    run_migrations = false;
    LOG(WARNING) << "Migrations will not be executed on launch.";
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
    to_seqno = v;
    return td::Status::OK();
  });
  p.add_option('\0', "force", "Ignore existing seqnos and force reindex", [&]() {
    force_index = true;
    LOG(WARNING) << "Force reindexing enabled";
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
  if (working_dir.size() == 0) {
    LOG(ERROR) << "Please specify working directory with -W or --working-dir";
    std::_Exit(2);
  }
  td::mkdir(working_dir).ensure();

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

  auto watcher = td::create_shared_destructor([] {
    td::actor::SchedulerContext::get()->stop();
  });

  td::actor::Scheduler scheduler({td::actor::Scheduler::NodeInfo{threads, io_workers}});
  scheduler.run_in_context([&] { insert_manager_ = td::actor::create_actor<InsertManagerPostgres>("insertmanager", credential, custom_types, create_indexes, run_migrations); });
  scheduler.run_in_context([&] { parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager"); });
  scheduler.run_in_context([&] { db_scanner_ = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, working_dir + "/secondary_logs"); });

  scheduler.run_in_context([&, watcher = std::move(watcher)] { index_scheduler_ = td::actor::create_actor<IndexScheduler>("indexscheduler", db_scanner_.get(), 
    insert_manager_.get(), parse_manager_.get(), working_dir, from_seqno, to_seqno, force_index, max_active_tasks, max_queue, stats_timeout, watcher); 
  });
  scheduler.run_in_context([&] { 
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_parallel_inserts_actors, max_insert_actors);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_batch_size, batch_size);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_max_data_depth, max_data_depth);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::print_info);
  });
  scheduler.run_in_context([&] { td::actor::send_closure(index_scheduler_, &IndexScheduler::run); });
  
  while(scheduler.run(1)) {
    // do something
  }
  LOG(INFO) << "Done!";
  return 0;
}
