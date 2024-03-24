#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "InsertManagerPostgres.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "EventProcessor.h"
#include "IndexScheduler.h"


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::actor::ActorOwn<DbScanner> db_scanner_;
  td::actor::ActorOwn<ParseManager> parse_manager_;
  td::actor::ActorOwn<EventProcessor> event_processor_;
  td::actor::ActorOwn<InsertManagerPostgres> insert_manager_;
  td::actor::ActorOwn<IndexScheduler> index_scheduler_;

  // options
  td::uint32 threads = 7;
  td::int32 stats_timeout = 10;
  std::string db_root;
  td::uint32 last_known_seqno = 0;
  td::int32 max_db_cache_size = 1024;

  InsertManagerPostgres::Credential credential;

  std::uint32_t max_active_tasks = 7;
  std::uint32_t max_queue_blocks = 20000;
  std::uint32_t max_queue_txs = 10000000;
  std::uint32_t max_queue_msgs = 10000000;
  
  std::uint32_t max_insert_actors = 12;
  std::uint32_t max_batch_blocks = 10000;
  std::uint32_t max_batch_txs = 50000;
  std::uint32_t max_batch_msgs = 50000;
  
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

  // queue settings
  p.add_checked_option('\0', "max-queue-blocks", "Max blocks in insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-blocks: not a number");
    }
    max_queue_blocks = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-queue-txs", "Max transactions in insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-txs: not a number");
    }
    max_queue_txs = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-queue-msgs", "Max messages in insert queue", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-queue-msgs: not a number");
    }
    max_queue_msgs = v;
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
    max_batch_blocks = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-batch-txs", "Max transactions in batch", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-batch-txs: not a number");
    }
    max_batch_txs = v;
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-batch-msgs", "Max messages in batch", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --max-batch-msgs: not a number");
    }
    max_batch_msgs = v;
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

  td::actor::Scheduler scheduler({threads});
  scheduler.run_in_context([&] { insert_manager_ = td::actor::create_actor<InsertManagerPostgres>("insertmanager", credential); });
  scheduler.run_in_context([&] { parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager"); });
  scheduler.run_in_context([&] { db_scanner_ = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, last_known_seqno, max_db_cache_size); });

  scheduler.run_in_context([&] { index_scheduler_ = td::actor::create_actor<IndexScheduler>("indexscheduler", db_scanner_.get(), 
    insert_manager_.get(), parse_manager_.get(), last_known_seqno, max_active_tasks, max_queue_blocks, max_queue_blocks, max_queue_txs, max_queue_msgs, stats_timeout); 
  });
  scheduler.run_in_context([&] { 
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_parallel_inserts_actors, max_insert_actors);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_mc_blocks, max_batch_blocks);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_blocks, max_batch_blocks);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_txs, max_batch_txs);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_msgs, max_batch_msgs);
    td::actor::send_closure(insert_manager_, &InsertManagerPostgres::print_info);
  });
  scheduler.run_in_context([&] { td::actor::send_closure(index_scheduler_, &IndexScheduler::run); });
  
  while(scheduler.run(1)) {
    // do something
  }
  LOG(INFO) << "Done!";
  return 0;
}
