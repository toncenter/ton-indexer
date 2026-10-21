// ton-tvm-executor answers ton.tvm.v1.Executor for the masterchain blocks of
// a node database it reads as a RocksDB secondary (substreams-tvm.md, step 2).
#include <atomic>
#include <iostream>
#include <thread>
#include "td/utils/OptionParser.h"
#include "td/utils/logging.h"
#include "td/utils/port/path.h"
#include "td/utils/port/signals.h"

#include "crypto/vm/vm.h"
#include "CellCache.h"
#include "DbScanner.h"
#include "Executor.h"
#include "ExecutorServer.h"

namespace {

std::atomic_bool g_stop_requested{false};

void request_stop(int) {
  g_stop_requested.store(true);
}

}  // namespace

int main(int argc, char* argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  std::string db_root;
  std::string working_dir;
  std::string listen_addr = "127.0.0.1:10060";
  td::uint32 threads = 8;
  td::uint32 max_requests = 128;
  td::uint64 celldb_cache_size = td::uint64{1024} << 20;
  td::uint64 cell_cache_size = td::uint64{1024} << 20;

  td::OptionParser p;
  p.set_description("Answer ton.tvm.v1.Executor over gRPC for the masterchain blocks of a TON node database");
  p.add_option('\0', "help", "prints help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cerr << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice value) { db_root = value.str(); });
  p.add_option('W', "working-dir", "Path to working dir for secondary rocksdb logs",
               [&](td::Slice value) { working_dir = value.str(); });
  p.add_option('\0', "listen-addr", "gRPC address (default: 127.0.0.1:10060)",
               [&](td::Slice value) { listen_addr = value.str(); });
  p.add_checked_option('t', "threads", "Scheduler threads (default: 8)", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(threads, td::to_integer_safe<td::uint32>(value));
    if (threads == 0 || threads > 128) return td::Status::Error("threads must be between 1 and 128");
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-requests", "Maximum queued and executing RPC batches (default: 128)", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(max_requests, td::to_integer_safe<td::uint32>(value));
    if (max_requests == 0) return td::Status::Error("max-requests must be positive");
    return td::Status::OK();
  });
  p.add_checked_option('\0', "celldb-cache-size", "Block cache of the node cell db in MiB (default: 1024)",
                       [&](td::Slice value) {
                         TRY_RESULT(v, td::to_integer_safe<td::uint32>(value));
                         celldb_cache_size = static_cast<td::uint64>(v) << 20;
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "cell-cache-size",
                       "Cache of loaded account cells across requests in MiB, 0 to disable (default: 1024)",
                       [&](td::Slice value) {
                         TRY_RESULT(v, td::to_integer_safe<td::uint32>(value));
                         cell_cache_size = static_cast<td::uint64>(v) << 20;
                         return td::Status::OK();
                       });
  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }
  if (db_root.empty() || working_dir.empty()) {
    LOG(ERROR) << "--db and --working-dir are required";
    std::_Exit(2);
  }
  td::mkpath(working_dir + "/").ensure();
  // Debug primitives stay off, as in liteserver: their output costs time, and
  // gas does not pay for it.
  vm::init_vm(false).ensure();
  // ShardAccount BOCs over the limit are found out before all their cells are
  // loaded only with the cache.
  firehose::set_cell_cache_size(cell_cache_size);

  td::actor::Scheduler scheduler({threads});
  auto requests = std::make_shared<tvm_executor::RequestQueue>(max_requests);
  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<tvm_executor::Executor> executor;
  scheduler.run_in_context([&] {
    // Caught up every second, and at once for a request beyond the head.
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary,
                                                     working_dir + "/secondary_logs", 1.0f, false, celldb_cache_size);
    executor = td::actor::create_actor<tvm_executor::Executor>(
        td::actor::ActorOptions().with_name("executor").with_poll(), requests, db_scanner.get());
  });

  std::string listen_error;
  auto server = tvm_executor::ExecutorServer::start(
      listen_addr,
      [requests](tvm_executor::Execution execution) { requests->submit(std::move(execution)); },
      &listen_error);
  if (!server) {
    LOG(ERROR) << listen_error;
    std::_Exit(1);
  }
  LOG(INFO) << "Serving ton.tvm.v1.Executor on " << listen_addr;

  td::set_signal_handler(td::SignalType::Quit, request_stop).ensure();
  while (!g_stop_requested.load() && scheduler.run(1)) {
  }
  requests->close();
  // Shutdown waits for RPCs, whose actors still need CPU workers and IO
  // timers (CellDb returns loaded cells through delay_action).
  std::atomic_bool drained{false};
  std::thread shutdown([&] {
    server->shutdown();
    drained.store(true);
  });
  while (!drained.load()) scheduler.run(0.1);
  shutdown.join();
  scheduler.run_in_context([&] {
    executor.reset();
    db_scanner.reset();
    td::actor::SchedulerContext::get().stop();
  });
  scheduler.run();
  return 0;
}
