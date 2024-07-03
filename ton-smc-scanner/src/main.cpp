#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/actor/actor.h"

#include "crypto/vm/cp0.h"
#include "DbScanner.h"
#include "SmcScanner.h"


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::uint32 seqno = 0;
  td::uint32 threads = 7;
  std::string db_root = "/var/lib/ton-work/db";

  td::OptionParser p;
  p.set_description("Scan all accounts at some seqno, detect interfaces and save them to postgres");
  p.add_checked_option('S', "seqno", "Masterchain seqno to start indexing from", [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error("bad value for --seqno: not a number");
    }
    seqno = v;
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
  p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice fname) { 
    db_root = fname.str();
  });

  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }

  td::actor::Scheduler scheduler({threads});

  // auto watcher = td::create_shared_destructor([] {
  //   td::actor::SchedulerContext::get()->stop();
  // });

  td::actor::ActorOwn<DbScanner> db_scanner;

  scheduler.run_in_context([&] { 
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_readonly);
    td::actor::create_actor<SmcScanner>("smcscanner", db_scanner.get(), seqno).release();
  });
  
  scheduler.run();

  LOG(INFO) << "TON DB integrity check finished successfully";
  
  return 0;
}


