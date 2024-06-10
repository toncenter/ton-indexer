#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "DataParser.h"
#include "DbScanner.h"
#include "DbReader.h"
#include "Scheduler.h"


int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::actor::ActorOwn<DbScanner> db_scanner_;
  td::actor::ActorOwn<DbReader> db_reader_;
  td::actor::ActorOwn<ParseManager> parse_manager_;
  td::actor::ActorOwn<Scheduler> scheduler_;

  // options
  td::uint32 threads = 7;
  td::int32 last_known_seqno = 2;
  std::string db_root;
  
  td::OptionParser p;
  p.set_description("Sandbox");
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
  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }

  td::actor::Scheduler scheduler({threads});
  scheduler.run_in_context([&] { 
    parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager");
    db_scanner_ = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary);
    db_reader_ = td::actor::create_actor<DbReader>("reader", db_root);

    scheduler_ = td::actor::create_actor<Scheduler>("scheduler", db_scanner_.get(), db_reader_.get(),
      parse_manager_.get(), last_known_seqno);
  });
  scheduler.run_in_context([&] { td::actor::send_closure(scheduler_, &Scheduler::run); });
  
  while(scheduler.run(1)) {
    // do something
  }
  LOG(INFO) << "Done!";
  return 0;
}
