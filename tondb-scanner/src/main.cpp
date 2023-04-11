#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "InsertManagerPostgres.h"
#include "DbScanner.h"

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  td::OptionParser p;
  p.set_description("Parse TON DB and insert data into Postgres");
  p.add_option('h', "help", "prints_help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });

  td::actor::ActorOwn<DbScanner> scanner;

  p.add_option('D', "db", "Path to TON DB folder",
               [&](td::Slice fname) { td::actor::send_closure(scanner, &DbScanner::set_db_root, fname.str()); });

  p.add_checked_option('F', "from", "Masterchain seqno to start indexing from",
               [&](td::Slice fname) { 
    int v;
    try {
      v = std::stoi(fname.str());
    } catch (...) {
      return td::Status::Error(ton::ErrorCode::error, "bad value for --from: not a number");
    }
    td::actor::send_closure(scanner, &DbScanner::set_last_known_seqno, v);
    return td::Status::OK();
  });

  SET_VERBOSITY_LEVEL(VERBOSITY_NAME(DEBUG));
  td::actor::Scheduler scheduler({32});
  td::actor::ActorOwn<InsertManagerInterface> insert_manager;
  scheduler.run_in_context([&] { insert_manager = td::actor::create_actor<InsertManagerPostgres>("insertmanager"); });
  scheduler.run_in_context([&] { scanner = td::actor::create_actor<DbScanner>("scanner", insert_manager.get()); });
  scheduler.run_in_context([&] { p.run(argc, argv).ensure(); });
  scheduler.run_in_context([&] { td::actor::send_closure(scanner, &DbScanner::run); });
  scheduler.run();
}



