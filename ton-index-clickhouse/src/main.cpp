#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "InsertManagerClickhouse.h"
#include "DataParser.h"
#include "DbScanner.h"
#include "IndexScheduler.h"


int main(int argc, char *argv[]) {
    SET_VERBOSITY_LEVEL(verbosity_INFO);
    td::set_default_failure_signal_handler().ensure();

    CHECK(vm::init_op_cp0());

    td::actor::ActorOwn<DbScanner> db_scanner_;
    td::actor::ActorOwn<InsertManagerClickhouse> insert_manager_;
    td::actor::ActorOwn<ParseManager> parse_manager_;
    td::actor::ActorOwn<IndexScheduler> index_scheduler_;

    // options
    td::uint32 threads = 7;
    std::string db_root;
    td::uint32 last_known_seqno = 0;
    td::int32 max_db_actors = 32;

    InsertManagerClickhouse::Credential credential;
    td::int32 batch_blocks_count = 512;
    td::int32 batch_txs_count = 32768;
    td::int32 batch_msgs_count = 65536;
    td::int32 max_insert_actors = 3;

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
    p.add_checked_option('\0', "max-db-actors", "Max database reader actors (default: 2048)", [&](td::Slice fname) { 
        int v;
        try {
            v = std::stoi(fname.str());
        } catch (...) {
            return td::Status::Error(ton::ErrorCode::error, "bad value for --max-db-actors: not a number");
        }
        max_db_actors = v;
        return td::Status::OK();
    });
    p.add_checked_option('\0', "insert-batch-blocks", "Max blocks in batch (default: 32768)", [&](td::Slice fname) { 
        int v;
        try {
            v = std::stoi(fname.str());
        } catch (...) {
            return td::Status::Error(ton::ErrorCode::error, "bad value for --insert-batch-blocks: not a number");
        }
        batch_txs_count = v;
        return td::Status::OK();
    });
    p.add_checked_option('\0', "insert-batch-txs", "Max transactions in batch (default: 32768)", [&](td::Slice fname) { 
        int v;
        try {
            v = std::stoi(fname.str());
        } catch (...) {
            return td::Status::Error(ton::ErrorCode::error, "bad value for --insert-batch-txs: not a number");
        }
        batch_txs_count = v;
        return td::Status::OK();
    });
    p.add_checked_option('\0', "insert-batch-msgs", "Max messages in batch (default: 65536)", [&](td::Slice fname) { 
        int v;
        try {
            v = std::stoi(fname.str());
        } catch (...) {
            return td::Status::Error(ton::ErrorCode::error, "bad value for --insert-batch-msgs: not a number");
        }
        batch_msgs_count = v;
        return td::Status::OK();
    });
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
    scheduler.run_in_context([&] { insert_manager_ = td::actor::create_actor<InsertManagerClickhouse>("insertmanager", credential); });
    scheduler.run_in_context([&] { parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager"); });
    scheduler.run_in_context([&] { db_scanner_ = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, max_db_actors); });
    
    scheduler.run_in_context([&] { index_scheduler_ = td::actor::create_actor<IndexScheduler>("indexscheduler", db_scanner_.get(), insert_manager_.get(), parse_manager_.get(), last_known_seqno); });
    scheduler.run_in_context([&] { td::actor::send_closure(index_scheduler_, &IndexScheduler::run); });

    while(scheduler.run(1)) {
        // do something
    }
    LOG(INFO) << "Done!";
    return 0;
}