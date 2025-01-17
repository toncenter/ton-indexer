#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"
#include "td/utils/port/path.h"
#include "td/actor/actor.h"
#include "crypto/vm/cp0.h"
#include "tddb/td/db/RocksDb.h"
#include <iostream>
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "crypto/vm/db/DynamicBagOfCellsDb.h"
#include "crypto/vm/db/CellStorage.h"


static td::Status from_rocksdb(rocksdb::Status status) {
  if (status.ok()) {
    return td::Status::OK();
  }
  return td::Status::Error(status.ToString());
}
static td::Slice from_rocksdb(rocksdb::Slice slice) {
  return td::Slice(slice.data(), slice.size());
}
static rocksdb::Slice to_rocksdb(td::Slice slice) {
  return rocksdb::Slice(slice.data(), slice.size());
}

class MigrateBatchActor: public td::actor::Actor {
    td::Bits256 from_;
    td::Bits256 to_;
    std::shared_ptr<td::RocksDb> db_;
    int new_compress_depth_;
    td::Promise<td::Unit> promise_;

    std::unique_ptr<vm::CellLoader> loader_;
    std::shared_ptr<vm::DynamicBagOfCellsDb> boc_;

    uint32_t migrated_{0};
public:
    MigrateBatchActor(td::Bits256 from, td::Bits256 to, std::shared_ptr<td::RocksDb> db, int new_compress_depth, td::Promise<td::Unit> promise)
        : from_(from), to_(to), db_(db), new_compress_depth_(new_compress_depth), promise_(std::move(promise)) {

        loader_ = std::make_unique<vm::CellLoader>(db_);
        boc_ = vm::DynamicBagOfCellsDb::create();
        boc_->set_celldb_compress_depth(new_compress_depth_); // probably not necessary in this context
        boc_->set_loader(std::make_unique<vm::CellLoader>(db_));
    }

    void start_up() override {
        vm::CellStorer storer{*db_};

        std::unique_ptr<rocksdb::Iterator> it;
        it.reset(db_->raw_db()->NewIterator({}));
        db_->begin_write_batch().ensure();
        for (it->Seek(to_rocksdb(from_.as_slice())); it->Valid(); it->Next()) {
            auto key = from_rocksdb(it->key());
            if (key.size() != 32) {
                LOG(WARNING) << "CellDb: skipping key with size " << key.size();
                continue;
            }
            td::Bits256 hash = td::Bits256(td::ConstBitPtr{key.ubegin()});
            if (!(hash < to_)) {
                break;
            }

            auto value = from_rocksdb(it->value());
            migrate_cell(hash, value, storer);
        }
        db_->commit_write_batch().ensure();

        LOG(INFO) << "Migrating batch from " << from_.to_hex() << " to " << to_.to_hex() << " done. Migrated " << migrated_ << " cells";
        promise_.set_value(td::Unit());
        stop();
    }

    td::Status migrate_cell(const td::Bits256& hash, const td::Slice& value, vm::CellStorer& storer) {
        auto R = loader_->load(hash.as_slice(), value, true, boc_->as_ext_cell_creator());
        if (R.is_error()) {
            LOG(WARNING) << "CellDb: failed to load cell: " << R.move_as_error();
            return td::Status::OK();
        }
        if (R.ok().status == vm::CellLoader::LoadResult::NotFound) {
            LOG(WARNING) << "CellDb: cell not found";
            return td::Status::OK();
        }
        bool expected_stored_boc = R.ok().cell_->get_depth() == new_compress_depth_;
        if (expected_stored_boc != R.ok().stored_boc_) {
            ++migrated_;
            storer.set(R.ok().refcnt(), R.ok().cell_, expected_stored_boc).ensure();
            LOG(DEBUG) << "Migrating cell " << hash.to_hex();
        }
        return td::Status::OK();
    }
};

class MigrateCellDBActor: public td::actor::Actor {
    std::string db_root_;
    int new_compress_depth_;
    int max_parallel_batches_;

    std::shared_ptr<td::RocksDb> db_;

    uint32_t migrated_{0};

    td::Bits256 current_;

    int cur_parallel_batches_{0};
public:
    MigrateCellDBActor(std::string db_root, int new_compress_depth, int max_parallel_batches)
        : db_root_(db_root), new_compress_depth_(new_compress_depth), max_parallel_batches_(max_parallel_batches) {
        td::RocksDbOptions read_db_options;
        read_db_options.use_direct_reads = true;
        auto db_r = td::RocksDb::open(db_root_ + "/celldb", std::move(read_db_options));
        if (db_r.is_error()) {
            LOG(FATAL) << "failed to open db: " << db_r.error();
            stop();
            return;
        }
        db_ = std::make_shared<td::RocksDb>(db_r.move_as_ok());

        current_ = td::Bits256::zero();
    }

    void start_up() override {
        uint64_t count;
        db_->raw_db()->GetIntProperty("rocksdb.estimate-num-keys", &count);
        LOG(INFO) << "Estimated total number of keys: " << count;

        deploy_batches();
    }

    void deploy_batches() {
        using namespace td::literals;
        const auto interval_bi = "0000100000000000000000000000000000000000000000000000000000000000"_rx256;
        CHECK(interval_bi.not_null());

        while (cur_parallel_batches_ < max_parallel_batches_) {
            auto current_bi = td::bits_to_refint(current_.bits(), 256, false);
            auto to_bi = current_bi + interval_bi;
            if (!to_bi->is_valid()) {
                LOG(INFO) << "New to_bi is invalid. Stopping.";
                return;
            }
            td::Bits256 to;
            std::string to_hex = to_bi->to_hex_string();
            if (to_hex.size() < 64) {
                to_hex = std::string(64 - to_hex.size(), '0') + to_hex;
            }
            if (to.from_hex(to_hex) >= 256) {
                LOG(INFO) << "New to_bi is too large. Stopping.";
                return;
            }

            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), current = current_, to = to](td::Result<td::Unit> R) {
                if (R.is_error()) {
                    LOG(ERROR) << "failed to migrate batch from " << current.to_hex() << " to " << to.to_hex() << ": " << R.error();
                } else {
                    LOG(INFO) << "migrated batch from " << current.to_hex() << " to " << to.to_hex();
                }
                td::actor::send_closure(SelfId, &MigrateCellDBActor::on_batch_migrated);
            });
            auto db_clone = std::make_shared<td::RocksDb>(db_->clone());
            td::actor::create_actor<MigrateBatchActor>("migrate", current_, to, db_clone, new_compress_depth_, std::move(P)).release();
            current_ = to;

            cur_parallel_batches_++;
        }
    }

    void on_batch_migrated() {
        cur_parallel_batches_--;

        deploy_batches();

        if (cur_parallel_batches_ == 0) {
            LOG(INFO) << "Migrated all batches";
            stop();
        }
    }
};

int main(int argc, char* argv[]) {
    SET_VERBOSITY_LEVEL(verbosity_INFO);
    td::set_default_failure_signal_handler().ensure();

    td::OptionParser p;
    std::string db_root;
    int new_compress_depth = 0;
    p.set_description("Migrate CellDB to another compress db value");
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
    p.add_checked_option('\0', "new-celldb-compress-depth", "New value of celldb compress depth", [&](td::Slice fname) { 
        int v;
        try {
            v = std::stoi(fname.str());
        } catch (...) {
            return td::Status::Error("bad value for --new-celldb-compress-depth: not a number");
        }
        new_compress_depth = v;
        return td::Status::OK();
    });
    auto S = p.run(argc, argv);
    if (S.is_error()) {
        LOG(ERROR) << "failed to parse options: " << S.move_as_error();
        std::_Exit(2);
    }
    if (db_root.empty()) {
        LOG(ERROR) << "db path is empty";
        std::_Exit(2);
    }
    if (new_compress_depth <= 0) {
        LOG(ERROR) << "new compress depth is invalid";
        std::_Exit(2);
    }

    td::actor::Scheduler scheduler({32});
    scheduler.run_in_context(
        [&] { td::actor::create_actor<MigrateCellDBActor>("migrate", db_root, new_compress_depth, 32).release(); });
    while (scheduler.run(1)) {
    }
    return 0;
}