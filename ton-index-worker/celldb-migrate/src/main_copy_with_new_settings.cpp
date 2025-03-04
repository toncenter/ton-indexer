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

#include <rocksdb/table.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/db.h>

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
    std::shared_ptr<rocksdb::DB> src_;
    std::shared_ptr<rocksdb::DB> dst_;
    std::string lower_;
    std::string upper_;
    td::Promise<td::Unit> promise_;

    std::unique_ptr<vm::CellLoader> loader_;
    std::shared_ptr<vm::DynamicBagOfCellsDb> boc_;

    uint32_t migrated_{0};
public:
    MigrateBatchActor(std::shared_ptr<rocksdb::DB> src, std::shared_ptr<rocksdb::DB> dst, 
                      std::string lower, std::string upper, td::Promise<td::Unit> promise)
        : src_(src), dst_(dst), lower_(lower), upper_(upper), promise_(std::move(promise)) {

        
    }

    void start_up() override {
        rocksdb::ReadOptions options;
        rocksdb::Slice lb_slice(lower_);
        rocksdb::Slice ub_slice(upper_);
        options.iterate_lower_bound = &lb_slice;
        options.iterate_upper_bound = &ub_slice;

        rocksdb::Iterator* it = src_->NewIterator(options);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            if (it->key().size() != 32) {
                LOG(WARNING) << "CellDb: skipping key with size " << it->key().size();
                continue;
            }
            dst_->Put(rocksdb::WriteOptions(), it->key(), it->value());
        }
        delete it;

        promise_.set_value(td::Unit());
    }
};

class MigrateCellDBActor: public td::actor::Actor {
    std::string db_src_root_;
    std::string db_dst_root_;
    int max_parallel_batches_;

    std::shared_ptr<rocksdb::DB> db_src_;
    std::shared_ptr<rocksdb::DB> db_dst_;

    uint32_t migrated_{0};

    td::Bits256 current_;

    int cur_parallel_batches_{0};
public:
    MigrateCellDBActor(std::string db_src_root, std::string db_dst_root, int max_parallel_batches)
                : db_src_root_(db_src_root), db_dst_root_(db_dst_root), max_parallel_batches_(max_parallel_batches) {
        rocksdb::DB* src_db;
        rocksdb::Options src_options;
        src_options.create_if_missing = false;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = rocksdb::NewLRUCache(8 << 30); // 8GB cache
        src_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        rocksdb::Status status = rocksdb::DB::Open(src_options, db_src_root_, &src_db);
        if (!status.ok()) {
            LOG(FATAL) << "failed to open src db: " << status.ToString();
        }
        db_src_ = std::shared_ptr<rocksdb::DB>(src_db);

        rocksdb::DB* dst_db;
        rocksdb::Options dst_options;

        // rocksdb::PlainTableOptions plain_table_options;
        // plain_table_options.user_key_len = 32;  // Full key length (256 bits = 32 bytes)
        // plain_table_options.bloom_bits_per_key = 10;  // Optional Bloom filter
        // plain_table_options.store_index_in_file = true;
        // dst_options.table_factory.reset(rocksdb::NewPlainTableFactory(plain_table_options));
        // dst_options.allow_mmap_reads = true;  // Required for Plain Table
        // dst_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(8));  // 8-byte prefix

        rocksdb::BlockBasedTableOptions dst_table_options;
        dst_table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        dst_table_options.block_size = 2 * 1024; // 2KB block size
        dst_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(dst_table_options));
        dst_options.compression = rocksdb::kNoCompression;
        dst_options.max_background_compactions = 4;
        dst_options.create_if_missing = true;
        dst_options.max_background_flushes = 2;


        status = rocksdb::DB::Open(dst_options, db_dst_root_, &dst_db);
        if (!status.ok()) {
            LOG(FATAL) << "failed to open dst db: " << status.ToString();
        }
        db_dst_ = std::shared_ptr<rocksdb::DB>(dst_db);
    }

    void start_up() override {
        // Get first and last keys
        std::string first_key, last_key;
        rocksdb::Iterator* it = db_src_->NewIterator(rocksdb::ReadOptions());
        it->SeekToFirst();
        if (it->Valid()) {
            first_key = it->key().ToString();
        } else {
            LOG(FATAL) << "Empty src database";
        }
        it->SeekToLast();
        if (it->Valid()) {
            last_key = it->key().ToString();
        } else {
            LOG(FATAL) << "Empty src database";
        }
        delete it;

        // Estimate number of keys
        uint64_t num_keys;
        db_src_->GetIntProperty("rocksdb.estimate-num-keys", &num_keys);
        uint64_t step = num_keys / max_parallel_batches_;

        // Collect split keys
        std::vector<std::string> split_keys;
        it = db_src_->NewIterator(rocksdb::ReadOptions());
        it->SeekToFirst();
        for (int i = 1; i < max_parallel_batches_; ++i) {
            for (uint64_t j = 0; j < step && it->Valid(); ++j) {
                it->Next();
            }
            if (!it->Valid()) break;
            split_keys.push_back(it->key().ToString());
            LOG(INFO) << "Found split key for batch " << i;
        }
        delete it;

        // Adjust for actual split points collected
        const int actual_threads = split_keys.size() + 1;
        std::vector<std::thread> threads;
        for (int i = 0; i < actual_threads; ++i) {
            std::string lower, upper;
            if (i == 0) {
                lower = first_key;
            } else {
                lower = split_keys[i-1];
            }
            if (i == actual_threads - 1) {
                upper = last_key;
            } else {
                upper = split_keys[i];
            }

            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), lower, upper](td::Result<td::Unit> R) {
                if (R.is_error()) {
                    LOG(ERROR) << "failed to migrate batch from " << lower << " to " << upper << ": " << R.error();
                } else {
                    LOG(INFO) << "migrated batch from " << lower << " to " << upper;
                }
                td::actor::send_closure(SelfId, &MigrateCellDBActor::on_batch_migrated);
            });

            LOG(INFO) << "Deploying batch from " << lower << " to " << upper;
            LOG(INFO) << lower.size() << " " << upper.size();

            td::actor::create_actor<MigrateBatchActor>("migrate", db_src_, db_dst_, lower, upper, std::move(P)).release();
            cur_parallel_batches_++;
        }
    }

    void on_batch_migrated() {
        cur_parallel_batches_--;

        if (cur_parallel_batches_ == 0) {
            LOG(INFO) << "Migrated all batches";
            stop();
        }

        auto status = db_dst_->Flush(rocksdb::FlushOptions());
        if (!status.ok()) {
            LOG(ERROR) << "failed to flush db: " << status.ToString();
        }

        // run compaction on dst db
        rocksdb::CompactRangeOptions options;
        status = db_dst_->CompactRange(options, nullptr, nullptr);
        if (!status.ok()) {
            LOG(ERROR) << "failed to compact db: " << status.ToString();
        }
        status = db_dst_->WaitForCompact({});
        if (!status.ok()) {
            LOG(ERROR) << "failed to wait for compaction: " << status.ToString();
        }
        status = db_src_->Close();
        if (!status.ok()) {
            LOG(ERROR) << "failed to close src db: " << status.ToString();
        }

        status = db_dst_->Close();
        if (!status.ok()) {
            LOG(ERROR) << "failed to close dst db: " << status.ToString();
        }

        std::exit(0);
    }
};

int main(int argc, char* argv[]) {
    SET_VERBOSITY_LEVEL(verbosity_INFO);
    td::set_default_failure_signal_handler().ensure();

    td::OptionParser p;
    std::string db_src_root;
    std::string db_dst_root;
    p.set_description("Migrate DB from block based to plain table");
    p.add_option('\0', "help", "prints_help", [&]() {
        char b[10240];
        td::StringBuilder sb(td::MutableSlice{b, 10000});
        sb << p;
        std::cout << sb.as_cslice().c_str();
        std::exit(2);
    });
    p.add_option('S', "src", "Path to source DB folder", [&](td::Slice fname) { 
        db_src_root = fname.str();
    });
    p.add_option('D', "dst", "Path to dest DB folder", [&](td::Slice fname) { 
        db_dst_root = fname.str();
    });
    
    auto S = p.run(argc, argv);
    if (S.is_error()) {
        LOG(ERROR) << "failed to parse options: " << S.move_as_error();
        std::_Exit(2);
    }
    if (db_src_root.empty()) {
        LOG(ERROR) << "src db path is empty";
        std::_Exit(2);
    }

    if (db_dst_root.empty()) {
        LOG(ERROR) << "dst db path is empty";
        std::_Exit(2);
    }

    td::actor::Scheduler scheduler({32});
    scheduler.run_in_context(
        [&] { td::actor::create_actor<MigrateCellDBActor>("migrate", db_src_root, db_dst_root, 32).release(); });
    while (scheduler.run(1)) {
    }
    return 0;
}