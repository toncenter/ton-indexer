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



int main(int argc, char* argv[]) {
    SET_VERBOSITY_LEVEL(verbosity_INFO);
    td::set_default_failure_signal_handler().ensure();

    td::OptionParser p;
    std::string db_root;
    p.set_description("Migrate DB from block based to plain table");
    p.add_option('\0', "help", "prints_help", [&]() {
        char b[10240];
        td::StringBuilder sb(td::MutableSlice{b, 10000});
        sb << p;
        std::cout << sb.as_cslice().c_str();
        std::exit(2);
    });
    p.add_option('\0', "db", "Path to DB folder", [&](td::Slice fname) { 
        db_root = fname.str();
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

    rocksdb::DB* db_raw;
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(32ULL << 30); // 8GB cache
    // table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false)); // 10 bits per key
    table_options.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    table_options.partition_filters = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));


    rocksdb::Status status = rocksdb::DB::Open(options, db_root, &db_raw);
    if (!status.ok()) {
        LOG(FATAL) << "failed to open src db: " << status.ToString();
    }
    auto db = std::shared_ptr<rocksdb::DB>(db_raw);
    // rocksdb::CompactRangeOptions compact_options;
    // compact_options.max_subcompactions = 24;
    // compact_options.exclusive_manual_compaction = true; // Bypass automatic compactions
    // compact_options.change_level = true;
    // compact_options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
    // status = db->CompactRange(compact_options, nullptr, nullptr); // Compact entire key range
    // if (!status.ok()) {
    //     LOG(FATAL) << "failed to compact db: " << status.ToString();
    // }
    // status = db->Flush({});
    // if (!status.ok()) {
    //     LOG(FATAL) << "failed to flush db: " << status.ToString();
    // }
    // status = db->WaitForCompact({});
    // if (!status.ok()) {
    //     LOG(FATAL) << "failed to wait for compaction: " << status.ToString();
    // }
    // status = db->Close();
    // if (!status.ok()) {
    //     LOG(FATAL) << "failed to close db: " << status.ToString();
    // }

    std::string block_cache_usage;
    db->GetProperty("rocksdb.block-cache-usage", &block_cache_usage);
    LOG(INFO) << "rocksdb.block_cache_usage: " << block_cache_usage;
    std::string table_readers_mem;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &table_readers_mem);
    LOG(INFO) << "rocksdb.estimate-table-readers-mem: " << table_readers_mem;
    std::string memtables_mem;
    db->GetProperty("rocksdb.cur-size-all-mem-tables", &memtables_mem);
    LOG(INFO) << "rocksdb.cur-size-all-mem-tables: " << memtables_mem;
    std::string pinned_mem;
    db->GetProperty("rocksdb.block-cache-pinned-usage", &pinned_mem);
    LOG(INFO) << "rocksdb.block-cache-pinned-usage: " << pinned_mem;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}