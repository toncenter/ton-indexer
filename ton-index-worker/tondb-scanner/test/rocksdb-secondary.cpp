// Public-API regression tests for the RocksDB secondary reader patches.
// These deliberately require a merge base, like TON's cell-refcount operator.
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"

namespace {
void require(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void check(const rocksdb::Status& status) {
  require(status.ok(), status.ToString());
}

class RequireBase final : public rocksdb::MergeOperator {
 public:
  mutable std::atomic<unsigned> missing_base{0};

  const char* Name() const override {
    return "SecondaryTestRequireBase";
  }

  bool FullMergeV2(const MergeOperationInput& in, MergeOperationOutput* out) const override {
    if (in.existing_value == nullptr) {
      ++missing_base;
      return false;
    }
    auto value = std::stoll(in.existing_value->ToString());
    for (const auto& delta : in.operand_list) {
      value += std::stoll(delta.ToString());
    }
    out->new_value = std::to_string(value);
    return true;
  }
};

struct Fixture {
  std::string path;
  std::shared_ptr<RequireBase> op = std::make_shared<RequireBase>();
  rocksdb::Options options;
  std::unique_ptr<rocksdb::DB> primary;
  std::unique_ptr<rocksdb::DB> secondary;

  explicit Fixture(std::string root) : path(std::move(root)) {
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    options.write_buffer_size = 1 << 20;
    options.max_background_jobs = 2;
    options.compression = rocksdb::kNoCompression;
    options.merge_operator = op;
    rocksdb::DB* db = nullptr;
    check(rocksdb::DB::Open(options, path + "/primary", &db));
    primary.reset(db);
  }

  void open_secondary() {
    secondary.reset();
    rocksdb::DB* db = nullptr;
    check(rocksdb::DB::OpenAsSecondary(options, path + "/primary", path + "/secondary", &db));
    secondary.reset(db);
  }

  void flush(rocksdb::ColumnFamilyHandle* cf = nullptr) {
    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    check(primary->Flush(flush_options, cf ? cf : primary->DefaultColumnFamily()));
  }

  void seed_merge() {
    check(primary->Put({}, "key", "0"));
    flush();
    check(primary->Merge({}, "key", "1"));
    open_secondary();
    expect(*secondary, "1");
  }

  void expect(rocksdb::DB& db, const std::string& expected,
              rocksdb::ColumnFamilyHandle* cf = nullptr) {
    std::string actual;
    check(db.Get({}, cf ? cf : db.DefaultColumnFamily(), "key", &actual));
    require(actual == expected, "expected " + expected + ", got " + actual);
    require(op->missing_base == 0, "merge without base");
  }

  void absent(rocksdb::DB& db) {
    std::string value;
    auto status = db.Get({}, "key", &value);
    require(status.IsNotFound(), "expected NotFound, got " + status.ToString());
    require(op->missing_base == 0, "merge without base after delete");
  }
};

void flushed_merge(const std::string& path, bool compact) {
  Fixture f(path);
  f.seed_merge();
  check(f.primary->Merge({}, "key", "1"));
  f.flush();  // The next WAL has no records to rotate the secondary's memtable.
  if (compact) {
    check(f.primary->CompactRange({}, nullptr, nullptr));
  }
  f.expect(*f.primary, "2");
  for (int i = 0; i < 4; ++i) {
    check(f.secondary->TryCatchUpWithPrimary());
    f.expect(*f.secondary, "2");
  }
  f.open_secondary();
  f.expect(*f.secondary, "2");
}

void flushed_delete(const std::string& path) {
  Fixture f(path);
  f.seed_merge();
  check(f.primary->Delete({}, "key"));
  f.flush();
  check(f.primary->CompactRange({}, nullptr, nullptr));
  f.absent(*f.primary);
  for (int i = 0; i < 4; ++i) {
    check(f.secondary->TryCatchUpWithPrimary());
    f.absent(*f.secondary);
  }
  f.open_secondary();
  f.absent(*f.secondary);
}

void pinned_iterator(const std::string& path) {
  Fixture f(path);
  f.seed_merge();
  std::unique_ptr<rocksdb::Iterator> old(f.secondary->NewIterator({}));
  old->Seek("key");
  check(old->status());
  require(old->Valid() && old->value().ToString() == "1", "initial iterator");
  for (int value = 2; value <= 5; ++value) {
    check(f.primary->Merge({}, "key", "1"));
    f.flush();
    check(f.primary->CompactRange({}, nullptr, nullptr));
    check(f.secondary->TryCatchUpWithPrimary());
    f.expect(*f.secondary, std::to_string(value));
    // Reseek, so this tests the retained memtable/SST view, not a cached value.
    old->SeekToLast();
    old->SeekToFirst();
    check(old->status());
    require(old->Valid() && old->key().ToString() == "key" && old->value().ToString() == "1",
            "pinned SuperVersion lost its old memtable");
  }
  old.reset();
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "5");
}

void unflushed_new_wal(const std::string& path) {
  Fixture f(path);
  f.seed_merge();
  for (int value = 2; value <= 8; value += 2) {
    check(f.primary->Merge({}, "key", "1"));
    f.flush();
    check(f.primary->CompactRange({}, nullptr, nullptr));
    check(f.primary->Merge({}, "key", "1"));
    check(f.primary->FlushWAL(true));
    check(f.secondary->TryCatchUpWithPrimary());
    f.expect(*f.primary, std::to_string(value + 1));
    f.expect(*f.secondary, std::to_string(value + 1));
  }
}

void future_empty_wal(const std::string& path) {
  Fixture f(path);
  check(f.primary->Put({}, "key", "0"));
  f.flush();
  check(f.primary->Merge({}, "key", "1"));
  check(f.primary->FlushWAL(true));
  std::unique_ptr<rocksdb::WalFile> current;
  check(f.primary->GetCurrentWalFile(&current));
  std::ostringstream future;
  future << path << "/primary/" << std::setfill('0') << std::setw(6) << current->LogNumber() + 1000 << ".log";
  std::unique_ptr<rocksdb::WritableFile> file;
  check(f.options.env->NewWritableFile(future.str(), &file, {}));
  check(file->Close());
  f.open_secondary();
  for (int value = 1; value <= 4; ++value) {
    check(f.secondary->TryCatchUpWithPrimary());
    f.expect(*f.secondary, std::to_string(value));
    if (value < 4) {
      check(f.primary->Merge({}, "key", "1"));
      check(f.primary->FlushWAL(true));
    }
  }
}

void column_family_boundary(const std::string& path) {
  Fixture f(path);
  rocksdb::ColumnFamilyHandle* other_raw = nullptr;
  check(f.primary->CreateColumnFamily(f.options, "other", &other_raw));
  auto destroy_primary = [&](rocksdb::ColumnFamilyHandle* cf) { check(f.primary->DestroyColumnFamilyHandle(cf)); };
  std::unique_ptr<rocksdb::ColumnFamilyHandle, decltype(destroy_primary)> other(other_raw, destroy_primary);
  check(f.primary->Put({}, "key", "0"));
  check(f.primary->Put({}, other.get(), "key", "10"));
  f.flush();
  f.flush(other.get());
  check(f.primary->Merge({}, "key", "1"));
  check(f.primary->Merge({}, other.get(), "key", "1"));

  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors{{rocksdb::kDefaultColumnFamilyName, f.options},
                                                        {"other", f.options}};
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DB* db = nullptr;
  check(rocksdb::DB::OpenAsSecondary(f.options, path + "/primary", path + "/secondary", descriptors, &handles, &db));
  f.secondary.reset(db);
  auto destroy_secondary = [&](rocksdb::ColumnFamilyHandle* cf) { check(f.secondary->DestroyColumnFamilyHandle(cf)); };
  std::unique_ptr<rocksdb::ColumnFamilyHandle, decltype(destroy_secondary)> default_handle(handles[0], destroy_secondary);
  std::unique_ptr<rocksdb::ColumnFamilyHandle, decltype(destroy_secondary)> other_handle(handles[1], destroy_secondary);
  f.expect(*f.secondary, "1");
  f.expect(*f.secondary, "11", other_handle.get());
  // Advancing one CF's log boundary must preserve the other CF's unflushed data.
  f.flush(other.get());
  check(f.primary->CompactRange({}, other.get(), nullptr, nullptr));
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "1");
  f.expect(*f.secondary, "11", other_handle.get());
  check(f.primary->Merge({}, other.get(), "key", "1"));
  f.flush();
  check(f.primary->CompactRange({}, nullptr, nullptr));
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "1");
  f.expect(*f.secondary, "12", other_handle.get());

  // Another CF can advance MANIFEST's global sequence beyond the first
  // unreplayed record in a new WAL. The replacement memtable must accept
  // the whole pending sequence range, not start at that global sequence.
  check(f.primary->Merge({}, "key", "1"));
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "2");
  f.flush();
  for (int i = 0; i < 3; ++i) {
    check(f.primary->Merge({}, "key", "1"));
  }
  check(f.primary->Merge({}, other.get(), "key", "1"));
  f.flush(other.get());
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "5");
  f.expect(*f.secondary, "13", other_handle.get());
}

void empty_secondary(const std::string& path) {
  Fixture f(path);
  f.open_secondary();
  check(f.primary->Put({}, "key", "10"));
  f.flush();
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "10");
  check(f.primary->Merge({}, "key", "1"));
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "11");
  f.flush();
  check(f.primary->CompactRange({}, nullptr, nullptr));
  check(f.secondary->TryCatchUpWithPrimary());
  f.expect(*f.secondary, "11");
}
}  // namespace

int main(int argc, char** argv) {
  try {
    require(argc <= 2, "usage: test-rocksdb-secondary [NEW_TEST_ROOT]");
    std::string root;
    if (argc == 2) {
      root = argv[1];
    } else {
      check(rocksdb::Env::Default()->GetTestDirectory(&root));
      root += "/secondary-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }
    require(std::filesystem::create_directory(root), "test root already exists");
    const std::vector<std::pair<std::string, std::function<void(const std::string&)>>> cases{
        {"flushed-merge", [](const auto& p) { flushed_merge(p, false); }},
        {"compacted-merge", [](const auto& p) { flushed_merge(p, true); }},
        {"compacted-delete", flushed_delete},
        {"pinned-iterator", pinned_iterator},
        {"unflushed-new-wal", unflushed_new_wal},
        {"future-empty-wal", future_empty_wal},
        {"column-family-boundary", column_family_boundary},
        {"empty-secondary", empty_secondary},
    };
    unsigned failures = 0;
    for (const auto& [name, run] : cases) {
      std::filesystem::create_directory(root + "/" + name);
      try {
        run(root + "/" + name);
        std::cout << "{\"case\":" << std::quoted(name) << ",\"success\":true}\n";
      } catch (const std::exception& error) {
        ++failures;
        std::cout << "{\"case\":" << std::quoted(name) << ",\"success\":false,\"error\":"
                  << std::quoted(error.what()) << "}\n";
      }
      std::cout.flush();
    }
    if (failures == 0) {
      std::filesystem::remove_all(root);
    } else {
      std::cerr << failures << " failures; retained synthetic databases at " << root << '\n';
    }
    return failures == 0 ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
