#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "IndexData.h"
#include "td/utils/Status.h"

namespace clickhouse {
class Client;
}

namespace dex_snapshots {

struct SnapshotWriterOptions {
  std::string host{"127.0.0.1"};
  int port{9000};
  std::string user{"default"};
  std::string password;
  std::string database{"dex_snapshots"};
  std::string writer{"backfill"};  // pool_snapshots.writer
  std::size_t max_queued_rows{2'000'000};  // a block without pools counts as one row
  std::size_t max_batch_rows{100'000};
};

// "[tcp|clickhouse]://[user[:password]@]host[:port][/database]" (scheme optional); missing parts use the defaults above.
td::Result<SnapshotWriterOptions> parse_writer_dsn(const std::string& dsn);

struct BlockSnapshots {
  std::uint32_t mc_seqno;
  std::uint32_t utime;
  std::vector<schema::DexPoolSnapshot> pools;
};

BlockSnapshots parsed_block_snapshots(std::uint32_t mc_seqno, const ParsedBlock& block);

// Creates the schema; throws on a ClickHouse error.
void create_schema(clickhouse::Client& client, const std::string& database);
// Writes nothing for blocks without pools; throws on a ClickHouse error.
void insert_snapshots(clickhouse::Client& client, const std::string& database,
                      const std::vector<BlockSnapshots>& blocks, const std::string& writer);
// Logs one ERROR line per contiguous range of masterchain seqnos.
void log_dropped_blocks(const std::vector<BlockSnapshots>& blocks, const std::string& reason);

// Writes snapshots in offer order on its own thread; offer waits while the queue is full.
class SnapshotWriter {
 public:
  explicit SnapshotWriter(SnapshotWriterOptions options);
  ~SnapshotWriter();

  void start();
  // Writes what is queued, then stops the worker.
  void stop();

  void offer(std::vector<BlockSnapshots> blocks);

  void flush();

  struct Stats {
    std::uint64_t blocks_written{0};
    std::uint64_t blocks_dropped{0};
    std::uint64_t batches{0};
    std::uint64_t insert_us{0};
  };
  Stats stats() const;

 private:
  void run();
  td::Status write(const std::vector<BlockSnapshots>& batch);
  td::Status connect();

  SnapshotWriterOptions options_;
  std::unique_ptr<clickhouse::Client> client_;

  mutable std::mutex mutex_;
  std::condition_variable wake_;
  std::condition_variable idle_;
  std::condition_variable space_;
  std::deque<BlockSnapshots> queue_;
  std::size_t queued_rows_{0};
  bool busy_{false};
  bool stopping_{false};
  Stats stats_;
  std::thread worker_;
};

}
