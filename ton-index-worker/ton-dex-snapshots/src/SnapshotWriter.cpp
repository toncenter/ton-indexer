#include "SnapshotWriter.h"

#include <algorithm>
#include <chrono>
#include <sstream>

#include <clickhouse/client.h>

#include "PoolMath.h"
#include "convert-utils.h"
#include "td/utils/base64.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"

extern const char* const DEX_SNAPSHOTS_SCHEMA_SQL;  // generated from ../sql/schema.sql

namespace dex_snapshots {

namespace {

using namespace clickhouse;

std::vector<std::string> schema_statements(const std::string& database) {
  std::stringstream in(DEX_SNAPSHOTS_SCHEMA_SQL);
  std::string text;
  for (std::string line; std::getline(in, line);) {
    auto comment = line.find("--");
    text += (comment == std::string::npos ? line : line.substr(0, comment)) + "\n";
  }
  std::vector<std::string> statements;
  size_t begin = 0;
  for (size_t end; (end = text.find(';', begin)) != std::string::npos; begin = end + 1) {
    auto statement = text.substr(begin, end - begin);
    if (statement.find_first_not_of(" \n\t") == std::string::npos) {
      continue;
    }
    if (statement.find("CREATE DATABASE") != std::string::npos) {
      statement = "CREATE DATABASE IF NOT EXISTS " + database;
    }
    const std::string from = "dex_snapshots.";
    for (size_t at = 0; (at = statement.find(from, at)) != std::string::npos; at += database.size() + 1) {
      statement.replace(at, from.size(), database + ".");
    }
    statements.push_back(std::move(statement));
  }
  return statements;
}

// Returns 0 for null or out-of-range values (nullable columns handle null before calling this).
UInt128 to_uint128(const td::RefInt256& x, const char* column) {
  if (x.is_null()) {
    return 0;
  }
  unsigned char bytes[16];
  if (!x->is_valid() || td::sgn(x) < 0 || !x->export_bytes(bytes, 16, false)) {
    LOG(ERROR) << "DEX snapshots: " << column << " out of UInt128 range: " << x;
    return 0;
  }
  std::uint64_t hi = 0, lo = 0;
  for (int i = 0; i < 8; i++) {
    hi = (hi << 8) | bytes[i];
    lo = (lo << 8) | bytes[8 + i];
  }
  return absl::MakeUint128(hi, lo);
}

std::uint32_t to_uint32(const td::RefInt256& x, const char* column) {
  if (x.is_null()) {
    return 0;
  }
  if (!x->is_valid() || td::sgn(x) < 0 || !x->unsigned_fits_bits(32)) {
    LOG(ERROR) << "DEX snapshots: " << column << " out of UInt32 range: " << x;
    return 0;
  }
  return static_cast<std::uint32_t>(x->to_long());
}

void append_nullable_uint128(ColumnNullableT<ColumnUInt128>& column, const td::RefInt256& x, const char* name) {
  if (x.is_null()) {
    column.Append(std::nullopt);
  } else {
    column.Append(to_uint128(x, name));
  }
}

using LowCardinalityString = ColumnLowCardinalityT<ColumnString>;
template <class T>
using Nullable = ColumnNullableT<T>;

class Columns {
 public:
  template <class T, class... Args>
  std::shared_ptr<T> add(std::string name, Args&&... args) {
    auto column = std::make_shared<T>(std::forward<Args>(args)...);
    columns_.emplace_back(std::move(name), column);
    return column;
  }

  Block block() const {
    Block block;
    for (const auto& [name, column] : columns_) {
      block.AppendColumn(name, column);
    }
    return block;
  }

 private:
  std::vector<std::pair<std::string, ColumnRef>> columns_;
};

Block make_snapshot_block(const std::vector<BlockSnapshots>& batch, const std::string& writer_name) {
  Columns snapshots;
  auto pool = snapshots.add<ColumnString>("pool");
  auto mc_seqno = snapshots.add<ColumnUInt32>("mc_seqno");
  auto block_utime = snapshots.add<ColumnDateTime>("block_utime");
  auto last_tx_lt = snapshots.add<ColumnUInt64>("last_tx_lt");
  auto dex = snapshots.add<LowCardinalityString>("dex");
  auto dex_version = snapshots.add<ColumnUInt8>("dex_version");
  auto pool_type = snapshots.add<LowCardinalityString>("pool_type");
  auto code_hash = snapshots.add<ColumnFixedString>("code_hash", 44);
  auto router = snapshots.add<ColumnString>("router");
  auto removed = snapshots.add<ColumnUInt8>("removed");
  auto asset0 = snapshots.add<ColumnString>("asset0");
  auto asset1 = snapshots.add<ColumnString>("asset1");
  auto reserve0 = snapshots.add<ColumnUInt128>("reserve0");
  auto reserve1 = snapshots.add<ColumnUInt128>("reserve1");
  auto lp_fee = snapshots.add<ColumnUInt32>("lp_fee");
  auto protocol_fee = snapshots.add<ColumnUInt32>("protocol_fee");
  auto ref_fee = snapshots.add<ColumnUInt32>("ref_fee");
  auto fee_denominator = snapshots.add<ColumnUInt32>("fee_denominator");
  auto amp = snapshots.add<Nullable<ColumnUInt128>>("amp");
  auto rate = snapshots.add<Nullable<ColumnUInt128>>("rate");
  auto w0 = snapshots.add<Nullable<ColumnUInt128>>("w0");
  auto is_locked = snapshots.add<ColumnUInt8>("is_locked");
  auto lp_supply = snapshots.add<Nullable<ColumnUInt128>>("lp_supply");
  auto spot = snapshots.add<Nullable<ColumnFloat64>>("spot");
  auto depth0 = snapshots.add<Nullable<ColumnFloat64>>("depth0");
  auto depth1 = snapshots.add<Nullable<ColumnFloat64>>("depth1");
  auto math_version = snapshots.add<ColumnUInt16>("math_version");
  auto writer = snapshots.add<LowCardinalityString>("writer");

  for (const auto& b : batch) {
    for (const auto& s : b.pools) {
      pool->Append(convert::to_raw_address(s.address));
      mc_seqno->Append(b.mc_seqno);
      block_utime->Append(static_cast<std::time_t>(b.utime));
      last_tx_lt->Append(s.last_transaction_lt);
      dex->Append(s.dex);
      dex_version->Append(static_cast<std::uint8_t>(s.dex_version));
      pool_type->Append(s.pool_type);
      code_hash->Append(td::base64_encode(s.code_hash.as_slice()));
      router->Append(s.router ? convert::to_raw_address(*s.router) : std::string());
      removed->Append(s.removed ? 1 : 0);
      asset0->Append(s.asset0);
      asset1->Append(s.asset1);
      reserve0->Append(to_uint128(s.reserve0, "reserve0"));
      reserve1->Append(to_uint128(s.reserve1, "reserve1"));
      lp_fee->Append(to_uint32(s.lp_fee, "lp_fee"));
      protocol_fee->Append(to_uint32(s.protocol_fee, "protocol_fee"));
      ref_fee->Append(to_uint32(s.ref_fee, "ref_fee"));
      fee_denominator->Append(to_uint32(s.fee_denominator, "fee_denominator"));
      append_nullable_uint128(*amp, s.amp, "amp");
      append_nullable_uint128(*rate, s.rate, "rate");
      append_nullable_uint128(*w0, s.w0, "w0");
      is_locked->Append(s.is_locked ? 1 : 0);
      append_nullable_uint128(*lp_supply, s.lp_supply, "lp_supply");

      SpotDepth p;
      if (!s.removed) {
        p = spot_depth(PoolInputs{.dex = s.dex, .dex_version = s.dex_version, .pool_type = s.pool_type,
                             .reserve0 = s.reserve0, .reserve1 = s.reserve1, .amp = s.amp, .rate = s.rate,
                             .w0 = s.w0});
      }
      spot->Append(p.spot);
      depth0->Append(p.depth0);
      depth1->Append(p.depth1);
      math_version->Append(p.math_version);
      writer->Append(writer_name);
    }
  }
  return snapshots.block();
}

// Empty blocks cost one row so they cannot grow the queue without a bound.
std::size_t queue_cost(const BlockSnapshots& block) {
  return std::max<std::size_t>(block.pools.size(), 1);
}

}

td::Result<SnapshotWriterOptions> parse_writer_dsn(const std::string& dsn) {
  SnapshotWriterOptions options;
  std::string rest = dsn;
  for (const std::string scheme : {"tcp://", "clickhouse://"}) {
    if (rest.rfind(scheme, 0) == 0) {
      rest = rest.substr(scheme.size());
      break;
    }
  }
  if (auto slash = rest.find('/'); slash != std::string::npos) {
    if (slash + 1 < rest.size()) {
      options.database = rest.substr(slash + 1);
    }
    rest = rest.substr(0, slash);
  }
  if (auto at = rest.rfind('@'); at != std::string::npos) {
    auto credentials = rest.substr(0, at);
    rest = rest.substr(at + 1);
    auto colon = credentials.find(':');
    options.user = credentials.substr(0, colon);
    if (colon != std::string::npos) {
      options.password = credentials.substr(colon + 1);
    }
  }
  if (auto colon = rest.rfind(':'); colon != std::string::npos) {
    auto port = td::to_integer_safe<int>(rest.substr(colon + 1));
    if (port.is_error() || port.ok() <= 0 || port.ok() > 65535) {
      return td::Status::Error("dex snapshots clickhouse dsn: bad port");
    }
    options.port = port.ok();
    rest = rest.substr(0, colon);
  }
  if (!rest.empty()) {
    options.host = rest;
  }
  for (char c : options.database) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
      return td::Status::Error("dex snapshots clickhouse dsn: database must be [A-Za-z0-9_]");
    }
  }
  return options;
}

SnapshotWriter::SnapshotWriter(SnapshotWriterOptions options) : options_(std::move(options)) {
}

SnapshotWriter::~SnapshotWriter() {
  stop();
}

void SnapshotWriter::start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!worker_.joinable()) {
    stopping_ = false;
    worker_ = std::thread([this] { run(); });
  }
}

void SnapshotWriter::stop() {
  flush();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  wake_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
}

void SnapshotWriter::offer(std::vector<BlockSnapshots> blocks) {
  std::size_t rows = 0;
  for (const auto& block : blocks) {
    rows += queue_cost(block);
  }
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // An empty queue accepts an oversized block.
    space_.wait(lock, [&] { return queued_rows_ + rows <= options_.max_queued_rows || queue_.empty(); });
    queued_rows_ += rows;
    for (auto& block : blocks) {
      queue_.push_back(std::move(block));
    }
  }
  wake_.notify_one();
}

void SnapshotWriter::flush() {
  std::unique_lock<std::mutex> lock(mutex_);
  idle_.wait(lock, [&] { return (queue_.empty() && !busy_) || !worker_.joinable(); });
}

SnapshotWriter::Stats SnapshotWriter::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

void SnapshotWriter::run() {
  while (true) {
    std::vector<BlockSnapshots> batch;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      wake_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
      if (stopping_) {
        return;
      }
      std::size_t rows = 0;
      while (!queue_.empty() && (batch.empty() || rows + queue_cost(queue_.front()) <= options_.max_batch_rows)) {
        rows += queue_cost(queue_.front());
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
      queued_rows_ -= rows;
      busy_ = true;
    }
    space_.notify_all();
    const auto insert_started = std::chrono::steady_clock::now();
    auto status = write(batch);
    const auto insert_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - insert_started).count();
    if (status.is_error()) {
      client_.reset();
      log_dropped_blocks(batch, status.message().str());
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.batches++;
      stats_.insert_us += insert_us;
      if (status.is_ok()) {
        stats_.blocks_written += batch.size();
      } else {
        stats_.blocks_dropped += batch.size();
      }
      busy_ = false;
    }
    idle_.notify_all();
    if (status.is_error()) {
      std::unique_lock<std::mutex> lock(mutex_);
      wake_.wait_for(lock, std::chrono::seconds(5), [&] { return stopping_; });
    }
  }
}

td::Status SnapshotWriter::connect() {
  if (client_) {
    return td::Status::OK();
  }
  try {
    ClientOptions client_options;
    client_options.SetHost(options_.host);
    client_options.SetPort(options_.port);
    client_options.SetUser(options_.user);
    client_options.SetPassword(options_.password);
    client_options.SetDefaultDatabase("default");
    client_options.SetConnectionConnectTimeout(std::chrono::seconds(5));
    client_options.SetConnectionRecvTimeout(std::chrono::seconds(60));
    client_options.SetConnectionSendTimeout(std::chrono::seconds(60));
    auto client = std::make_unique<Client>(client_options);
    create_schema(*client, options_.database);
    client_ = std::move(client);
  } catch (const std::exception& e) {
    return td::Status::Error(PSLICE() << "connect: " << e.what());
  }
  return td::Status::OK();
}

td::Status SnapshotWriter::write(const std::vector<BlockSnapshots>& batch) {
  TRY_STATUS(connect());
  try {
    insert_snapshots(*client_, options_.database, batch, options_.writer);
  } catch (const std::exception& e) {
    return td::Status::Error(PSLICE() << "insert: " << e.what());
  }
  return td::Status::OK();
}

void log_dropped_blocks(const std::vector<BlockSnapshots>& blocks, const std::string& reason) {
  std::vector<std::uint32_t> seqnos;
  for (const auto& block : blocks) {
    seqnos.push_back(block.mc_seqno);
  }
  std::sort(seqnos.begin(), seqnos.end());
  auto one_line_reason = reason.substr(0, reason.find_first_of("\r\n\t"));
  for (size_t i = 0; i < seqnos.size();) {
    size_t j = i;
    while (j + 1 < seqnos.size() && seqnos[j + 1] <= seqnos[j] + 1) {
      j++;
    }
    LOG(ERROR) << "DEX snapshots writer: dropped mc seqno " << seqnos[i] << ".." << seqnos[j]
               << ": " << one_line_reason;
    i = j + 1;
  }
}

BlockSnapshots parsed_block_snapshots(std::uint32_t mc_seqno, const ParsedBlock& block) {
  // The parser puts the masterchain block first.
  return {.mc_seqno = mc_seqno, .utime = static_cast<std::uint32_t>(block.blocks_.front().gen_utime),
          .pools = block.dex_pool_snapshots_};
}

void create_schema(Client& client, const std::string& database) {
  for (const auto& statement : schema_statements(database)) {
    client.Execute(statement);
  }
}

void insert_snapshots(Client& client, const std::string& database, const std::vector<BlockSnapshots>& blocks,
                      const std::string& writer) {
  auto snapshots = make_snapshot_block(blocks, writer);
  if (snapshots.GetRowCount() > 0) {
    client.Insert(database + ".pool_snapshots", snapshots);
  }
}

}
