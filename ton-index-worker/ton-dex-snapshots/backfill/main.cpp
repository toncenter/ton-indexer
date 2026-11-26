// ClickHouse-only backfill of DEX pool snapshots for masterchain blocks --from..--to. Blocks are parsed in parallel
// and detected in order, so the pool cache and its removal rows follow the chain.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <utility>
#include <unordered_map>
#include <thread>
#include <sys/resource.h>
#include <unordered_set>

#include "block/block-parse.h"
#include "crypto/vm/cp0.h"
#include "ton/ton-shard.h"
#include "td/actor/actor.h"
#include "td/utils/OptionParser.h"
#include "td/utils/filesystem.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/port/signals.h"

#include "DataParser.h"
#include "DbScanner.h"
#include "SnapshotWriter.h"
#include "smc-interfaces/DexConstants.h"
#include "smc-interfaces/DexPools.h"
#include "smc-interfaces/Stonfi.h"
#include "smc-interfaces/Tokens.h"

namespace {

// The actors only add to these counters; a separate thread prints them, so it keeps its pace while every scheduler
// thread reads listed accounts.
struct Progress {
  std::atomic<std::uint64_t> accounts_total{0};
  std::atomic<std::uint64_t> accounts_read{0};
  std::atomic<std::uint64_t> blocks_offered{0};
  std::atomic<std::uint64_t> pool_rows{0};
  std::atomic<std::uint32_t> last_utime{0};
  // For the stages line: stage times summed over blocks (microseconds), parsed blocks waiting for an earlier one.
  std::atomic<std::uint64_t> read_us{0}, parse_us{0}, detect_us{0}, offer_us{0}, parsed_blocks{0}, waiting{0};
};

std::uint64_t now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
Progress g_progress;

AllShardStates shard_states_of(const ParsedBlock& block) {
  AllShardStates out;
  for (const auto& shard : block.mc_block_.shard_blocks_) {
    out.push_back(shard.block_state);
  }
  return out;
}

std::optional<schema::DexPoolSnapshot> pool_snapshot(const schema::AccountState& state, const AllShardStates& shard_states,
                                                     const std::shared_ptr<block::ConfigInfo>& config) {
  const auto code_hash = state.code_hash.value_or(td::Bits256::zero());
  if (state.code.is_null() || state.data.is_null() || !dex_constants::is_pool_code_hash(code_hash.to_hex())) {
    return std::nullopt;
  }
  if (auto dedust = DedustPoolDetector::detect(state.account, state.code, state.data, shard_states, config); dedust.is_ok()) {
    return make_dex_pool_snapshot(dedust.ok(), state.last_trans_lt, code_hash);
  }
  if (auto stonfi = StonfiPoolDetector::detect(state.account, state.code, state.data, shard_states, config); stonfi.is_ok()) {
    return make_dex_pool_snapshot(stonfi.ok(), state.last_trans_lt, code_hash);
  }
  return std::nullopt;
}

// BlockInterfaceProcessor's rule for pools: each account's newest state is a trusted pool or ends a cached one.
dex_snapshots::BlockSnapshots block_snapshots(std::uint32_t mc_seqno, const ParsedBlock& block,
                                           const std::unordered_set<block::StdAddress>& skip) {
  // The parser puts the masterchain block first.
  dex_snapshots::BlockSnapshots out{.mc_seqno = mc_seqno, .utime = static_cast<std::uint32_t>(block.blocks_.front().gen_utime)};
  const auto shard_states = shard_states_of(block);
  std::unordered_map<block::StdAddress, const schema::AccountState*> latest;
  for (const auto& state : block.account_states_) {
    auto& l = latest[state.account];
    if (l == nullptr || state.last_trans_lt > l->last_trans_lt) {
      l = &state;
    }
  }
  for (const auto& [address, state] : latest) {
    if (skip.contains(address)) {
      continue;
    }
    if (auto pool = pool_snapshot(*state, shard_states, block.mc_block_.config_)) {
      out.pools.push_back(std::move(*pool));
    } else if (auto gone = make_dex_pool_removal(address, state->code_hash.value_or(td::Bits256::zero()), state->last_trans_lt)) {
      out.pools.push_back(std::move(*gone));
    }
  }
  return out;
}

// The listed accounts that are trusted pools in the block's shard states.
td::Result<std::vector<schema::DexPoolSnapshot>> listed_snapshots(const ParsedBlock& block,
                                                                  const std::vector<block::StdAddress>& accounts) {
  const auto shard_states = shard_states_of(block);
  std::vector<schema::DexPoolSnapshot> out;
  try {
    for (const auto& shard : block.mc_block_.shard_blocks_) {
      block::gen::ShardStateUnsplit::Record state;
      if (!tlb::unpack_cell(shard.block_state, state)) {
        return td::Status::Error("Failed to unpack shard state " + shard.handle->id().to_str());
      }
      vm::AugmentedDictionary dict{vm::load_cell_slice_ref(state.accounts), 256, block::tlb::aug_ShardAccounts};
      for (const auto& address : accounts) {
        if (!ton::shard_contains(shard.handle->id().shard_full(), ton::extract_addr_prefix(address.workchain, address.addr))) {
          continue;
        }
        g_progress.accounts_read.fetch_add(1, std::memory_order_relaxed);
        auto value = dict.lookup(address.addr);
        if (value.is_null()) {
          continue;
        }
        block::gen::ShardAccount::Record account;
        if (!tlb::csr_unpack(std::move(value), account)) {
          return td::Status::Error("Failed to unpack ShardAccount of " + address.rserialize());
        }
        if (block::gen::t_Account.get_tag(vm::load_cell_slice(account.account)) != block::gen::Account::account) {
          continue;
        }
        TRY_RESULT(parsed, ParseQuery::parse_account(account.account, state.gen_utime, account.last_trans_hash,
                                                     account.last_trans_lt));
        if (auto pool = pool_snapshot(parsed, shard_states, block.mc_block_.config_)) {
          out.push_back(std::move(*pool));
        }
      }
    }
  } catch (const vm::VmError& e) {
    return td::Status::Error(PSLICE() << "error while reading listed accounts: " << e.get_msg());
  }
  return out;
}

class ReadListed : public td::actor::Actor {
 public:
  ReadListed(ParsedBlockPtr block, std::vector<block::StdAddress> accounts,
             td::Promise<std::vector<schema::DexPoolSnapshot>> promise)
      : block_(std::move(block)), accounts_(std::move(accounts)), promise_(std::move(promise)) {
  }
  void start_up() override {
    promise_.set_result(listed_snapshots(*block_, accounts_));
    stop();
  }

 private:
  ParsedBlockPtr block_;
  std::vector<block::StdAddress> accounts_;
  td::Promise<std::vector<schema::DexPoolSnapshot>> promise_;
};

class Backfill : public td::actor::Actor {
 public:
  Backfill(td::actor::ActorId<DbScanner> db, td::actor::ActorId<ParseManager> parser, std::uint32_t from,
           std::uint32_t to, std::uint32_t parallel, std::vector<block::StdAddress> accounts,
           std::shared_ptr<dex_snapshots::SnapshotWriter> writer, td::Promise<td::Unit> done)
      : db_(db), parser_(parser), from_(from), next_fetch_(from), next_write_(from), to_(to), parallel_(parallel),
        accounts_(std::move(accounts)), listed_(accounts_.begin(), accounts_.end()), writer_(std::move(writer)),
        done_(std::move(done)) {
  }

  void start_up() override {
    schedule();
  }

  // Buffers parsed blocks and detects them in seqno order. Block --from pauses once to snapshot --accounts-file,
  // then re-enters here with accounts_ empty to merge listed_pools_ and drain ready_.
  void parsed(std::uint32_t seqno, ParsedBlockPtr block) {
    attempts_.erase(seqno);
    if (seqno == from_ && !accounts_.empty()) {
      read_listed(std::move(block));
      return;
    }
    ready_.emplace(seqno, std::move(block));
    std::vector<dex_snapshots::BlockSnapshots> out;
    const auto detect_started = now_us();
    for (auto it = ready_.find(next_write_); it != ready_.end(); it = ready_.find(next_write_)) {
      out.push_back(block_snapshots(next_write_, *it->second, listed_));
      if (next_write_ == from_) {
        auto& pools = out.back().pools;
        pools.insert(pools.end(), std::make_move_iterator(listed_pools_.begin()), std::make_move_iterator(listed_pools_.end()));
        listed_pools_.clear();
        listed_.clear();
      }
      ready_.erase(it);
      next_write_++;
    }
    g_progress.detect_us += now_us() - detect_started;
    g_progress.waiting = ready_.size();
    if (!out.empty()) {
      std::uint64_t rows = 0;
      for (const auto& b : out) {
        rows += b.pools.size();
      }
      g_progress.pool_rows += rows;
      g_progress.last_utime = out.back().utime;
      g_progress.blocks_offered += out.size();
      const auto offer_started = now_us();
      writer_->offer(std::move(out));
      g_progress.offer_us += now_us() - offer_started;
    }
    if (next_write_ > to_) {
      LOG(INFO) << "All " << (to_ - from_ + 1) << " blocks parsed: " << g_progress.pool_rows.load()
                << " pool rows; waiting for the writer";
      done_.set_value(td::Unit());
      stop();
      return;
    }
    schedule();
  }

  void listed_read(ParsedBlockPtr block, td::Result<std::vector<schema::DexPoolSnapshot>> pools) {
    if (pools.is_error()) {
      done_.set_error(pools.move_as_error_prefix(PSLICE() << "mc seqno " << from_ << ": "));
      stop();
      return;
    }
    for (auto& pool : pools.move_as_ok()) {
      listed_pools_.push_back(std::move(pool));
    }
    if (--listed_running_ == 0) {
      LOG(INFO) << "mc seqno " << from_ << ": " << listed_pools_.size() << " of " << listed_.size()
                << " listed accounts are trusted pools";
      parsed(from_, std::move(block));
    }
  }

  void failed(std::uint32_t seqno, td::Status error) {
    if (++attempts_[seqno] < 5) {
      LOG(WARNING) << "mc seqno " << seqno << ": " << error << "; retrying";
      fetch(seqno);
      return;
    }
    done_.set_error(error.move_as_error_prefix(PSLICE() << "mc seqno " << seqno << ": "));
    stop();
  }

 private:
  // The listed accounts are read and detected in `parallel` parts; the pool cache is thread-safe.
  void read_listed(ParsedBlockPtr block) {
    const auto part = (accounts_.size() + parallel_ - 1) / parallel_;
    g_progress.accounts_total = accounts_.size();
    LOG(INFO) << "mc seqno " << from_ << ": reading " << accounts_.size() << " listed accounts";
    for (std::size_t i = 0; i < accounts_.size(); i += part) {
      std::vector<block::StdAddress> accounts(accounts_.begin() + i, accounts_.begin() + std::min(i + part, accounts_.size()));
      ++listed_running_;
      td::actor::create_actor<ReadListed>("readlisted", block, std::move(accounts),
          td::PromiseCreator::lambda([SelfId = actor_id(this), block](td::Result<std::vector<schema::DexPoolSnapshot>> R) {
            td::actor::send_closure(SelfId, &Backfill::listed_read, block, std::move(R));
          })).release();
    }
    accounts_.clear();
  }

  // Bounds the sliding window from next_write_ (not just in-flight fetches) so ready_ cannot grow unboundedly.
  void schedule() {
    while (next_fetch_ <= to_ && next_fetch_ - next_write_ < parallel_) {
      fetch(next_fetch_++);
    }
  }

  void fetch(std::uint32_t seqno) {
    const auto started = now_us();
    auto on_parsed = [SelfId = actor_id(this), seqno](td::Result<ParsedBlockPtr> R) {
      g_progress.parsed_blocks++;
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &Backfill::failed, seqno, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &Backfill::parsed, seqno, R.move_as_ok());
      }
    };
    td::actor::send_closure(db_, &DbScanner::fetch_seqno, seqno,
        [SelfId = actor_id(this), db = db_, parser = parser_, seqno, started, on_parsed = std::move(on_parsed)](
            td::Result<schema::MasterchainBlockDataState> R) mutable {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &Backfill::failed, seqno, R.move_as_error());
            return;
          }
          td::actor::send_closure(db, &DbScanner::get_cell_db_reader,
              [SelfId, parser, seqno, started, state = R.move_as_ok(), on_parsed = std::move(on_parsed)](
                  td::Result<std::shared_ptr<vm::CellDbReader>> reader) mutable {
                const auto got_reader = now_us();
                g_progress.read_us += got_reader - started;
                if (reader.is_error()) {
                  td::actor::send_closure(SelfId, &Backfill::failed, seqno, reader.move_as_error());
                  return;
                }
                td::actor::send_closure(parser, &ParseManager::parse, seqno, std::move(state), reader.move_as_ok(),
                                        td::PromiseCreator::lambda([got_reader, on_parsed = std::move(on_parsed)](
                                            td::Result<ParsedBlockPtr> R) mutable {
                                          g_progress.parse_us += now_us() - got_reader;
                                          on_parsed(std::move(R));
                                        }));
              });
        });
  }

  td::actor::ActorId<DbScanner> db_;
  td::actor::ActorId<ParseManager> parser_;
  std::uint32_t from_, next_fetch_, next_write_, to_, parallel_;
  // --accounts-file: block --from adds the listed pools and skips them among its touched accounts.
  std::vector<block::StdAddress> accounts_;
  std::unordered_set<block::StdAddress> listed_;
  std::vector<schema::DexPoolSnapshot> listed_pools_;
  std::size_t listed_running_{0};
  std::shared_ptr<dex_snapshots::SnapshotWriter> writer_;
  td::Promise<td::Unit> done_;
  std::map<std::uint32_t, ParsedBlockPtr> ready_;
  std::map<std::uint32_t, int> attempts_;
};

std::string format_duration(double seconds) {
  const auto s = static_cast<long long>(seconds);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%lld:%02lld:%02lld", s / 3600, s / 60 % 60, s % 60);
  return buf;
}

class ProgressLog {
 public:
  ProgressLog(std::uint32_t from, std::uint32_t to, std::shared_ptr<dex_snapshots::SnapshotWriter> writer, double every)
      : from_(from), to_(to), writer_(std::move(writer)), every_(every) {
  }
  void start() {
    if (every_ > 0) {
      thread_ = std::thread([this] { run(); });
    }
  }
  void stop() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    wake_.notify_all();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  using Clock = std::chrono::steady_clock;

  void run() {
    const auto started = Clock::now();
    std::optional<Clock::time_point> blocks_started;
    std::uint64_t blocks_base = 0, prev_accounts = 0, prev_blocks = 0;
    auto prev_at = started;
    std::unique_lock<std::mutex> lock(mutex_);
    while (!wake_.wait_for(lock, std::chrono::duration<double>(every_), [&] { return stopping_; })) {
      const auto now = Clock::now();
      const double dt = std::max(std::chrono::duration<double>(now - prev_at).count(), 1e-3);
      const double elapsed = std::chrono::duration<double>(now - started).count();
      prev_at = now;
      const auto accounts_total = g_progress.accounts_total.load();
      const auto accounts_read = g_progress.accounts_read.load();
      const auto blocks = g_progress.blocks_offered.load();
      if (blocks == 0) {
        if (accounts_total == 0) {
          LOG(INFO) << "progress: waiting for mc seqno " << from_ << ", " << format_duration(elapsed) << " elapsed";
          continue;
        }
        if (accounts_read >= accounts_total) {
          LOG(INFO) << "progress: snapshot at mc seqno " << from_ << ": all " << accounts_total
                    << " listed accounts read, writing its pools; " << format_duration(elapsed) << " elapsed";
          continue;
        }
        const double rate = (accounts_read - prev_accounts) / dt;
        prev_accounts = accounts_read;
        char percent[16];
        std::snprintf(percent, sizeof(percent), "%.1f", 100.0 * accounts_read / accounts_total);
        LOG(INFO) << "progress: snapshot at mc seqno " << from_ << ": " << accounts_read << " of " << accounts_total
                  << " listed accounts read (" << percent << "%), " << static_cast<long long>(rate)
                  << " accounts/s, ETA " << (rate > 0 ? format_duration((accounts_total - accounts_read) / rate) : "-")
                  << ", " << format_duration(elapsed) << " elapsed";
        continue;
      }
      if (!blocks_started) {
        blocks_started = prev_at = now;
        blocks_base = prev_blocks = blocks;
      }
      const std::uint64_t total = std::uint64_t{to_} - from_ + 1;
      const double phase = std::chrono::duration<double>(now - *blocks_started).count();
      const double rate = (blocks - prev_blocks) / dt;
      const double average = phase > 0 ? (blocks - blocks_base) / phase : 0;
      prev_blocks = blocks;
      char chain_time[32] = "-";
      if (const std::time_t t = g_progress.last_utime.load(); t > 0) {
        std::tm tm{};
        gmtime_r(&t, &tm);
        std::strftime(chain_time, sizeof(chain_time), "%Y-%m-%d %H:%M:%S", &tm);
      }
      const double eta_rate = average > 0 ? average : rate;
      char percent[16];
      std::snprintf(percent, sizeof(percent), "%.1f", 100.0 * blocks / total);
      const auto stats = writer_->stats();
      LOG(INFO) << "progress: " << blocks << " of " << total << " blocks (" << percent << "%), mc seqno "
                << (from_ + blocks - 1) << " at " << chain_time << " UTC, "
                << (phase > 0 ? std::to_string(static_cast<long long>(rate)) : "-") << " blocks/s now, "
                << (phase > 0 ? std::to_string(static_cast<long long>(average)) : "-") << " average, ETA "
                << (eta_rate > 0 && blocks < total ? format_duration((total - blocks) / eta_rate) : "-") << ", "
                << g_progress.pool_rows.load() << " pool rows; writer " << stats.blocks_written << " blocks written, "
                << stats.blocks_dropped << " dropped; " << format_duration(elapsed) << " elapsed";
      log_stages(dt, stats);
    }
  }

  // Where the time of the last interval went. The first call only takes the first sample.
  struct Sample {
    std::uint64_t read_us, parse_us, detect_us, offer_us, blocks, batches, insert_us, cpu_us;
  };
  void log_stages(double dt, const dex_snapshots::SnapshotWriter::Stats& stats) {
    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    const Sample cur{g_progress.read_us, g_progress.parse_us, g_progress.detect_us, g_progress.offer_us,
                     g_progress.parsed_blocks, stats.batches, stats.insert_us,
                     static_cast<std::uint64_t>((ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000ll +
                                                ru.ru_utime.tv_usec + ru.ru_stime.tv_usec)};
    const auto prev = std::exchange(prev_sample_, cur);
    if (!prev) {
      return;
    }
    const auto& p = *prev;
    const double blocks = static_cast<double>(cur.blocks - p.blocks);
    const double batches = static_cast<double>(cur.batches - p.batches);
    auto per_block = [&](std::uint64_t a, std::uint64_t b) { return blocks > 0 ? (a - b) / 1000.0 / blocks : 0.0; };
    auto share = [&](std::uint64_t a, std::uint64_t b) { return (a - b) / 1e4 / dt; };  // % of the interval
    char line[384];
    std::snprintf(line, sizeof(line),
                  "stages: %.0f blocks; ms per block: read %.1f, parse %.1f; %.1f blocks in the pipeline, %llu parsed "
                  "waiting for an earlier one; detect %.0f%%, writer wait %.0f%% of the actor; inserts %.0f batches, "
                  "%.0f ms each, busy %.0f%%; CPU %.1f cores",
                  blocks, per_block(cur.read_us, p.read_us), per_block(cur.parse_us, p.parse_us),
                  (cur.read_us - p.read_us + cur.parse_us - p.parse_us) / 1e6 / dt,
                  static_cast<unsigned long long>(g_progress.waiting.load()), share(cur.detect_us, p.detect_us),
                  share(cur.offer_us, p.offer_us), batches,
                  batches > 0 ? (cur.insert_us - p.insert_us) / 1000.0 / batches : 0.0,
                  share(cur.insert_us, p.insert_us), (cur.cpu_us - p.cpu_us) / 1e6 / dt);
    LOG(INFO) << line;
  }
  std::optional<Sample> prev_sample_;

  std::uint32_t from_, to_;
  std::shared_ptr<dex_snapshots::SnapshotWriter> writer_;
  double every_;
  std::mutex mutex_;
  std::condition_variable wake_;
  bool stopping_{false};
  std::thread thread_;
};

}  // namespace

int main(int argc, char* argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();
  CHECK(vm::init_op_cp0());

  std::string db_root, secondary_dir, accounts_file;
  std::uint32_t from = 0, to = 0, threads = 7, parallel = 7, progress_sec = 10;
  std::string clickhouse;
  std::size_t queue_rows = 0;

  td::OptionParser p;
  p.set_description("Write DEX pool snapshots of masterchain blocks --from..--to to ClickHouse (writer backfill)");
  p.add_option('h', "help", "Print help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('D', "db", "Path to the TON DB", [&](td::Slice v) { db_root = v.str(); });
  p.add_option('\0', "secondary", "Required: a new directory for the RocksDB secondary instance and stats files. The node's "
               "database is only read (RocksDB read-only mode would write LOG and db_stats.txt files into it)",
               [&](td::Slice v) { secondary_dir = v.str(); });
  p.add_checked_option('\0', "from", "First masterchain seqno", [&](td::Slice v) {
    TRY_RESULT_ASSIGN(from, td::to_integer_safe<std::uint32_t>(v));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "to", "Last masterchain seqno", [&](td::Slice v) {
    TRY_RESULT_ASSIGN(to, td::to_integer_safe<std::uint32_t>(v));
    return td::Status::OK();
  });
  p.add_checked_option('t', "threads", "Scheduler threads (default: 7)", [&](td::Slice v) {
    TRY_RESULT_ASSIGN(threads, td::to_integer_safe<std::uint32_t>(v));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "parallel", "Blocks fetched and parsed at the same time (default: 7)", [&](td::Slice v) {
    TRY_RESULT_ASSIGN(parallel, td::to_integer_safe<std::uint32_t>(v));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "progress-sec", "Seconds between progress lines (default: 10; 0: none)", [&](td::Slice v) {
    TRY_RESULT_ASSIGN(progress_sec, td::to_integer_safe<std::uint32_t>(v));
    return td::Status::OK();
  });
  p.add_option('\0', "accounts-file", "Raw addresses of the pools that exist at --from, one per line (blank lines and "
               "lines starting with # are ignored); the first block snapshots all of them", [&](td::Slice v) {
    accounts_file = v.str();
  });
  p.add_option('\0', "dex-snapshots-clickhouse",
               "Required: [tcp|clickhouse]://[user[:password]@]host[:port][/database] "
               "(default port 9000, database dex_snapshots)", [&](td::Slice v) { clickhouse = v.str(); });
  p.add_checked_option('\0', "dex-snapshots-queue-rows",
                       "Max DEX pool snapshots queued for ClickHouse (default: 2000000)", [&](td::Slice v) {
    TRY_RESULT_ASSIGN(queue_rows, td::to_integer_safe<std::size_t>(v));
    return td::Status::OK();
  });
  if (auto r = p.run(argc, argv); r.is_error()) {
    LOG(ERROR) << r.move_as_error();
    return 2;
  }
  if (db_root.empty() || secondary_dir.empty() || from == 0 || to < from || clickhouse.empty()) {
    LOG(ERROR) << "--db, --secondary, --from, --to (>= --from) and --dex-snapshots-clickhouse are required";
    return 2;
  }
  if (threads == 0 || parallel == 0) {
    LOG(ERROR) << "--threads and --parallel must be positive";
    return 2;
  }
  std::vector<block::StdAddress> accounts;
  if (!accounts_file.empty()) {
    auto content = td::read_file_str(accounts_file);
    if (content.is_error()) {
      LOG(ERROR) << "--accounts-file: " << content.move_as_error();
      return 2;
    }
    for (auto line : td::full_split(td::Slice(content.ok()), '\n')) {
      line = td::trim(line);
      if (line.empty() || line[0] == '#') {
        continue;
      }
      auto address = block::StdAddress::parse(line);
      if (address.is_error()) {
        LOG(ERROR) << "--accounts-file: bad address " << line;
        return 2;
      }
      accounts.push_back(address.move_as_ok());
    }
    if (accounts.empty()) {
      LOG(ERROR) << "--accounts-file: no addresses";
      return 2;
    }
  }
  auto options = dex_snapshots::parse_writer_dsn(clickhouse);
  if (options.is_error()) {
    LOG(ERROR) << options.error();
    return 2;
  }
  if (queue_rows > 0) {
    options.ok_ref().max_queued_rows = queue_rows;
  }
  auto writer = std::make_shared<dex_snapshots::SnapshotWriter>(options.move_as_ok());
  writer->start();
  enable_dex_snapshots();

  const auto started = std::chrono::steady_clock::now();
  ProgressLog progress(from, to, writer, static_cast<double>(progress_sec));
  progress.start();
  std::atomic<bool> ok{false};
  td::actor::Scheduler scheduler({threads});
  td::actor::ActorOwn<DbScanner> db;
  td::actor::ActorOwn<ParseManager> parser;
  scheduler.run_in_context([&] {
    db = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary, secondary_dir, 60.0f, false);
    parser = td::actor::create_actor<ParseManager>("parsemanager");
    td::actor::create_actor<Backfill>("backfill", db.get(), parser.get(), from, to, parallel, std::move(accounts), writer,
        td::PromiseCreator::lambda([&](td::Result<td::Unit> R) {
          if (R.is_error()) {
            LOG(ERROR) << "Backfill failed: " << R.move_as_error();
          } else {
            ok = true;
          }
          td::actor::SchedulerContext::get().stop();
        })).release();
  });
  while (scheduler.run(1)) {
  }
  // Blocks offered before a failure are still written, or logged as dropped.
  writer->stop();
  const bool written = writer->stats().blocks_dropped == 0;
  progress.stop();
  int code = ok && written ? 0 : 1;
  const auto stats = writer->stats();
  LOG(INFO) << "Backfill " << (code == 0 ? "finished" : "failed") << ": blocks written " << stats.blocks_written
            << ", dropped " << stats.blocks_dropped << ", pool rows " << g_progress.pool_rows.load() << ", "
            << format_duration(std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count())
            << " elapsed";
  std::cout.flush();
  std::cerr.flush();
  // Actors of the stopped scheduler are not torn down one by one.
  std::_Exit(code);
}
