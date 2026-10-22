// ton-tvm-replay replays the transactions of masterchain rounds of a node
// database with the native emulator of ton-tvm-executor, and compares every
// transaction and resulting state with the block (substreams-tvm.md, step 5).
// It writes a line of JSON per round; replay-report.py sums them up and
// compares a run with a reference.
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <google/protobuf/util/message_differencer.h>
#include "td/utils/OptionParser.h"
#include "td/utils/Time.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/port/path.h"
#include "td/utils/port/signals.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "crypto/vm/vm.h"
#include "CellCache.h"
#include "DbScanner.h"
#include "FirehoseBlock.h"
#include "Replay.h"

namespace tvm_executor {
namespace {

using Cell = td::Ref<vm::Cell>;
using Handle = ton::validator::ConstBlockHandle;

// What a replay needs of the masterchain state a block refers to.
constexpr int kReplayConfig = block::ConfigInfo::needLibraries | block::ConfigInfo::needPrevBlocks | kTransactionConfig;
constexpr std::size_t kMaxDiffLines = 6;

std::string json_string(td::Slice text) {
  std::string out = "\"";
  for (unsigned char c : text) {
    if (c == '"' || c == '\\') {
      out += '\\';
      out += static_cast<char>(c);
    } else if (c < 0x20 || c > 0x7e) {
      char escaped[8];
      std::snprintf(escaped, sizeof(escaped), "\\u%04x", c);
      out += escaped;
    } else {
      out += static_cast<char>(c);
    }
  }
  return out + "\"";
}

const char* transaction_type(const Cell& root) {
  block::gen::Transaction::Record tx;
  if (!tlb::unpack_cell(root, tx)) {
    return "unknown";
  }
  using block::gen::TransactionDescr;
  switch (block::gen::t_TransactionDescr.get_tag(vm::load_cell_slice(tx.description))) {
    case TransactionDescr::trans_ord:
      return "ord";
    case TransactionDescr::trans_storage:
      return "storage";
    case TransactionDescr::trans_tick_tock: {
      TransactionDescr::Record_trans_tick_tock description;
      return tlb::unpack_cell(tx.description, description) && description.is_tock ? "tock" : "tick";
    }
    case TransactionDescr::trans_split_prepare:
      return "split_prepare";
    case TransactionDescr::trans_split_install:
      return "split_install";
    case TransactionDescr::trans_merge_prepare:
      return "merge_prepare";
    case TransactionDescr::trans_merge_install:
      return "merge_install";
  }
  return "unknown";
}

// The fields in which the emulated transaction differs from the original,
// hashes aside, which differ with anything.
std::string transaction_diff(const Cell& original, const Cell& emulated) {
  if (emulated.is_null()) {
    return {};
  }
  auto a = transaction_to_proto(original);
  auto b = transaction_to_proto(emulated);
  if (a.is_error() || b.is_error()) {
    return "cannot convert the transactions";
  }
  google::protobuf::util::MessageDifferencer differencer;
  std::string report;
  differencer.ReportDifferencesToString(&report);
  differencer.Compare(a.ok(), b.ok());
  std::istringstream lines(report);
  std::string line, out;
  std::size_t kept = 0;
  while (std::getline(lines, line) && kept < kMaxDiffLines) {
    if (line.find("hash") != std::string::npos) {
      continue;
    }
    out += (kept++ ? "; " : "") + line;
  }
  return out;
}

// The account in a shard state, account_none when the state has none.
td::Result<AccountState> account_in_state(const Cell& root, const td::Bits256& account_id) {
  block::gen::ShardStateUnsplit::Record state;
  if (!tlb::unpack_cell(root, state)) {
    return td::Status::Error("cannot unpack a shard state");
  }
  vm::AugmentedDictionary accounts{vm::load_cell_slice_ref(state.accounts), 256, block::tlb::aug_ShardAccounts};
  auto value = accounts.lookup(account_id.cbits(), 256);
  if (value.is_null()) {
    return AccountState{vm::CellBuilder{}.store_long(0, 1).finalize()};
  }
  block::gen::ShardAccount::Record shard_account;
  if (!tlb::csr_unpack(value, shard_account)) {
    return td::Status::Error("cannot unpack a ShardAccount");
  }
  return AccountState{shard_account.account, shard_account.last_trans_hash, shard_account.last_trans_lt};
}

std::vector<td::Bits256> block_accounts(const ShardBlockData& block) {
  std::vector<td::Bits256> out;
  vm::AugmentedDictionary accounts{vm::load_cell_slice_ref(block.account_blocks), 256,
                                   block::tlb::aug_ShardAccountBlocks};
  accounts.check_for_each([&](td::Ref<vm::CellSlice>, td::ConstBitPtr key, int bits) {
    out.emplace_back(key);
    return bits == 256;
  });
  return out;
}

struct Mismatch {
  std::string block;
  std::string account;
  td::uint64 lt;
  std::string type;
  std::string reason;
  std::string diff;
};

struct RoundReport {
  td::uint32 seqno;
  int version{-1};
  int blocks{0};
  int accounts{0};
  int transactions{0};
  int replayed{0};
  // Transactions after a mismatch or an unsupported one of their account.
  int skipped{0};
  std::map<std::string, int> types;
  std::map<std::string, int> unsupported;
  std::vector<Mismatch> mismatches;
  std::vector<std::string> errors;
  double seconds{0};

  std::string json() const {
    std::string out = PSTRING() << "{\"seqno\":" << seqno << ",\"version\":" << version << ",\"blocks\":" << blocks
                                << ",\"accounts\":" << accounts << ",\"transactions\":" << transactions
                                << ",\"replayed\":" << replayed << ",\"skipped\":" << skipped
                                << ",\"seconds\":" << seconds;
    auto counts = [&](const char* name, const std::map<std::string, int>& map) {
      out += PSTRING() << ",\"" << name << "\":{";
      bool first = true;
      for (const auto& [key, n] : map) {
        out += PSTRING() << (first ? "" : ",") << json_string(key) << ":" << n;
        first = false;
      }
      out += "}";
    };
    counts("types", types);
    counts("unsupported", unsupported);
    out += ",\"mismatches\":[";
    for (std::size_t i = 0; i < mismatches.size(); i++) {
      const auto& m = mismatches[i];
      out += PSTRING() << (i ? "," : "") << "{\"block\":" << json_string(m.block) << ",\"account\":"
                       << json_string(m.account) << ",\"lt\":" << m.lt << ",\"type\":" << json_string(m.type)
                       << ",\"reason\":" << json_string(m.reason) << ",\"diff\":" << json_string(m.diff) << "}";
    }
    out += "],\"errors\":[";
    for (std::size_t i = 0; i < errors.size(); i++) {
      out += PSTRING() << (i ? "," : "") << json_string(errors[i]);
    }
    return out + "]}";
  }
};

// Replays one masterchain round: the masterchain block and the shard blocks
// it commits, every transaction of every account from its state before the
// block, in the context of the block.
class RoundActor final : public td::actor::Actor {
 public:
  RoundActor(td::actor::ActorId<DbScanner> db, td::uint32 seqno, td::Promise<RoundReport> promise)
      : db_(db), promise_(std::move(promise)) {
    report_.seqno = seqno;
  }

 private:
  struct Block {
    ShardBlockData data;
    // Owns the lazy archive cells of the block data.
    td::Ref<ton::validator::BlockData> owner;
  };

  td::actor::ActorId<DbScanner> db_;
  td::Promise<RoundReport> promise_;
  RoundReport report_;
  td::Timer timer_;
  std::vector<Block> blocks_;
  // The states after blocks: of the round, and of the predecessors before it.
  std::map<ton::BlockIdExt, Cell> states_;
  std::map<ton::BlockIdExt, std::shared_ptr<block::ConfigInfo>> configs_;
  int pending_{0};
  // The accounts to replay, by block.
  std::vector<std::pair<std::size_t, td::Bits256>> work_;
  std::size_t next_{0};

  template <class T>
  auto reply(void (RoundActor::*method)(td::Result<T>)) {
    return td::PromiseCreator::lambda([self = actor_id(this), method](td::Result<T> result) {
      td::actor::send_closure(self, method, std::move(result));
    });
  }

  void start_up() override {
    td::actor::send_closure(db_, &DbScanner::fetch_seqno, report_.seqno, reply(&RoundActor::got_round));
  }

  void got_round(td::Result<schema::MasterchainBlockDataState> result) {
    if (result.is_error()) {
      return fail(PSTRING() << "round: " << result.error().message());
    }
    auto round = result.move_as_ok();
    std::set<ton::BlockIdExt> masters, predecessors;
    for (const auto& b : round.shard_blocks_diff_) {
      auto data = shard_block_data(b.block_data->root_cell(), b.handle->id());
      if (data.is_error()) {
        return fail(data.error().message().str());
      }
      if (b.handle->id().is_masterchain()) {
        auto config = block::ConfigInfo::extract_config(b.block_state, b.handle->id(), block::Config::needCapabilities);
        if (config.is_ok()) {
          report_.version = config.ok()->get_global_version();
        }
      }
      states_[b.handle->id()] = b.block_state;
      masters.insert(data.ok().master);
      for (const auto& prev : data.ok().prev) {
        predecessors.insert(prev);
      }
      blocks_.push_back({data.move_as_ok(), b.block_data});
    }
    report_.blocks = static_cast<int>(blocks_.size());
    for (const auto& id : predecessors) {
      if (!states_.count(id)) {
        load(id, false);
      }
    }
    for (const auto& id : masters) {
      load(id, true);
    }
    if (pending_ == 0) {
      start_replay();
    }
  }

  void load(const ton::BlockIdExt& id, bool config) {
    pending_++;
    auto self = actor_id(this);
    auto db = db_;
    td::actor::send_closure(db_, &DbScanner::get_block_handle, id,
                            td::PromiseCreator::lambda([self, db, id, config](td::Result<Handle> handle) mutable {
                              if (handle.is_error()) {
                                return td::actor::send_closure(self, &RoundActor::loaded, id, config,
                                                               td::Result<Cell>(handle.move_as_error()));
                              }
                              td::actor::send_closure(
                                  db, &DbScanner::get_block_state_root, handle.move_as_ok(),
                                  td::PromiseCreator::lambda([self, id, config](td::Result<Cell> root) mutable {
                                    td::actor::send_closure(self, &RoundActor::loaded, id, config, std::move(root));
                                  }));
                            }));
  }

  void loaded(ton::BlockIdExt id, bool config, td::Result<Cell> root) {
    if (root.is_error()) {
      return fail(PSTRING() << id.to_str() << ": " << root.error().message());
    }
    if (config) {
      auto info = block::ConfigInfo::extract_config(root.ok(), id, kReplayConfig);
      if (info.is_error()) {
        return fail(PSTRING() << "config of " << id.to_str() << ": " << info.error().message());
      }
      std::shared_ptr<block::ConfigInfo> shared{info.move_as_ok()};
      auto status = load_tree(shared->get_root_cell());
      if (status.is_error()) {
        return fail(PSTRING() << "config of " << id.to_str() << ": " << status.message());
      }
      configs_[id] = std::move(shared);
    } else {
      states_[id] = root.move_as_ok();
    }
    if (--pending_ == 0) {
      start_replay();
    }
  }

  void start_replay() {
    try {
      for (std::size_t i = 0; i < blocks_.size(); i++) {
        for (const auto& account : block_accounts(blocks_[i].data)) {
          work_.emplace_back(i, account);
        }
      }
    } catch (vm::VmError& e) {
      return fail(PSTRING() << "account blocks: " << e.get_msg());
    }
    yield();
  }

  void loop() override {
    if (next_ == work_.size()) {
      return finish();
    }
    const auto& [index, account] = work_[next_++];
    try {
      replay_account(blocks_[index].data, account);
    } catch (vm::VmVirtError&) {
      report_.errors.push_back("pruned cell");
    } catch (vm::VmError& e) {
      report_.errors.push_back(PSTRING() << "cell: " << e.get_msg());
    }
    yield();
  }

  void replay_account(const ShardBlockData& block, const td::Bits256& account_id) {
    block::StdAddress address{block.id.id.workchain, account_id};
    auto name = PSTRING() << address.workchain << ":" << account_id.to_hex();
    auto transactions = account_transactions(block, account_id);
    if (transactions.is_error()) {
      report_.errors.push_back(PSTRING() << name << ": " << transactions.error().message());
      return;
    }
    const auto& txs = transactions.ok();
    report_.accounts++;
    report_.transactions += static_cast<int>(txs.size());
    for (const auto& tx : txs) {
      report_.types[transaction_type(tx.root)]++;
    }
    auto mismatch = [&](std::size_t i, std::string reason, std::string diff = {}) {
      report_.mismatches.push_back(
          {block.id.to_str(), name, txs[i].lt, transaction_type(txs[i].root), std::move(reason), std::move(diff)});
      report_.skipped += static_cast<int>(txs.size() - i - 1);
    };
    auto predecessor = account_predecessor(block, address);
    if (predecessor.is_error()) {
      report_.errors.push_back(PSTRING() << name << ": " << predecessor.error().message());
      return;
    }
    auto before = account_in_state(states_.at(predecessor.ok()), account_id);
    if (before.is_error()) {
      report_.errors.push_back(PSTRING() << name << ": " << before.error().message());
      return;
    }
    if (txs.front().before != before.ok().account->get_hash().bits()) {
      return mismatch(0, "the state before the block is not the one of its first transaction");
    }
    auto replay = Replay::create(before.ok(), address, block, configs_.at(block.master));
    if (replay.is_error()) {
      report_.errors.push_back(PSTRING() << name << ": " << replay.error().message());
      return;
    }
    for (std::size_t i = 0; i < txs.size(); i++) {
      auto type = transaction_type(txs[i].root);
      if (std::string(type) != "ord" && std::string(type) != "tick" && std::string(type) != "tock") {
        report_.unsupported[type]++;
        report_.skipped += static_cast<int>(txs.size() - i - 1);
        return;
      }
      auto result = replay.ok()->advance(txs[i]);
      if (result.is_error()) {
        report_.errors.push_back(PSTRING() << name << ": " << result.error().message());
        return;
      }
      if (!result.ok().empty()) {
        return mismatch(i, result.move_as_ok(), transaction_diff(txs[i].root, replay.ok()->emulated()));
      }
      report_.replayed++;
    }
  }

  void fail(std::string error) {
    report_.errors.push_back(std::move(error));
    finish();
  }

  void finish() {
    report_.seconds = timer_.elapsed();
    promise_.set_value(std::move(report_));
    stop();
  }
};

// Replays the rounds, so many at a time, and writes their reports.
class Coordinator final : public td::actor::Actor {
 public:
  Coordinator(td::actor::ActorId<DbScanner> db, std::vector<td::uint32> seqnos, std::size_t parallel,
              std::string out)
      : db_(db), seqnos_(std::move(seqnos)), parallel_(parallel), out_(out, std::ios::app) {
  }

 private:
  td::actor::ActorId<DbScanner> db_;
  std::vector<td::uint32> seqnos_;
  std::size_t parallel_;
  std::ofstream out_;
  std::size_t started_{0}, done_{0};
  long long transactions_{0}, replayed_{0}, mismatches_{0};
  td::Timer timer_;
  double last_log_{0};

  void start_up() override {
    std::cerr << "replaying " << seqnos_.size() << " rounds, " << parallel_ << " at a time" << std::endl;
    while (started_ < seqnos_.size() && started_ - done_ < parallel_) {
      start_next();
    }
    if (seqnos_.empty()) {
      td::actor::SchedulerContext::get().stop();
    }
  }

  void start_next() {
    auto seqno = seqnos_[started_++];
    td::actor::create_actor<RoundActor>("replay-round", db_, seqno,
                                        td::PromiseCreator::lambda([self = actor_id(this)](td::Result<RoundReport> r) {
                                          td::actor::send_closure(self, &Coordinator::done, r.move_as_ok());
                                        }))
        .release();
  }

  void done(RoundReport report) {
    done_++;
    transactions_ += report.transactions;
    replayed_ += report.replayed;
    mismatches_ += static_cast<long long>(report.mismatches.size());
    out_ << report.json() << "\n";
    out_.flush();
    if (timer_.elapsed() - last_log_ > 30 || done_ == seqnos_.size()) {
      last_log_ = timer_.elapsed();
      std::cerr << done_ << "/" << seqnos_.size() << " rounds, " << transactions_ << " transactions, " << replayed_
                << " replayed, " << mismatches_ << " mismatches, "
                << static_cast<long long>(transactions_ / std::max(timer_.elapsed(), 1e-3)) << " tx/s" << std::endl;
    }
    if (started_ < seqnos_.size()) {
      start_next();
    } else if (done_ == seqnos_.size()) {
      td::actor::SchedulerContext::get().stop();
    }
  }
};

// What a search looks for in masterchain states.
enum class Searched { Version, Shards };

// The masterchain seqnos where a value of the masterchain state changes,
// found by bisection between evenly spaced probes: the global version of
// config param 8, or the number of shards. A value that changes back within
// a gap between probes is not found.
class ValueSearch final : public td::actor::Actor {
 public:
  ValueSearch(td::actor::ActorId<DbScanner> db, Searched what, int probes, td::uint32 from, td::uint32 to)
      : db_(db), what_(what), probes_(probes), from_(from), to_(to) {
  }

 private:
  td::actor::ActorId<DbScanner> db_;
  Searched what_;
  int probes_;
  td::uint32 from_, to_;
  std::map<td::uint32, int> versions_;
  std::set<td::uint32> asked_;
  // Probes wait here: each opens the archive slice of its block, with a
  // RocksDB index of its own, and thousands at once exhaust open files.
  std::vector<td::uint32> queue_;
  int pending_{0}, running_{0};
  static constexpr int kMaxRunning = 32;

  void start_up() override {
    td::actor::send_closure(db_, &DbScanner::get_last_mc_seqno,
                            td::PromiseCreator::lambda([self = actor_id(this)](td::Result<ton::BlockSeqno> head) {
                              td::actor::send_closure(self, &ValueSearch::got_head, head.move_as_ok());
                            }));
  }

  void got_head(ton::BlockSeqno head) {
    auto from = std::max<td::uint32>(from_, 1);
    auto to = to_ == 0 ? head : std::min<td::uint32>(to_, head);
    for (int i = 0; i <= probes_; i++) {
      probe(static_cast<td::uint32>(from + (static_cast<td::uint64>(to - from) * i) / probes_));
    }
  }

  void probe(td::uint32 seqno) {
    if (!asked_.insert(seqno).second) {
      return;
    }
    pending_++;
    queue_.push_back(seqno);
    run_queued();
  }

  void run_queued() {
    while (running_ < kMaxRunning && !queue_.empty()) {
      auto seqno = queue_.back();
      queue_.pop_back();
      running_++;
      run(seqno);
    }
  }

  void run(td::uint32 seqno) {
    auto self = actor_id(this);
    auto db = db_;
    td::actor::send_closure(
        db_, &DbScanner::get_mc_block_handle, seqno,
        td::PromiseCreator::lambda([self, db, seqno](td::Result<Handle> handle) mutable {
          if (handle.is_error()) {
            return td::actor::send_closure(self, &ValueSearch::got_value, seqno, -1);
          }
          auto id = handle.ok()->id();
          td::actor::send_closure(db, &DbScanner::get_block_state_root, handle.move_as_ok(),
                                  td::PromiseCreator::lambda([self, seqno, id](td::Result<Cell> root) {
                                    td::actor::send_closure(self, &ValueSearch::got_state, seqno, id,
                                                            std::move(root));
                                  }));
        }));
  }

  void got_state(td::uint32 seqno, ton::BlockIdExt id, td::Result<Cell> root) {
    int value = -1;
    if (root.is_ok()) {
      auto mode = what_ == Searched::Version ? block::Config::needCapabilities : block::ConfigInfo::needShardHashes;
      auto config = block::ConfigInfo::extract_config(root.ok(), id, mode);
      if (config.is_ok()) {
        value = what_ == Searched::Version ? config.ok()->get_global_version()
                                           : static_cast<int>(config.ok()->get_shard_hash_ids(true).size());
      }
    }
    got_value(seqno, value);
  }

  void got_value(td::uint32 seqno, int version) {
    versions_[seqno] = version;
    running_--;
    run_queued();
    if (--pending_ > 0) {
      return;
    }
    // Bisect every gap between probes of different versions.
    for (auto it = versions_.begin(); std::next(it) != versions_.end(); ++it) {
      auto next = std::next(it);
      if (it->second != next->second && next->first - it->first > 1) {
        probe(it->first + (next->first - it->first) / 2);
      }
    }
    if (pending_ > 0) {
      return;
    }
    int previous = -2;
    for (const auto& [seqno, version] : versions_) {
      if (version != previous) {
        std::cout << (what_ == Searched::Version ? "version " : "shards ") << version << " from " << seqno
                  << std::endl;
        previous = version;
      }
    }
    td::actor::SchedulerContext::get().stop();
  }
};

// "a-b:step", "a-b" or "a" items, comma separated.
td::Result<std::vector<td::uint32>> parse_seqnos(td::Slice spec) {
  std::vector<td::uint32> out;
  for (auto item : td::full_split(spec, ',')) {
    if (item.empty()) {
      continue;
    }
    auto [range, step_text] = td::split(item, ':');
    td::uint32 step = 1;
    if (!step_text.empty()) {
      TRY_RESULT_ASSIGN(step, td::to_integer_safe<td::uint32>(step_text));
    }
    auto [from_text, to_text] = td::split(range, '-');
    TRY_RESULT(from, td::to_integer_safe<td::uint32>(from_text));
    td::uint32 to = from;
    if (!to_text.empty()) {
      TRY_RESULT_ASSIGN(to, td::to_integer_safe<td::uint32>(to_text));
    }
    for (td::uint32 seqno = from; seqno <= to; seqno += std::max<td::uint32>(step, 1)) {
      out.push_back(seqno);
    }
  }
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

}  // namespace
}  // namespace tvm_executor

int main(int argc, char* argv[]) {
  // TON logs every precompiled contract it runs in TVM at INFO.
  SET_VERBOSITY_LEVEL(verbosity_WARNING);
  td::set_default_failure_signal_handler().ensure();

  std::string db_root, working_dir, out = "replay.jsonl", spec, seqnos_file;
  td::uint32 threads = 8, parallel = 64;
  int verbosity = VERBOSITY_NAME(WARNING);
  int version_probes = 0, shard_probes = 0;
  td::uint32 search_from = 1, search_to = 0;
  td::uint64 celldb_cache_size = td::uint64{4096} << 20;

  td::OptionParser p;
  p.set_description(
      "Replay the transactions of masterchain rounds of a TON node database and compare them with the blocks");
  p.add_option('\0', "help", "prints help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cerr << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice value) { db_root = value.str(); });
  p.add_option('W', "working-dir", "Path to working dir for secondary rocksdb logs",
               [&](td::Slice value) { working_dir = value.str(); });
  p.add_option('\0', "seqnos", "Rounds: a, a-b, a-b:step, comma separated",
               [&](td::Slice value) { spec = value.str(); });
  p.add_option('\0', "seqnos-file", "Rounds, one seqno per line", [&](td::Slice value) { seqnos_file = value.str(); });
  p.add_checked_option('v', "verbosity", "Log verbosity: 2 is WARNING (default), 3 INFO, where TON tells why "
                       "a transaction phase fails", [&](td::Slice value) {
                         TRY_RESULT_ASSIGN(verbosity, td::to_integer_safe<int>(value));
                         return td::Status::OK();
                       });
  p.add_option('o', "out", "JSON lines, appended (default: replay.jsonl)", [&](td::Slice value) { out = value.str(); });
  p.add_checked_option('t', "threads", "Scheduler threads (default: 8)", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(threads, td::to_integer_safe<td::uint32>(value));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "parallel", "Rounds replayed at a time (default: 64)", [&](td::Slice value) {
    TRY_RESULT_ASSIGN(parallel, td::to_integer_safe<td::uint32>(value));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "find-versions", "Print where the global version changes, bisecting from so many probes",
                       [&](td::Slice value) {
                         TRY_RESULT_ASSIGN(version_probes, td::to_integer_safe<int>(value));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "find-shards",
                       "Print where the number of shards changes, bisecting from so many probes",
                       [&](td::Slice value) {
                         TRY_RESULT_ASSIGN(shard_probes, td::to_integer_safe<int>(value));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "search-range", "Seqnos a-b of a search (default: the whole history)",
                       [&](td::Slice value) {
                         auto [from, to] = td::split(value, '-');
                         TRY_RESULT_ASSIGN(search_from, td::to_integer_safe<td::uint32>(from));
                         TRY_RESULT_ASSIGN(search_to, td::to_integer_safe<td::uint32>(to));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "celldb-cache-size", "Block cache of the node cell db in MiB (default: 4096)",
                       [&](td::Slice value) {
                         TRY_RESULT(v, td::to_integer_safe<td::uint32>(value));
                         celldb_cache_size = static_cast<td::uint64>(v) << 20;
                         return td::Status::OK();
                       });
  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::_Exit(2);
  }
  SET_VERBOSITY_LEVEL(verbosity);
  if (db_root.empty() || working_dir.empty()) {
    LOG(ERROR) << "--db and --working-dir are required";
    std::_Exit(2);
  }
  std::vector<td::uint32> seqnos;
  if (version_probes == 0 && shard_probes == 0) {
    if (!seqnos_file.empty()) {
      std::ifstream file(seqnos_file);
      std::string line;
      while (std::getline(file, line)) {
        if (!line.empty()) {
          spec += "," + line;
        }
      }
    }
    auto parsed = tvm_executor::parse_seqnos(spec);
    if (parsed.is_error() || parsed.ok().empty()) {
      LOG(ERROR) << "no rounds to replay: " << (parsed.is_error() ? parsed.error().to_string() : spec);
      std::_Exit(2);
    }
    seqnos = parsed.move_as_ok();
  }
  td::mkpath(working_dir + "/").ensure();
  vm::init_vm(false).ensure();

  td::actor::Scheduler scheduler({threads});
  td::actor::ActorOwn<DbScanner> db_scanner;
  td::actor::ActorOwn<tvm_executor::ValueSearch> search;
  td::actor::ActorOwn<tvm_executor::Coordinator> coordinator;
  scheduler.run_in_context([&] {
    db_scanner = td::actor::create_actor<DbScanner>("scanner", db_root, dbs_secondary,
                                                     working_dir + "/secondary_logs", 1.0f, false, celldb_cache_size);
    if (version_probes > 0 || shard_probes > 0) {
      auto what = version_probes > 0 ? tvm_executor::Searched::Version : tvm_executor::Searched::Shards;
      search = td::actor::create_actor<tvm_executor::ValueSearch>(
          "search", db_scanner.get(), what, version_probes > 0 ? version_probes : shard_probes, search_from, search_to);
    } else {
      coordinator = td::actor::create_actor<tvm_executor::Coordinator>("replay", db_scanner.get(),
                                                                        std::move(seqnos), parallel, out);
    }
  });
  scheduler.run();
  std::_Exit(0);
}
