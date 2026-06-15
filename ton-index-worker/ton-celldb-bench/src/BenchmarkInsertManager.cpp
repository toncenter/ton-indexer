#include "BenchmarkInsertManager.h"

#include "Statistics.h"
#include "convert-utils.h"
#include "crypto/vm/boc.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/crypto.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t elapsed_us(Clock::time_point started_at) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started_at).count());
}

std::string extra_currencies_to_json_string(const std::map<std::uint32_t, td::RefInt256>& extra_currencies) {
  td::JsonBuilder extra_currencies_json;
  auto obj = extra_currencies_json.enter_object();
  for (auto& currency : extra_currencies) {
    obj(std::to_string(currency.first), currency.second->to_dec_string());
  }
  obj.leave();
  return extra_currencies_json.string_builder().as_cslice().str();
}

struct LatestAccountStateSourceRow {
  schema::AccountState account_state;
  std::string raw_account;
  std::uint32_t source_mc_seqno;
};

struct PreparedLatestAccountStateRow {
  block::StdAddress account;
  td::Bits256 hash;
  td::RefInt256 balance;
  std::string balance_extra_currencies;
  std::string account_status;
  std::uint32_t timestamp;
  td::Bits256 last_trans_hash;
  std::uint64_t last_trans_lt;
  std::optional<td::Bits256> frozen_hash;
  std::optional<td::Bits256> data_hash;
  std::optional<td::Bits256> code_hash;
  std::optional<std::string> data_boc;
  std::optional<std::string> code_boc;
  std::uint32_t source_mc_seqno;
};

struct SerializedCell {
  std::optional<std::string> base64;
  std::size_t raw_bytes{0};
  std::size_t base64_bytes{0};
  bool ok{false};
};

struct LatestAccountStatesChunkPreparedResult {
  std::vector<PreparedLatestAccountStateRow> rows;
  BenchmarkPrepareCounters counters;
};

SerializedCell serialize_cell_to_base64(td::Ref<vm::Cell> cell, BenchmarkStats& stats, bool is_code,
                                        BenchmarkPrepareCounters& counters) {
  auto serialize_started_at = Clock::now();
  auto res = vm::std_boc_serialize(cell);
  const auto serialize_us = elapsed_us(serialize_started_at);
  if (is_code) {
    stats.record_code_serialize_us(serialize_us);
  } else {
    stats.record_data_serialize_us(serialize_us);
  }

  if (res.is_error()) {
    if (is_code) {
      ++counters.code_serialize_errors;
    } else {
      ++counters.data_serialize_errors;
    }
    return {};
  }

  auto boc = res.move_as_ok();
  SerializedCell result;
  result.raw_bytes = boc.size();

  auto base64_started_at = Clock::now();
  auto encoded = td::base64_encode(boc.as_slice());
  stats.record_base64_us(elapsed_us(base64_started_at));

  result.base64_bytes = encoded.size();
  result.base64 = std::move(encoded);
  result.ok = true;
  return result;
}

PreparedLatestAccountStateRow prepare_latest_account_state_row(const LatestAccountStateSourceRow& source,
                                                               std::int32_t max_data_depth,
                                                               BenchmarkStats& stats,
                                                               BenchmarkPrepareCounters& counters) {
  const auto& account_state = source.account_state;
  std::optional<std::string> code_str = std::nullopt;
  std::optional<std::string> data_str = std::nullopt;

  ++counters.rows;
  if (account_state.code.not_null()) {
    ++counters.rows_with_code;
  }
  if (account_state.data.not_null()) {
    ++counters.rows_with_data;
  }

  if (max_data_depth >= 0 && account_state.data.not_null()) {
    auto depth_started_at = Clock::now();
    const auto depth = account_state.data->get_depth();
    stats.record_data_depth_us(elapsed_us(depth_started_at));
    ++counters.data_depth_checks;

    if (max_data_depth == 0 || depth <= max_data_depth) {
      auto serialized = serialize_cell_to_base64(account_state.data, stats, false, counters);
      if (serialized.ok) {
        data_str = std::move(serialized.base64);
        ++counters.data_serialize_count;
        counters.data_boc_raw_bytes += serialized.raw_bytes;
        counters.data_boc_base64_bytes += serialized.base64_bytes;
      }
    } else {
      ++counters.data_skipped_by_depth;
    }
  }

  auto serialized_code = serialize_cell_to_base64(account_state.code, stats, true, counters);
  if (serialized_code.ok) {
    code_str = std::move(serialized_code.base64);
    ++counters.code_serialize_count;
    counters.code_boc_raw_bytes += serialized_code.raw_bytes;
    counters.code_boc_base64_bytes += serialized_code.base64_bytes;
  }

  return PreparedLatestAccountStateRow{
      .account = account_state.account,
      .hash = account_state.hash,
      .balance = account_state.balance.grams,
      .balance_extra_currencies = extra_currencies_to_json_string(account_state.balance.extra_currencies),
      .account_status = account_state.account_status,
      .timestamp = account_state.timestamp,
      .last_trans_hash = account_state.last_trans_hash,
      .last_trans_lt = account_state.last_trans_lt,
      .frozen_hash = account_state.frozen_hash,
      .data_hash = account_state.data_hash,
      .code_hash = account_state.code_hash,
      .data_boc = std::move(data_str),
      .code_boc = std::move(code_str),
      .source_mc_seqno = source.source_mc_seqno,
  };
}

class PrepareLatestAccountStatesChunkActor final : public td::actor::Actor {
 public:
  PrepareLatestAccountStatesChunkActor(std::vector<LatestAccountStateSourceRow> rows, std::int32_t max_data_depth,
                                       std::shared_ptr<BenchmarkStats> stats,
                                       td::Promise<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> promise)
      : rows_(std::move(rows))
      , max_data_depth_(max_data_depth)
      , stats_(std::move(stats))
      , promise_(std::move(promise)) {
  }

  void start_up() override {
    auto chunk_started_at = Clock::now();
    try {
      auto result = std::make_shared<LatestAccountStatesChunkPreparedResult>();
      result->rows.reserve(rows_.size());
      for (const auto& row : rows_) {
        result->rows.push_back(prepare_latest_account_state_row(row, max_data_depth_, *stats_, result->counters));
      }
      stats_->record_chunk_prepare_us(elapsed_us(chunk_started_at));
      promise_.set_result(std::move(result));
    } catch (const std::exception& e) {
      promise_.set_error(td::Status::Error(td::Slice(e.what())));
    }
    stop();
  }

 private:
  std::vector<LatestAccountStateSourceRow> rows_;
  std::int32_t max_data_depth_;
  std::shared_ptr<BenchmarkStats> stats_;
  td::Promise<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> promise_;
};

class BenchmarkInsertBatchActor final : public td::actor::Actor {
 public:
  BenchmarkInsertBatchActor(std::vector<InsertTaskStruct> insert_tasks,
                            td::Promise<InsertManagerInterface::InsertResult> promise,
                            std::shared_ptr<BenchmarkStats> stats, std::int32_t max_data_depth,
                            std::int32_t latest_states_prepare_parallelism,
                            std::int32_t latest_states_prepare_chunk_size)
      : insert_tasks_(std::move(insert_tasks))
      , promise_(std::move(promise))
      , stats_(std::move(stats))
      , max_data_depth_(max_data_depth)
      , latest_states_prepare_parallelism_(std::max<std::int32_t>(1, latest_states_prepare_parallelism))
      , latest_states_prepare_chunk_size_(std::max<std::int32_t>(1, latest_states_prepare_chunk_size)) {
    std::sort(insert_tasks_.begin(), insert_tasks_.end(), [](const auto& left, const auto& right) {
      return left.mc_seqno_ > right.mc_seqno_;
    });
  }

  void start_up() override {
    stats_->mark_benchmark_started();
    batch_started_at_ = Clock::now();
    try {
      collect_latest_account_states();
      if (latest_account_state_sources_.empty()) {
        finish_ok();
        return;
      }

      const auto chunk_size = static_cast<std::size_t>(latest_states_prepare_chunk_size_);
      total_chunks_ = (latest_account_state_sources_.size() + chunk_size - 1) / chunk_size;
      chunks_.resize(total_chunks_);
      spawn_next_chunks();
    } catch (const std::exception& e) {
      fail(td::Status::Error(td::Slice(e.what())));
    }
  }

 private:
  void collect_latest_account_states() {
    auto collect_started_at = Clock::now();

    BenchmarkBatchCounters batch_counters;
    batch_counters.mc_seqnos = insert_tasks_.size();

    struct AccountStateWithSource {
      schema::AccountState account_state;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<std::string, AccountStateWithSource> latest_account_states;

    for (const auto& task : insert_tasks_) {
      const auto& parsed_block = *task.parsed_block_;
      batch_counters.blocks += parsed_block.blocks_.size();
      batch_counters.traces += parsed_block.traces_.size();
      batch_counters.account_states_seen += parsed_block.account_states_.size();

      for (const auto& block : parsed_block.blocks_) {
        batch_counters.transactions += block.transactions.size();
        for (const auto& tx : block.transactions) {
          batch_counters.messages += tx.out_msgs.size() + (tx.in_msg ? 1 : 0);
        }
      }

      for (const auto& account_state : parsed_block.account_states_) {
        auto raw_account = convert::to_raw_address(account_state.account);
        auto it = latest_account_states.find(raw_account);
        if (it == latest_account_states.end() ||
            it->second.account_state.last_trans_lt < account_state.last_trans_lt) {
          latest_account_states[raw_account] = AccountStateWithSource{
              .account_state = account_state,
              .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }

    latest_account_state_sources_.reserve(latest_account_states.size());
    for (auto& [raw_account, account_state_with_source] : latest_account_states) {
      latest_account_state_sources_.push_back({
          .account_state = std::move(account_state_with_source.account_state),
          .raw_account = std::move(raw_account),
          .source_mc_seqno = account_state_with_source.source_mc_seqno,
      });
    }
    std::sort(latest_account_state_sources_.begin(), latest_account_state_sources_.end(),
              [](const auto& left, const auto& right) {
                return left.raw_account < right.raw_account;
              });

    batch_counters.latest_account_states = latest_account_state_sources_.size();
    stats_->record_batch_input(batch_counters);
    stats_->record_collect_us(elapsed_us(collect_started_at));
  }

  void spawn_next_chunks() {
    const auto chunk_size = static_cast<std::size_t>(latest_states_prepare_chunk_size_);
    const auto parallelism = static_cast<std::size_t>(latest_states_prepare_parallelism_);
    while (in_flight_chunks_ < parallelism && next_chunk_to_spawn_ < total_chunks_) {
      const auto chunk_index = next_chunk_to_spawn_++;
      const auto start = chunk_index * chunk_size;
      const auto end = std::min(start + chunk_size, latest_account_state_sources_.size());
      std::vector<LatestAccountStateSourceRow> rows(
          latest_account_state_sources_.begin() + static_cast<std::ptrdiff_t>(start),
          latest_account_state_sources_.begin() + static_cast<std::ptrdiff_t>(end));

      auto promise = td::PromiseCreator::lambda(
          [SelfId = actor_id(this), generation = generation_, chunk_index](
              td::Result<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> result) mutable {
            td::actor::send_closure(SelfId, &BenchmarkInsertBatchActor::on_chunk_prepared, generation, chunk_index,
                                    std::move(result));
          });
      ++in_flight_chunks_;
      td::actor::create_actor<PrepareLatestAccountStatesChunkActor>("bench_prepare_latest_states_chunk",
                                                                    std::move(rows), max_data_depth_, stats_,
                                                                    std::move(promise))
          .release();
    }
  }

  void on_chunk_prepared(std::uint64_t generation, std::size_t chunk_index,
                         td::Result<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> result) {
    if (generation != generation_) {
      return;
    }
    if (in_flight_chunks_ > 0) {
      --in_flight_chunks_;
    }
    if (result.is_error()) {
      fail(result.move_as_error());
      return;
    }

    chunks_[chunk_index] = result.move_as_ok();
    stats_->record_prepare_counters(chunks_[chunk_index]->counters);
    ++completed_chunks_;

    if (completed_chunks_ == total_chunks_) {
      finish_ok();
      return;
    }
    spawn_next_chunks();
  }

  InsertManagerInterface::InsertResult get_insert_result() const {
    return InsertManagerInterface::InsertResult{.is_leader = std::nullopt};
  }

  void finish_ok() {
    stats_->record_batch_prepare_us(elapsed_us(batch_started_at_));
    auto result = get_insert_result();
    for (auto& task : insert_tasks_) {
      task.promise_.set_result(result);
    }
    promise_.set_result(result);
    stop();
  }

  void fail(td::Status error) {
    auto message = error.to_string();
    for (auto& task : insert_tasks_) {
      task.promise_.set_error(td::Status::Error(message));
    }
    promise_.set_error(std::move(error));
    stop();
  }

  std::vector<InsertTaskStruct> insert_tasks_;
  td::Promise<InsertManagerInterface::InsertResult> promise_;
  std::shared_ptr<BenchmarkStats> stats_;
  std::int32_t max_data_depth_;
  std::int32_t latest_states_prepare_parallelism_;
  std::int32_t latest_states_prepare_chunk_size_;

  Clock::time_point batch_started_at_;
  std::uint64_t generation_{1};
  std::vector<LatestAccountStateSourceRow> latest_account_state_sources_;
  std::vector<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> chunks_;
  std::size_t total_chunks_{0};
  std::size_t next_chunk_to_spawn_{0};
  std::size_t in_flight_chunks_{0};
  std::size_t completed_chunks_{0};
};

}  // namespace

BenchmarkInsertManager::BenchmarkInsertManager(std::shared_ptr<BenchmarkStats> stats, std::uint32_t resume_seqno)
    : stats_(std::move(stats)), resume_seqno_(std::max<std::uint32_t>(1, resume_seqno)) {
}

void BenchmarkInsertManager::set_max_data_depth(std::int32_t value) {
  max_data_depth_ = value;
  LOG(INFO) << "BenchmarkInsertManager max_data_depth set to " << max_data_depth_;
}

void BenchmarkInsertManager::set_latest_states_prepare_parallelism(std::int32_t value) {
  latest_states_prepare_parallelism_ = std::max<std::int32_t>(1, value);
  LOG(INFO) << "BenchmarkInsertManager latest_states_prepare_parallelism set to "
            << latest_states_prepare_parallelism_;
}

void BenchmarkInsertManager::set_latest_states_prepare_chunk_size(std::int32_t value) {
  latest_states_prepare_chunk_size_ = std::max<std::int32_t>(1, value);
  LOG(INFO) << "BenchmarkInsertManager latest_states_prepare_chunk_size set to " << latest_states_prepare_chunk_size_;
}

void BenchmarkInsertManager::create_insert_actor(std::vector<InsertTaskStruct> insert_tasks,
                                                 td::Promise<InsertManagerInterface::InsertResult> promise) {
  td::actor::create_actor<BenchmarkInsertBatchActor>("benchmark_insert_batch", std::move(insert_tasks),
                                                     std::move(promise), stats_, max_data_depth_,
                                                     latest_states_prepare_parallelism_,
                                                     latest_states_prepare_chunk_size_)
      .release();
}

void BenchmarkInsertManager::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t,
                                                 std::int32_t) {
  promise.set_value({});
}

void BenchmarkInsertManager::ensure_resume_state_initialized(td::Promise<bool> promise, std::int32_t from_seqno) {
  if (from_seqno > 0) {
    resume_seqno_ = static_cast<std::uint32_t>(from_seqno);
  }
  promise.set_value(false);
}

void BenchmarkInsertManager::get_resume_seqno(td::Promise<InsertManagerInterface::ResumeState> promise,
                                              std::int32_t from_seqno, std::int32_t) {
  if (from_seqno > 0) {
    resume_seqno_ = static_cast<std::uint32_t>(from_seqno);
  }
  promise.set_value(InsertManagerInterface::ResumeState{
      .next_seqno = resume_seqno_,
      .initialized_from_cli = true,
  });
}
