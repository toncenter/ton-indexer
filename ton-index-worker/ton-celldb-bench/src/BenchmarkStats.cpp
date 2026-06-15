#include "BenchmarkStats.h"

#include <iomanip>
#include <sstream>

void BenchmarkTiming::add(std::uint64_t value) {
  count_.fetch_add(1, std::memory_order_relaxed);
  sum_.fetch_add(value, std::memory_order_relaxed);

  auto current = max_.load(std::memory_order_relaxed);
  while (value > current && !max_.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
  }
}

std::uint64_t BenchmarkTiming::count() const {
  return count_.load(std::memory_order_relaxed);
}

std::uint64_t BenchmarkTiming::sum() const {
  return sum_.load(std::memory_order_relaxed);
}

std::uint64_t BenchmarkTiming::max() const {
  return max_.load(std::memory_order_relaxed);
}

double BenchmarkTiming::avg() const {
  auto total = count();
  if (total == 0) {
    return 0.0;
  }
  return static_cast<double>(sum()) / static_cast<double>(total);
}

void BenchmarkStats::mark_benchmark_started() {
  auto expected = std::uint64_t{0};
  auto started_at = elapsed_process_us();
  if (started_at == 0) {
    started_at = 1;
  }
  benchmark_started_at_process_us_.compare_exchange_strong(expected, started_at, std::memory_order_relaxed);
}

void BenchmarkStats::record_batch_input(const BenchmarkBatchCounters& counters) {
  add(mc_seqnos_, counters.mc_seqnos);
  add(blocks_, counters.blocks);
  add(transactions_, counters.transactions);
  add(messages_, counters.messages);
  add(traces_, counters.traces);
  add(account_states_seen_, counters.account_states_seen);
  add(latest_account_states_, counters.latest_account_states);
}

void BenchmarkStats::record_prepare_counters(const BenchmarkPrepareCounters& counters) {
  add(rows_, counters.rows);
  add(rows_with_code_, counters.rows_with_code);
  add(rows_with_data_, counters.rows_with_data);
  add(code_serialize_count_, counters.code_serialize_count);
  add(code_serialize_errors_, counters.code_serialize_errors);
  add(code_boc_raw_bytes_, counters.code_boc_raw_bytes);
  add(code_boc_base64_bytes_, counters.code_boc_base64_bytes);
  add(data_depth_checks_, counters.data_depth_checks);
  add(data_serialize_count_, counters.data_serialize_count);
  add(data_serialize_errors_, counters.data_serialize_errors);
  add(data_skipped_by_depth_, counters.data_skipped_by_depth);
  add(data_boc_raw_bytes_, counters.data_boc_raw_bytes);
  add(data_boc_base64_bytes_, counters.data_boc_base64_bytes);
}

void BenchmarkStats::record_batch_prepare_us(std::uint64_t value) {
  batch_prepare_us_.add(value);
}

void BenchmarkStats::record_collect_us(std::uint64_t value) {
  collect_us_.add(value);
}

void BenchmarkStats::record_chunk_prepare_us(std::uint64_t value) {
  chunk_prepare_us_.add(value);
}

void BenchmarkStats::record_data_depth_us(std::uint64_t value) {
  data_depth_us_.add(value);
}

void BenchmarkStats::record_code_serialize_us(std::uint64_t value) {
  code_serialize_us_.add(value);
}

void BenchmarkStats::record_data_serialize_us(std::uint64_t value) {
  data_serialize_us_.add(value);
}

void BenchmarkStats::record_base64_us(std::uint64_t value) {
  base64_us_.add(value);
}

std::string BenchmarkStats::summary() const {
  const auto process_elapsed_us = elapsed_process_us();
  const auto process_elapsed = static_cast<double>(process_elapsed_us) / 1'000'000.0;
  const auto elapsed = benchmark_elapsed_seconds(process_elapsed_us);
  const auto benchmark_started_at_us = benchmark_started_at_process_us_.load(std::memory_order_relaxed);
  std::string out;
  std::ostringstream header;
  header << std::fixed << std::setprecision(3);
  header << "ton-celldb-bench summary\n";
  header << "elapsed_seconds: " << elapsed << "\n";
  header << "process_elapsed_seconds: " << process_elapsed << "\n";
  header << "startup_excluded_seconds: " << static_cast<double>(benchmark_started_at_us) / 1'000'000.0 << "\n";
  out += header.str();

  const auto mc_seqnos = mc_seqnos_.load(std::memory_order_relaxed);
  const auto blocks = blocks_.load(std::memory_order_relaxed);
  const auto transactions = transactions_.load(std::memory_order_relaxed);
  const auto messages = messages_.load(std::memory_order_relaxed);
  const auto latest_account_states = latest_account_states_.load(std::memory_order_relaxed);

  append_counter(out, "mc_seqnos", mc_seqnos);
  append_counter(out, "blocks", blocks);
  append_counter(out, "transactions", transactions);
  append_counter(out, "messages", messages);
  append_counter(out, "traces", traces_.load(std::memory_order_relaxed));
  append_counter(out, "account_states_seen", account_states_seen_.load(std::memory_order_relaxed));
  append_counter(out, "latest_account_states", latest_account_states);
  append_rate(out, "mc_seqnos_per_second", mc_seqnos, elapsed);
  append_rate(out, "blocks_per_second", blocks, elapsed);
  append_rate(out, "transactions_per_second", transactions, elapsed);
  append_rate(out, "messages_per_second", messages, elapsed);
  append_rate(out, "latest_account_states_per_second", latest_account_states, elapsed);

  append_counter(out, "prepare_rows", rows_.load(std::memory_order_relaxed));
  append_counter(out, "rows_with_code", rows_with_code_.load(std::memory_order_relaxed));
  append_counter(out, "rows_with_data", rows_with_data_.load(std::memory_order_relaxed));
  append_counter(out, "code_serialize_count", code_serialize_count_.load(std::memory_order_relaxed));
  append_counter(out, "code_serialize_errors", code_serialize_errors_.load(std::memory_order_relaxed));
  append_counter(out, "code_boc_raw_bytes", code_boc_raw_bytes_.load(std::memory_order_relaxed));
  append_counter(out, "code_boc_base64_bytes", code_boc_base64_bytes_.load(std::memory_order_relaxed));
  append_counter(out, "data_depth_checks", data_depth_checks_.load(std::memory_order_relaxed));
  append_counter(out, "data_serialize_count", data_serialize_count_.load(std::memory_order_relaxed));
  append_counter(out, "data_serialize_errors", data_serialize_errors_.load(std::memory_order_relaxed));
  append_counter(out, "data_skipped_by_depth", data_skipped_by_depth_.load(std::memory_order_relaxed));
  append_counter(out, "data_boc_raw_bytes", data_boc_raw_bytes_.load(std::memory_order_relaxed));
  append_counter(out, "data_boc_base64_bytes", data_boc_base64_bytes_.load(std::memory_order_relaxed));

  append_timing(out, "batch_prepare", batch_prepare_us_, "us");
  append_timing(out, "collect_latest_states", collect_us_, "us");
  append_timing(out, "chunk_prepare", chunk_prepare_us_, "us");
  append_timing(out, "data_depth", data_depth_us_, "us");
  append_timing(out, "code_serialize", code_serialize_us_, "us");
  append_timing(out, "data_serialize", data_serialize_us_, "us");
  append_timing(out, "base64", base64_us_, "us");

  return out;
}

void BenchmarkStats::add(std::atomic<std::uint64_t>& target, std::uint64_t value) {
  target.fetch_add(value, std::memory_order_relaxed);
}

std::uint64_t BenchmarkStats::elapsed_process_us() const {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                       std::chrono::steady_clock::now() - process_started_at_)
                                       .count());
}

double BenchmarkStats::benchmark_elapsed_seconds(std::uint64_t process_elapsed_us) const {
  const auto started_at_us = benchmark_started_at_process_us_.load(std::memory_order_relaxed);
  if (started_at_us == 0 || process_elapsed_us <= started_at_us) {
    return 0.0;
  }
  return static_cast<double>(process_elapsed_us - started_at_us) / 1'000'000.0;
}

void BenchmarkStats::append_counter(std::string& out, const char* name, std::uint64_t value) {
  out += name;
  out += ": ";
  out += std::to_string(value);
  out += "\n";
}

void BenchmarkStats::append_rate(std::string& out, const char* name, std::uint64_t value, double elapsed_seconds) {
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(3);
  ss << name << ": " << (elapsed_seconds > 0.0 ? static_cast<double>(value) / elapsed_seconds : 0.0) << "\n";
  out += ss.str();
}

void BenchmarkStats::append_timing(std::string& out, const char* name, const BenchmarkTiming& timing,
                                   const char* unit) {
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(3);
  ss << name << "_" << unit
     << " avg=" << timing.avg()
     << " max=" << timing.max()
     << " count=" << timing.count()
     << " sum=" << timing.sum()
     << "\n";
  out += ss.str();
}
