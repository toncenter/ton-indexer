#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

class BenchmarkTiming {
 public:
  void add(std::uint64_t value);
  std::uint64_t count() const;
  std::uint64_t sum() const;
  std::uint64_t max() const;
  double avg() const;

 private:
  std::atomic<std::uint64_t> count_{0};
  std::atomic<std::uint64_t> sum_{0};
  std::atomic<std::uint64_t> max_{0};
};

struct BenchmarkBatchCounters {
  std::uint64_t mc_seqnos{0};
  std::uint64_t blocks{0};
  std::uint64_t transactions{0};
  std::uint64_t messages{0};
  std::uint64_t traces{0};
  std::uint64_t account_states_seen{0};
  std::uint64_t latest_account_states{0};
};

struct BenchmarkPrepareCounters {
  std::uint64_t rows{0};
  std::uint64_t rows_with_code{0};
  std::uint64_t rows_with_data{0};
  std::uint64_t code_serialize_count{0};
  std::uint64_t code_serialize_errors{0};
  std::uint64_t code_boc_raw_bytes{0};
  std::uint64_t code_boc_base64_bytes{0};
  std::uint64_t data_depth_checks{0};
  std::uint64_t data_serialize_count{0};
  std::uint64_t data_serialize_errors{0};
  std::uint64_t data_skipped_by_depth{0};
  std::uint64_t data_boc_raw_bytes{0};
  std::uint64_t data_boc_base64_bytes{0};
};

class BenchmarkStats {
 public:
  void mark_benchmark_started();

  void record_batch_input(const BenchmarkBatchCounters& counters);
  void record_prepare_counters(const BenchmarkPrepareCounters& counters);
  void record_batch_prepare_us(std::uint64_t value);
  void record_collect_us(std::uint64_t value);
  void record_chunk_prepare_us(std::uint64_t value);
  void record_data_depth_us(std::uint64_t value);
  void record_code_serialize_us(std::uint64_t value);
  void record_data_serialize_us(std::uint64_t value);
  void record_base64_us(std::uint64_t value);

  std::string summary() const;

 private:
  static void add(std::atomic<std::uint64_t>& target, std::uint64_t value);
  static void append_counter(std::string& out, const char* name, std::uint64_t value);
  static void append_rate(std::string& out, const char* name, std::uint64_t value, double elapsed_seconds);
  static void append_timing(std::string& out, const char* name, const BenchmarkTiming& timing, const char* unit);

  std::uint64_t elapsed_process_us() const;
  double benchmark_elapsed_seconds(std::uint64_t process_elapsed_us) const;

  const std::chrono::steady_clock::time_point process_started_at_{std::chrono::steady_clock::now()};
  std::atomic<std::uint64_t> benchmark_started_at_process_us_{0};

  std::atomic<std::uint64_t> mc_seqnos_{0};
  std::atomic<std::uint64_t> blocks_{0};
  std::atomic<std::uint64_t> transactions_{0};
  std::atomic<std::uint64_t> messages_{0};
  std::atomic<std::uint64_t> traces_{0};
  std::atomic<std::uint64_t> account_states_seen_{0};
  std::atomic<std::uint64_t> latest_account_states_{0};

  std::atomic<std::uint64_t> rows_{0};
  std::atomic<std::uint64_t> rows_with_code_{0};
  std::atomic<std::uint64_t> rows_with_data_{0};
  std::atomic<std::uint64_t> code_serialize_count_{0};
  std::atomic<std::uint64_t> code_serialize_errors_{0};
  std::atomic<std::uint64_t> code_boc_raw_bytes_{0};
  std::atomic<std::uint64_t> code_boc_base64_bytes_{0};
  std::atomic<std::uint64_t> data_depth_checks_{0};
  std::atomic<std::uint64_t> data_serialize_count_{0};
  std::atomic<std::uint64_t> data_serialize_errors_{0};
  std::atomic<std::uint64_t> data_skipped_by_depth_{0};
  std::atomic<std::uint64_t> data_boc_raw_bytes_{0};
  std::atomic<std::uint64_t> data_boc_base64_bytes_{0};

  BenchmarkTiming batch_prepare_us_;
  BenchmarkTiming collect_us_;
  BenchmarkTiming chunk_prepare_us_;
  BenchmarkTiming data_depth_us_;
  BenchmarkTiming code_serialize_us_;
  BenchmarkTiming data_serialize_us_;
  BenchmarkTiming base64_us_;
};
