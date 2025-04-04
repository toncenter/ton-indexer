#pragma once
#include <atomic>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <new>
#include <cassert>
#include "td/utils/ThreadLocalStorage.h"

constexpr size_t bucketCount() { 
  size_t count = 2; // 1 and 2
  double bucket_val = 2.0;
  constexpr double max = static_cast<double>(std::numeric_limits<uint64_t>::max());
  while ((bucket_val = 1.5 * bucket_val) <= max) {
    ++count;
  }
  return count;
};

consteval auto bucketValues() {
  std::array<uint64_t, bucketCount()> bucketValues;
  bucketValues[0] = 1;
  bucketValues[1] = 2;
  size_t i = 1;
  double bucket_val = static_cast<double>(bucketValues[i++]);
  constexpr double max = static_cast<double>(std::numeric_limits<uint64_t>::max());
  while ((bucket_val = 1.5 * bucket_val) <= max) {
    bucketValues[i++] = static_cast<uint64_t>(bucket_val);
  }
  if (bucketCount() != i) {
    throw "bucketValues() does not match bucketCount()";
  }
  return bucketValues;
}

class HistogramBucketMapper
{
public:
  consteval HistogramBucketMapper() : bucketValues_(bucketValues()) {}

  size_t IndexForValue(const uint64_t value) const {
    auto beg = bucketValues_.begin();
    auto end = bucketValues_.end();
    if (value >= LastValue())
      return end - beg - 1; // bucketValues_.size() - 1
    else
      return std::lower_bound(beg, end, value) - beg;
  }

  constexpr uint64_t LastValue() const { return bucketValues_.back(); }
  constexpr uint64_t FirstValue() const { return bucketValues_.front(); }
  constexpr uint64_t BucketCount() const { return bucketValues_.size(); }

  uint64_t BucketLimit(const size_t bucketNumber) const {
    assert(bucketNumber < BucketCount());
    return bucketValues_[bucketNumber];
  }

private:
  std::invoke_result_t<decltype(&bucketValues)> bucketValues_;
};

constexpr HistogramBucketMapper bucketMapper;

class HistogramImpl {
public:
  HistogramImpl() = default;
  void add(uint64_t duration, size_t count = 1);
  uint64_t get_count() const;
  uint64_t get_sum() const;
  double compute_percentile(double percentile) const;
  uint64_t get_max() const;
  void merge(const HistogramImpl &other);
  void reset();

private:
  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> sum_{0};
  std::atomic<uint64_t> min_{0};
  std::atomic<uint64_t> max_{0};
  std::vector<uint64_t> bucket_limits_;
  std::atomic<uint64_t> buckets_[bucketCount()];
  mutable std::mutex mutex_;

  static void update_max(std::atomic<uint64_t> &max, uint64_t value);
  static void update_min(std::atomic<uint64_t> &min, uint64_t value);
};

enum Ticker : uint32_t {
  SEQNO_FETCH_ERROR = 0,
  INSERT_CONFLICT,
  EMULATE_TRACE_ERROR,

  EMULATE_SRC_REDIS,
  EMULATE_SRC_OVERLAY,
  EMULATE_SRC_BLOCKS,
  TICKERS_COUNT
};

enum Histogram : uint32_t {
  PROCESS_SEQNO = 0,

  CATCH_UP_WITH_PRIMARY,
  FETCH_SEQNO,
  
  PARSE_SEQNO,
  PARSE_BLOCK,
  PARSE_TRANSACTION,
  PARSE_ACCOUNT_STATE,
  PARSE_CONFIG,
  
  TRACE_ASSEMBLER_GC_STATE,
  TRACE_ASSEMBLER_SAVE_STATE,
  TRACE_ASSEMBLER_PROCESS_BLOCK,

  DETECT_INTERFACES_SEQNO,
  DETECT_ACTIONS_SEQNO,

  INSERT_SEQNO,
  INSERT_BATCH_CONNECT,
  INSERT_BATCH_EXEC_DATA,
  INSERT_BATCH_EXEC_STATES,
  INSERT_BATCH_COMMIT,

  EMULATE_TRACE,
  INSERT_TRACE,
  
  HISTOGRAMS_COUNT
};

const std::unordered_map<uint32_t, std::string_view> ticker_names = {
    {SEQNO_FETCH_ERROR, "indexer.seqno.fetch.error"},
    {INSERT_CONFLICT, "indexer.insert.conflict"},
    {EMULATE_TRACE_ERROR, "emulator.emulate.trace.error"},
    {EMULATE_SRC_REDIS, "emulator.source.redis"},
    {EMULATE_SRC_OVERLAY, "emulator.source.overlay"},
    {EMULATE_SRC_BLOCKS, "emulator.source.blocks"}
};
const std::unordered_map<uint32_t, std::string_view> histogram_names = {
    {PROCESS_SEQNO, "indexer.index.seqno.millis"},
    {CATCH_UP_WITH_PRIMARY, "indexer.catch_up_with_primary.millis"},
    {FETCH_SEQNO, "indexer.fetch.seqno.millis"},
    {PARSE_SEQNO, "indexer.parse.seqno.millis"},
    {PARSE_BLOCK, "indexer.parse.block.millis"},
    {PARSE_TRANSACTION, "indexer.parse.transaction.micros"},
    {PARSE_ACCOUNT_STATE, "indexer.parse.account_state.micros"},
    {PARSE_CONFIG, "indexer.parse.config.micros"},
    {TRACE_ASSEMBLER_GC_STATE, "indexer.traceassembler.gc_state.micros"},
    {TRACE_ASSEMBLER_SAVE_STATE, "indexer.traceassembler.save_state.micros"},
    {TRACE_ASSEMBLER_PROCESS_BLOCK, "indexer.traceassembler.process_block.micros"},
    {DETECT_INTERFACES_SEQNO, "indexer.interfaces.seqno.millis"},
    {DETECT_ACTIONS_SEQNO, "indexer.actions.seqno.micros"},
    {INSERT_SEQNO, "indexer.insert.seqno.millis"},
    {INSERT_BATCH_CONNECT, "indexer.insert.batch.connect.millis"},
    {INSERT_BATCH_EXEC_DATA, "indexer.insert.batch.exec_data.millis"},
    {INSERT_BATCH_EXEC_STATES, "indexer.insert.batch.exec_states.millis"},
    {INSERT_BATCH_COMMIT, "indexer.insert.batch.commit.millis"},
    {EMULATE_TRACE, "emulator.emulate.trace.millis"},
    {INSERT_TRACE, "emulator.insert.trace.millis"},
};

class Statistics {
public:
  void record_time(Histogram hist, uint64_t duration, uint32_t count = 1);
  void record_count(Ticker ticker, uint64_t count = 1);
  std::string generate_report_and_reset();

private:
  struct StatisticsData {
    std::atomic_uint_fast64_t tickers_[TICKERS_COUNT] = {{0}};
    HistogramImpl histograms_[HISTOGRAMS_COUNT];
  };

  td::ThreadLocalStorage<StatisticsData> per_core_stats_;
};

extern Statistics g_statistics;
