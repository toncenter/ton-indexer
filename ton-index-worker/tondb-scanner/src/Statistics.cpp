#include "Statistics.h"
#include <sstream>
#include <iomanip>


void HistogramImpl::add(uint64_t duration, size_t count) {
    count_.fetch_add(count, std::memory_order_relaxed);
    sum_.fetch_add(duration, std::memory_order_relaxed);
    update_max(max_, duration);

    size_t index = bucketMapper.IndexForValue(duration);
    buckets_[index].fetch_add(count, std::memory_order_relaxed);
}

uint64_t HistogramImpl::get_count() const {
    return count_.load(std::memory_order_relaxed);
}

uint64_t HistogramImpl::get_sum() const {
    return sum_.load(std::memory_order_relaxed);
}

double HistogramImpl::compute_percentile(double percentile) const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t total = get_count();
    if (total == 0) return 0.0;

    double threshold = total * (percentile / 100.0);
    uint64_t accumulated = 0;
    size_t bucket_idx = 0;

    for (; bucket_idx < bucketCount(); ++bucket_idx) {
        uint64_t bucket_count = buckets_[bucket_idx].load(std::memory_order_relaxed);
        if (accumulated + bucket_count >= threshold) break;
        accumulated += bucket_count;
    }

    uint64_t lower = 0, upper = 0;
    if (bucket_idx == 0) {
        upper = bucketMapper.FirstValue();
    } else if (bucket_idx < bucketCount()) {
        lower = bucketMapper.BucketLimit(bucket_idx - 1) + 1;
        upper = bucketMapper.BucketLimit(bucket_idx);
    } else {
        lower = bucketMapper.LastValue() + 1;
        upper = max_.load(std::memory_order_relaxed);
    }

    uint64_t actual_max = max_.load(std::memory_order_relaxed);
    if (upper > actual_max) {
        upper = actual_max;
    }

    uint64_t bucket_count = buckets_[bucket_idx].load(std::memory_order_relaxed);
    if (bucket_count == 0) return upper;

    double fraction = (threshold - accumulated) / bucket_count;
    return lower + fraction * (upper - lower);
}

uint64_t HistogramImpl::get_max() const {
    return max_.load(std::memory_order_relaxed);
}

void HistogramImpl::merge(const HistogramImpl &other) {
    std::lock_guard<std::mutex> lock(mutex_);
    count_.fetch_add(other.count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    sum_.fetch_add(other.sum_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    update_max(max_, other.max_.load(std::memory_order_relaxed));
    update_min(min_, other.min_.load(std::memory_order_relaxed));

    for (size_t i = 0; i < bucketCount(); ++i) {
        buckets_[i].fetch_add(other.buckets_[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
}

void HistogramImpl::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    count_.store(0, std::memory_order_relaxed);
    sum_.store(0, std::memory_order_relaxed);
    max_.store(0, std::memory_order_relaxed);
    min_.store(0, std::memory_order_relaxed);

    for (size_t i = 0; i < bucketCount(); ++i) {
        buckets_[i].store(0, std::memory_order_relaxed);
    }
}

void HistogramImpl::update_max(std::atomic<uint64_t>& max, uint64_t value) {
    uint64_t current = max.load(std::memory_order_relaxed);
    while (value > current && !max.compare_exchange_weak(current, value, std::memory_order_relaxed));
}

void HistogramImpl::update_min(std::atomic<uint64_t>& min, uint64_t value) {
    uint64_t current = min.load(std::memory_order_relaxed);
    while (value < current && !min.compare_exchange_weak(current, value, std::memory_order_relaxed));
}

void Statistics::record_time(Histogram hist, uint64_t duration, uint32_t count) {
    if (count > 1) {
        // for batch events, we average the duration
        duration /= count;
    }
    per_core_stats_.get().histograms_[hist].add(duration, count);
}

void Statistics::record_count(Ticker ticker, uint64_t count) {
    per_core_stats_.get().tickers_[ticker].fetch_add(count, std::memory_order_relaxed);
}

std::string Statistics::generate_report_and_reset() {
  std::array<uint64_t, TICKERS_COUNT> aggTickers = {};
  std::array<HistogramImpl, HISTOGRAMS_COUNT> aggHist;

  per_core_stats_.for_each([&](StatisticsData &data) {
    // Merge tickers.
    for (uint32_t i = 0; i < TICKERS_COUNT; ++i) {
      uint64_t value = data.tickers_[i].load(std::memory_order_relaxed);
      aggTickers[i] += value;
      data.tickers_[i].store(0, std::memory_order_relaxed);
    }

    // Merge histograms.
    for (uint32_t h = 0; h < HISTOGRAMS_COUNT; ++h) {
      aggHist[h].merge(data.histograms_[h]);
      data.histograms_[h].reset();
    }
  });

  std::ostringstream oss;
  oss << std::setprecision(3) << std::fixed;
  for (uint32_t i = 0; i < TICKERS_COUNT; ++i) {
    oss << ticker_names.at(i) << " COUNT : " << aggTickers[i] << std::endl;
  }

  for (uint32_t h = 0; h < HISTOGRAMS_COUNT; ++h) {
    oss << histogram_names.at(h) << " P50 : " << aggHist[h].compute_percentile(50.0)
                                 << " P95 : " << aggHist[h].compute_percentile(95.0)
                                 << " P99 : " << aggHist[h].compute_percentile(99.0)
                                 << " P100 : " << aggHist[h].get_max()
                                 << " COUNT : " << aggHist[h].get_count()
                                 << " SUM : " << aggHist[h].get_sum() << std::endl;
  }

  return oss.str();
}

Statistics g_statistics;