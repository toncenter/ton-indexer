#pragma once

#include <atomic>
#include <cstdint>
#include <limits>

// Progress values observed by get_resume_seqno at startup, shared with the
// failover watchdog so that its regression baselines start from what the
// resume logic actually acted upon. Without this, a failover happening
// between the resume read and the watchdog's first sample would go unnoticed:
// the watchdog would adopt the already-regressed value as its baseline while
// the worker keeps indexing from a resume point computed with the higher one.
struct FailoverBaseline {
  static constexpr std::int64_t kUnset = std::numeric_limits<std::int64_t>::min();

  std::atomic<std::int64_t> finalized_mc_seqno{kUnset};
  std::atomic<std::int64_t> kvrocks_watermark{kUnset};
};
