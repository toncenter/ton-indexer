#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>

#include "opentelemetry/sdk/trace/sampler.h"

namespace ton::observability {

class TraceRateLimiter {
 public:
  using Clock = std::chrono::steady_clock;

  explicit TraceRateLimiter(double max_traces_per_second, Clock::time_point now = Clock::now());

  bool try_acquire(Clock::time_point now = Clock::now());

 private:
  const double max_traces_per_second_;
  const double capacity_;
  double available_tokens_;
  Clock::time_point last_refill_;
  std::mutex mutex_;
};

std::unique_ptr<opentelemetry::sdk::trace::Sampler> make_otel_sampler(std::string name, std::string argument);
std::unique_ptr<opentelemetry::sdk::trace::Sampler> make_rate_limited_otel_sampler(
    std::unique_ptr<opentelemetry::sdk::trace::Sampler> sampler, double max_traces_per_second);
std::unique_ptr<opentelemetry::sdk::trace::Sampler> make_otel_sampler_from_environment();

}  // namespace ton::observability
