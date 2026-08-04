#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/trace/samplers/always_off_factory.h"
#include "opentelemetry/sdk/trace/samplers/always_on_factory.h"
#include "opentelemetry/sdk/trace/samplers/parent_factory.h"
#include "opentelemetry/sdk/trace/samplers/trace_id_ratio_factory.h"
#include "opentelemetry/trace/span_context.h"

#include "OtelSampler.h"

namespace trace_sdk = opentelemetry::sdk::trace;

namespace ton::observability {
namespace {

constexpr const char* kMaxTracesPerSecondEnv = "TON_OTEL_MAX_TRACES_PER_SECOND";

std::string normalize(std::string value) {
  const auto first = value.find_first_not_of(" \t\n\r");
  if (first == std::string::npos) {
    return {};
  }
  const auto last = value.find_last_not_of(" \t\n\r");
  value = value.substr(first, last - first + 1);
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

double parse_ratio(const std::string& argument) {
  if (argument.empty()) {
    return 1.0;
  }

  try {
    std::size_t parsed = 0;
    const auto ratio = std::stod(argument, &parsed);
    if (parsed == argument.size() && std::isfinite(ratio) && ratio >= 0.0 && ratio <= 1.0) {
      return ratio;
    }
  } catch (...) {
  }

  OTEL_INTERNAL_LOG_WARN("Invalid OTEL_TRACES_SAMPLER_ARG <" << argument << ">, defaulting to 1.0");
  return 1.0;
}

std::optional<double> parse_max_traces_per_second(const std::string& argument) {
  if (argument.empty()) {
    return std::nullopt;
  }

  try {
    std::size_t parsed = 0;
    const auto rate = std::stod(argument, &parsed);
    if (parsed == argument.size() && std::isfinite(rate) && rate > 0.0) {
      return rate;
    }
  } catch (...) {
  }

  OTEL_INTERNAL_LOG_WARN("Invalid " << kMaxTracesPerSecondEnv << " <" << argument << ">, rate limiting disabled");
  return std::nullopt;
}

std::unique_ptr<trace_sdk::Sampler> make_parent_based(std::unique_ptr<trace_sdk::Sampler> root) {
  std::shared_ptr<trace_sdk::Sampler> shared_root = std::move(root);
  return trace_sdk::ParentBasedSamplerFactory::Create(shared_root);
}

std::unique_ptr<trace_sdk::Sampler> make_parent_based_always_on() {
  return make_parent_based(trace_sdk::AlwaysOnSamplerFactory::Create());
}

class RootRateLimitingSampler final : public trace_sdk::Sampler {
 public:
  RootRateLimitingSampler(std::unique_ptr<trace_sdk::Sampler> sampler, double max_traces_per_second)
      : sampler_(std::move(sampler))
      , rate_limiter_(max_traces_per_second)
      , description_("RootRateLimitingSampler{" + std::string(sampler_->GetDescription()) + "," +
                     std::to_string(max_traces_per_second) + "}") {
  }

  trace_sdk::SamplingResult ShouldSample(
      const opentelemetry::trace::SpanContext& parent_context, opentelemetry::trace::TraceId trace_id,
      opentelemetry::nostd::string_view name, opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable& attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable& links) noexcept override {
    auto result = sampler_->ShouldSample(parent_context, trace_id, name, span_kind, attributes, links);
    if (parent_context.IsValid() || !result.IsSampled() || rate_limiter_.try_acquire()) {
      return result;
    }
    return {trace_sdk::Decision::DROP, nullptr, {}};
  }

  opentelemetry::nostd::string_view GetDescription() const noexcept override {
    return description_;
  }

 private:
  std::unique_ptr<trace_sdk::Sampler> sampler_;
  TraceRateLimiter rate_limiter_;
  std::string description_;
};

}  // namespace

TraceRateLimiter::TraceRateLimiter(double max_traces_per_second, Clock::time_point now)
    : max_traces_per_second_(max_traces_per_second)
    , capacity_(std::max(1.0, max_traces_per_second))
    , available_tokens_(capacity_)
    , last_refill_(now) {
}

bool TraceRateLimiter::try_acquire(Clock::time_point now) {
  std::lock_guard lock(mutex_);

  if (now > last_refill_) {
    const auto elapsed_seconds = std::chrono::duration<double>(now - last_refill_).count();
    available_tokens_ = std::min(capacity_, available_tokens_ + elapsed_seconds * max_traces_per_second_);
    last_refill_ = now;
  }

  if (available_tokens_ < 1.0) {
    return false;
  }

  available_tokens_ -= 1.0;
  return true;
}

std::unique_ptr<trace_sdk::Sampler> make_otel_sampler(std::string name, std::string argument) {
  name = normalize(std::move(name));
  argument = normalize(std::move(argument));

  if (name.empty() || name == "parentbased_always_on") {
    return make_parent_based_always_on();
  }
  if (name == "always_on") {
    return trace_sdk::AlwaysOnSamplerFactory::Create();
  }
  if (name == "always_off") {
    return trace_sdk::AlwaysOffSamplerFactory::Create();
  }
  if (name == "traceidratio") {
    return trace_sdk::TraceIdRatioBasedSamplerFactory::Create(parse_ratio(argument));
  }
  if (name == "parentbased_always_off") {
    return make_parent_based(trace_sdk::AlwaysOffSamplerFactory::Create());
  }
  if (name == "parentbased_traceidratio") {
    return make_parent_based(trace_sdk::TraceIdRatioBasedSamplerFactory::Create(parse_ratio(argument)));
  }

  OTEL_INTERNAL_LOG_WARN("Unsupported OTEL_TRACES_SAMPLER <" << name << ">, defaulting to parentbased_always_on");
  return make_parent_based_always_on();
}

std::unique_ptr<trace_sdk::Sampler> make_rate_limited_otel_sampler(std::unique_ptr<trace_sdk::Sampler> sampler,
                                                                   double max_traces_per_second) {
  return std::make_unique<RootRateLimitingSampler>(std::move(sampler), max_traces_per_second);
}

std::unique_ptr<trace_sdk::Sampler> make_otel_sampler_from_environment() {
  const auto* name = std::getenv("OTEL_TRACES_SAMPLER");
  const auto* argument = std::getenv("OTEL_TRACES_SAMPLER_ARG");
  auto sampler = make_otel_sampler(name ? name : "", argument ? argument : "");

  const auto* raw_max_traces_per_second = std::getenv(kMaxTracesPerSecondEnv);
  const auto max_traces_per_second =
      parse_max_traces_per_second(normalize(raw_max_traces_per_second ? raw_max_traces_per_second : ""));
  if (!max_traces_per_second) {
    return sampler;
  }
  return make_rate_limited_otel_sampler(std::move(sampler), *max_traces_per_second);
}

}  // namespace ton::observability
