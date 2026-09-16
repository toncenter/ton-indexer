#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_context_kv_iterable_view.h"
#include "opentelemetry/trace/span_id.h"
#include "opentelemetry/trace/trace_flags.h"
#include "opentelemetry/trace/trace_id.h"
#include "td/utils/tests.h"

#include "Otel.h"
#include "OtelSampler.h"

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;

namespace {

trace_sdk::Decision sampling_decision(trace_sdk::Sampler& sampler, const trace_api::SpanContext& parent) {
  const std::uint8_t trace_id_bytes[trace_api::TraceId::kSize] = {1};
  const trace_api::TraceId trace_id(trace_id_bytes);
  const std::map<std::string, int> attributes;
  const std::vector<std::pair<trace_api::SpanContext, std::map<std::string, std::string>>> links;
  const opentelemetry::common::KeyValueIterableView<decltype(attributes)> attribute_view(attributes);
  const trace_api::SpanContextKeyValueIterableView<decltype(links)> links_view(links);

  return sampler.ShouldSample(parent, trace_id, "test", trace_api::SpanKind::kInternal, attribute_view, links_view)
      .decision;
}

trace_api::SpanContext parent(bool sampled) {
  const std::uint8_t trace_id_bytes[trace_api::TraceId::kSize] = {1};
  const std::uint8_t span_id_bytes[trace_api::SpanId::kSize] = {1};
  return trace_api::SpanContext(trace_api::TraceId(trace_id_bytes), trace_api::SpanId(span_id_bytes),
                                trace_api::TraceFlags(sampled ? trace_api::TraceFlags::kIsSampled : 0), true);
}

}  // namespace

TEST(OtelSampler, parent_based_ratio_makes_the_decision_only_for_root_spans) {
  auto sampler = ton::observability::make_otel_sampler("parentbased_traceidratio", "0");

  ASSERT_EQ(trace_sdk::Decision::DROP, sampling_decision(*sampler, trace_api::SpanContext::GetInvalid()));
  ASSERT_EQ(trace_sdk::Decision::RECORD_AND_SAMPLE, sampling_decision(*sampler, parent(true)));
  ASSERT_EQ(trace_sdk::Decision::DROP, sampling_decision(*sampler, parent(false)));
}

TEST(OtelSampler, parent_based_ratio_one_samples_root_spans) {
  auto sampler = ton::observability::make_otel_sampler("parentbased_traceidratio", "1");

  ASSERT_EQ(trace_sdk::Decision::RECORD_AND_SAMPLE, sampling_decision(*sampler, trace_api::SpanContext::GetInvalid()));
}

TEST(OtelSampler, rate_limiter_keeps_all_traces_when_the_input_rate_is_low) {
  using Clock = ton::observability::TraceRateLimiter::Clock;

  auto now = Clock::time_point{};
  ton::observability::TraceRateLimiter limiter(10.0, now);

  for (int i = 0; i < 20; ++i) {
    ASSERT_TRUE(limiter.try_acquire(now));
    now += std::chrono::milliseconds(200);
  }
}

TEST(OtelSampler, rate_limiter_caps_a_burst_and_refills_over_time) {
  using Clock = ton::observability::TraceRateLimiter::Clock;

  const auto started_at = Clock::time_point{};
  ton::observability::TraceRateLimiter limiter(2.0, started_at);

  ASSERT_TRUE(limiter.try_acquire(started_at));
  ASSERT_TRUE(limiter.try_acquire(started_at));
  ASSERT_TRUE(!limiter.try_acquire(started_at));

  const auto half_second_later = started_at + std::chrono::milliseconds(500);
  ASSERT_TRUE(limiter.try_acquire(half_second_later));
  ASSERT_TRUE(!limiter.try_acquire(half_second_later));

  const auto one_second_later = half_second_later + std::chrono::seconds(1);
  ASSERT_TRUE(limiter.try_acquire(one_second_later));
  ASSERT_TRUE(limiter.try_acquire(one_second_later));
  ASSERT_TRUE(!limiter.try_acquire(one_second_later));
}

TEST(OtelSampler, rate_limiter_shares_one_budget_between_threads) {
  using Clock = ton::observability::TraceRateLimiter::Clock;

  const auto now = Clock::time_point{};
  ton::observability::TraceRateLimiter limiter(100.0, now);
  std::atomic<int> accepted{0};
  std::vector<std::thread> workers;

  for (int worker = 0; worker < 8; ++worker) {
    workers.emplace_back([&] {
      for (int attempt = 0; attempt < 100; ++attempt) {
        if (limiter.try_acquire(now)) {
          accepted.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  ASSERT_EQ(100, accepted.load(std::memory_order_relaxed));
}

TEST(OtelSampler, rate_limiter_only_limits_root_spans) {
  auto sampler = ton::observability::make_rate_limited_otel_sampler(
      ton::observability::make_otel_sampler("parentbased_always_on", ""), 1.0);

  ASSERT_EQ(trace_sdk::Decision::RECORD_AND_SAMPLE, sampling_decision(*sampler, trace_api::SpanContext::GetInvalid()));
  ASSERT_EQ(trace_sdk::Decision::DROP, sampling_decision(*sampler, trace_api::SpanContext::GetInvalid()));

  ASSERT_EQ(trace_sdk::Decision::RECORD_AND_SAMPLE, sampling_decision(*sampler, parent(true)));
  ASSERT_EQ(trace_sdk::Decision::DROP, sampling_decision(*sampler, parent(false)));
}

TEST(OtelSampler, max_traces_per_second_environment_variable_enables_the_rate_limiter) {
  setenv("OTEL_TRACES_SAMPLER", "parentbased_always_on", 1);
  setenv("TON_OTEL_MAX_TRACES_PER_SECOND", "1", 1);
  auto sampler = ton::observability::make_otel_sampler_from_environment();
  unsetenv("TON_OTEL_MAX_TRACES_PER_SECOND");
  unsetenv("OTEL_TRACES_SAMPLER");

  ASSERT_EQ(trace_sdk::Decision::RECORD_AND_SAMPLE, sampling_decision(*sampler, trace_api::SpanContext::GetInvalid()));
  ASSERT_EQ(trace_sdk::Decision::DROP, sampling_decision(*sampler, trace_api::SpanContext::GetInvalid()));
}

TEST(OtelSampler, unsampled_root_still_propagates_the_skip_decision) {
  setenv("TON_OTEL_ENABLED", "true", 1);
  setenv("OTEL_TRACES_EXPORTER", "otlp", 1);
  setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio", 1);
  setenv("OTEL_TRACES_SAMPLER_ARG", "0", 1);

  OtelStageSpan span("test", "test.root", "test", OtelStageSpan::system_now_ns(), OtelStageSpan::steady_now_ns());
  const auto fields = span.propagation_fields();
  const auto otel_data = fields.find("otel_data");
  span.end();

  unsetenv("OTEL_TRACES_SAMPLER_ARG");
  unsetenv("OTEL_TRACES_SAMPLER");
  unsetenv("OTEL_TRACES_EXPORTER");
  unsetenv("TON_OTEL_ENABLED");

  ASSERT_TRUE(otel_data != fields.end());
  ASSERT_TRUE(otel_data->second.find("-00\"") != std::string::npos);
}
