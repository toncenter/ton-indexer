#include "Otel.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <unistd.h>

#include "opentelemetry/baggage/baggage.h"
#include "opentelemetry/baggage/baggage_context.h"
#include "opentelemetry/baggage/propagation/baggage_propagator.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/common/disabled.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/span.h"
#include "opentelemetry/trace/span_startoptions.h"
#include "opentelemetry/trace/tracer.h"
#include "td/utils/JsonBuilder.h"

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace resource_api = opentelemetry::sdk::resource;
namespace baggage_api = opentelemetry::baggage;
namespace context_api = opentelemetry::context;
namespace otlp = opentelemetry::exporter::otlp;

namespace {

constexpr const char* kScopeName = "ton-indexer.observability";
constexpr const char* kScopeVersion = "1.0.0";
constexpr const char* kOtelDataField = "otel_data";

constexpr std::array<const char*, 5> kPropagationBaggageKeys = {
    "ton.trace.external_message_hash",
    "ton.trace.external_message_hash_norm",
    "ton.trace.finality",
    "ton.processing.pass_id",
    "ton.trace.root_tx_hash",
};

bool env_flag(const char* name, bool default_value) {
    const char* raw = std::getenv(name);
    if (!raw) {
        return default_value;
    }

    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });

    return value == "1" || value == "true" || value == "yes" || value == "on";
}

std::string getenv_or_empty(const char* name) {
    const char* value = std::getenv(name);
    return value ? std::string(value) : std::string();
}

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) {
        return {};
    }
    const auto last = value.find_last_not_of(" \t\n\r");
    return value.substr(first, last - first + 1);
}

std::string normalized_env(const char* name) {
    auto value = trim(getenv_or_empty(name));
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string signal_env(const char* traces_name, const char* generic_name) {
    auto value = trim(getenv_or_empty(traces_name));
    if (!value.empty()) {
        return value;
    }
    return trim(getenv_or_empty(generic_name));
}

bool traces_exporter_enabled() {
    const auto exporter = normalized_env("OTEL_TRACES_EXPORTER");
    return exporter.empty() || exporter == "otlp";
}

bool tracing_runtime_enabled() {
    return env_flag("TON_OTEL_ENABLED", false) &&
           !opentelemetry::sdk::common::GetSdkDisabled() &&
           traces_exporter_enabled();
}

long exporter_timeout_ms() {
    auto raw = signal_env("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "OTEL_EXPORTER_OTLP_TIMEOUT");
    if (raw.empty()) {
        return 1000;
    }

    try {
        const auto timeout = std::stol(raw);
        return timeout > 0 ? timeout : 1000;
    } catch (...) {
        return 1000;
    }
}

std::string random_hex(std::size_t bytes_count) {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<unsigned int> dist(0, 255);

    std::ostringstream oss;
    oss << std::hex;

    for (std::size_t i = 0; i < bytes_count; ++i) {
        const auto value = dist(rng);
        if (value < 16) {
            oss << '0';
        }
        oss << value;
    }

    return oss.str();
}

std::string hostname_or_empty() {
    std::array<char, 256> buffer{};
    if (gethostname(buffer.data(), buffer.size()) != 0) {
        return {};
    }

    buffer.back() = '\0';
    return std::string(std::string_view(buffer.data()));
}

resource_api::Resource build_resource(const std::string& service_name) {
    resource_api::ResourceAttributes attributes;
    attributes["service.namespace"] = std::string("ton");
    attributes["service.name"] = service_name;
    if (const auto hostname = hostname_or_empty(); !hostname.empty()) {
        attributes["service.instance.id"] = hostname;
    }
    return resource_api::Resource::Create(attributes);
}

opentelemetry::common::SystemTimestamp system_timestamp_from_ns(std::int64_t value) {
    return opentelemetry::common::SystemTimestamp(
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>(
            std::chrono::nanoseconds(value)));
}

opentelemetry::common::SteadyTimestamp steady_timestamp_from_ns(std::int64_t value) {
    return opentelemetry::common::SteadyTimestamp(
        std::chrono::time_point<std::chrono::steady_clock, std::chrono::nanoseconds>(
            std::chrono::nanoseconds(value)));
}

void set_span_attribute(
    const opentelemetry::nostd::shared_ptr<trace_api::Span>& span,
    const std::string& key,
    const OtelStageSpan::AttributeValue& value) {
    if (!span) {
        return;
    }

    if (std::holds_alternative<std::string>(value)) {
        span->SetAttribute(key, std::get<std::string>(value));
        return;
    }

    if (std::holds_alternative<bool>(value)) {
        span->SetAttribute(key, std::get<bool>(value));
        return;
    }

    span->SetAttribute(key, std::get<std::int64_t>(value));
}

std::optional<std::string> string_attribute(
    const std::map<std::string, OtelStageSpan::AttributeValue>& attributes,
    const std::string& key) {
    const auto it = attributes.find(key);
    if (it == attributes.end() || !std::holds_alternative<std::string>(it->second)) {
        return std::nullopt;
    }
    return std::get<std::string>(it->second);
}

class HeaderCarrier final : public context_api::propagation::TextMapCarrier {
public:
    opentelemetry::nostd::string_view Get(
        opentelemetry::nostd::string_view key) const noexcept override {
        const auto it = headers_.find(std::string(key));
        if (it == headers_.end()) {
            return {};
        }
        return it->second;
    }

    void Set(
        opentelemetry::nostd::string_view key,
        opentelemetry::nostd::string_view value) noexcept override {
        headers_[std::string(key)] = std::string(value);
    }

    std::string value_or(const std::string& key) const {
        const auto it = headers_.find(key);
        return it == headers_.end() ? std::string() : it->second;
    }

private:
    std::map<std::string, std::string> headers_;
};

std::string build_otel_data_json(const HeaderCarrier& carrier) {
    td::JsonBuilder jb;
    auto obj = jb.enter_object();
    obj("traceparent", carrier.value_or("traceparent"));
    obj("tracestate", carrier.value_or("tracestate"));
    obj("baggage", carrier.value_or("baggage"));
    obj.leave();
    return jb.string_builder().as_cslice().str();
}

opentelemetry::nostd::shared_ptr<baggage_api::Baggage> build_baggage(
    const std::map<std::string, OtelStageSpan::AttributeValue>& attributes) {
    auto baggage = baggage_api::Baggage::GetDefault();

    for (const auto* key : kPropagationBaggageKeys) {
        const auto value = string_attribute(attributes, key);
        if (!value || value->empty()) {
            continue;
        }
        baggage = baggage->Set(key, *value);
    }

    return baggage;
}

class TracerRuntime {
public:
    TracerRuntime() = default;

    explicit TracerRuntime(std::string service_name) {
        trace_sdk::BatchSpanProcessorOptions processor_options;
        auto exporter = otlp::OtlpHttpExporterFactory::Create();
        auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
            std::move(exporter), processor_options);

        provider_ = trace_sdk::TracerProviderFactory::Create(
            std::move(processor), build_resource(service_name));
        tracer_ = provider_->GetTracer(kScopeName, kScopeVersion);
    }

    ~TracerRuntime() {
        if (!provider_) {
            return;
        }

        const auto timeout = std::chrono::milliseconds(exporter_timeout_ms());
        provider_->ForceFlush(timeout);
        provider_->Shutdown(timeout);
    }

    opentelemetry::nostd::shared_ptr<trace_api::Tracer> tracer() const {
        return tracer_;
    }

private:
    std::shared_ptr<trace_sdk::TracerProvider> provider_;
    opentelemetry::nostd::shared_ptr<trace_api::Tracer> tracer_;
};

// First service_name passed here wins for the lifetime of the process.
// That matches the usual "one process = one service.name" model.
TracerRuntime& tracer_runtime(const std::string& service_name) {
    static TracerRuntime runtime =
        tracing_runtime_enabled() ? TracerRuntime(service_name) : TracerRuntime();
    return runtime;
}

}  // namespace

class OtelStageSpan::Impl {
public:
    Impl(std::string service_name,
         std::string span_name,
         std::string service_stage,
         std::int64_t start_system_time_ns,
         std::int64_t start_steady_time_ns)
        : service_name_(std::move(service_name)),
          span_name_(std::move(span_name)),
          start_system_time_ns_(start_system_time_ns),
          start_steady_time_ns_(start_steady_time_ns) {
        attributes_["ton.processing.pipeline"] = std::string("trace_to_actions_to_stream");
        attributes_["ton.processing.service_stage"] = std::move(service_stage);
        attributes_["transaction.type"] = std::string("processing");
        if (const auto hostname = hostname_or_empty(); !hostname.empty()) {
            attributes_["service.instance.id"] = hostname;
        }
    }

    void set_attribute(const std::string& key, const std::string& value) {
        if (value.empty()) {
            return;
        }
        attributes_[key] = value;
    }

    void set_attribute(const std::string& key, const char* value) {
        if (!value || value[0] == '\0') {
            return;
        }
        attributes_[key] = std::string(value);
    }

    void set_attribute(const std::string& key, bool value) {
        attributes_[key] = value;
    }

    void set_attribute(const std::string& key, std::int64_t value) {
        attributes_[key] = value;
    }

    void mark_error(const std::string& error_type, const std::string& error_message) {
        if (error_type.empty()) {
            return;
        }

        error_type_ = error_type;
        error_message_ = error_message.empty() ? error_type : error_message;
        attributes_["ton.error.type"] = error_type;
    }

    std::map<std::string, std::string> propagation_fields() {
        ensure_span();
        if (!span_) {
            return {};
        }

        context_api::Context context;
        context = trace_api::SetSpan(context, span_);
        context = baggage_api::SetBaggage(context, build_baggage(attributes_));

        HeaderCarrier carrier;
        trace_api::propagation::HttpTraceContext trace_propagator;
        trace_propagator.Inject(carrier, context);

        baggage_api::propagation::BaggagePropagator baggage_propagator;
        baggage_propagator.Inject(carrier, context);

        return {{kOtelDataField, build_otel_data_json(carrier)}};
    }

    void emit() {
        if (emitted_) {
            return;
        }

        ensure_span();
        if (!span_) {
            return;
        }

        apply_attributes();
        apply_status();
        span_->End();
        emitted_ = true;
    }

private:
    void ensure_pass_id() {
        if (attributes_.find("ton.processing.pass_id") != attributes_.end()) {
            return;
        }
        attributes_["ton.processing.pass_id"] = random_hex(16);
    }

    void apply_attributes() {
        if (!span_) {
            return;
        }

        for (const auto& [key, value] : attributes_) {
            set_span_attribute(span_, key, value);
        }
    }

    void apply_status() {
        if (!span_ || !error_type_) {
            return;
        }

        span_->SetStatus(trace_api::StatusCode::kError, error_message_);
    }

    void ensure_span() {
        if (span_) {
            return;
        }

        ensure_pass_id();

        const auto tracer = tracer_runtime(service_name_).tracer();
        if (!tracer) {
            return;
        }

        trace_api::StartSpanOptions options;
        options.kind = trace_api::SpanKind::kServer;
        options.start_system_time = system_timestamp_from_ns(start_system_time_ns_);
        options.start_steady_time = steady_timestamp_from_ns(start_steady_time_ns_);

        span_ = tracer->StartSpan(span_name_, options);
        apply_attributes();
        apply_status();
    }

    std::string service_name_;
    std::string span_name_;
    std::int64_t start_system_time_ns_;
    std::int64_t start_steady_time_ns_;
    std::map<std::string, OtelStageSpan::AttributeValue> attributes_;
    std::optional<std::string> error_type_;
    std::string error_message_;
    opentelemetry::nostd::shared_ptr<trace_api::Span> span_;
    bool emitted_{false};
};

OtelStageSpan::OtelStageSpan(std::string service_name,
                             std::string span_name,
                             std::string service_stage,
                             std::int64_t start_system_time_ns,
                             std::int64_t start_steady_time_ns)
    : impl_(std::make_unique<Impl>(
          std::move(service_name),
          std::move(span_name),
          std::move(service_stage),
          start_system_time_ns,
          start_steady_time_ns)) {}

OtelStageSpan::~OtelStageSpan() = default;
OtelStageSpan::OtelStageSpan(OtelStageSpan&&) noexcept = default;
OtelStageSpan& OtelStageSpan::operator=(OtelStageSpan&&) noexcept = default;

void OtelStageSpan::set_attribute(const std::string& key, const std::string& value) {
    impl_->set_attribute(key, value);
}

void OtelStageSpan::set_attribute(const std::string& key, const char* value) {
    impl_->set_attribute(key, value);
}

void OtelStageSpan::set_attribute(const std::string& key, bool value) {
    impl_->set_attribute(key, value);
}

void OtelStageSpan::set_attribute(const std::string& key, std::int64_t value) {
    impl_->set_attribute(key, value);
}

void OtelStageSpan::mark_error(const std::string& error_type, const std::string& error_message) {
    impl_->mark_error(error_type, error_message);
}

std::map<std::string, std::string> OtelStageSpan::propagation_fields() {
    if (!tracing_enabled()) {
        return {};
    }
    return impl_->propagation_fields();
}

void OtelStageSpan::emit() {
    if (!tracing_enabled()) {
        return;
    }
    impl_->emit();
}

bool OtelStageSpan::tracing_enabled() {
    return tracing_runtime_enabled();
}
