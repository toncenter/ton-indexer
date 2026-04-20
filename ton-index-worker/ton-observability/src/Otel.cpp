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
#include <unistd.h>
#include <utility>

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

namespace baggage_api = opentelemetry::baggage;
namespace context_api = opentelemetry::context;
namespace otlp = opentelemetry::exporter::otlp;
namespace resource_api = opentelemetry::sdk::resource;
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;

namespace {

constexpr const char* kScopeName = "ton-indexer.observability";
constexpr const char* kScopeVersion = "1.0.0";
constexpr const char* kDefaultPipeline = "trace_to_actions_to_stream";
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
    std::uniform_int_distribution<unsigned int> distribution(0, 255);

    std::ostringstream oss;
    oss << std::hex;
    for (std::size_t i = 0; i < bytes_count; ++i) {
        const auto value = distribution(rng);
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

std::string json_escape(std::string_view input) {
    std::string output;
    output.reserve(input.size() + 8);

    for (const unsigned char ch : input) {
        switch (ch) {
            case '\\':
                output += "\\\\";
                break;
            case '"':
                output += "\\\"";
                break;
            case '\b':
                output += "\\b";
                break;
            case '\f':
                output += "\\f";
                break;
            case '\n':
                output += "\\n";
                break;
            case '\r':
                output += "\\r";
                break;
            case '\t':
                output += "\\t";
                break;
            default:
                if (ch < 0x20) {
                    std::ostringstream oss;
                    oss << "\\u" << std::hex << std::uppercase
                        << static_cast<int>((ch >> 12) & 0xF)
                        << static_cast<int>((ch >> 8) & 0xF)
                        << static_cast<int>((ch >> 4) & 0xF)
                        << static_cast<int>(ch & 0xF);
                    output += oss.str();
                } else {
                    output.push_back(static_cast<char>(ch));
                }
                break;
        }
    }

    return output;
}

std::string build_otel_data_json(const HeaderCarrier& carrier) {
    return std::string("{\"traceparent\":\"") + json_escape(carrier.value_or("traceparent")) +
           "\",\"tracestate\":\"" + json_escape(carrier.value_or("tracestate")) +
           "\",\"baggage\":\"" + json_escape(carrier.value_or("baggage")) + "\"}";
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

void set_span_attribute(
    const opentelemetry::nostd::shared_ptr<trace_api::Span>& span,
    const std::string& key,
    const OtelStageSpan::AttributeValue& value) {
    if (!span) {
        return;
    }

    if (std::holds_alternative<std::string>(value)) {
        const auto& string_value = std::get<std::string>(value);
        if (!string_value.empty()) {
            span->SetAttribute(key, string_value);
        }
        return;
    }

    if (std::holds_alternative<bool>(value)) {
        span->SetAttribute(key, std::get<bool>(value));
        return;
    }

    span->SetAttribute(key, std::get<std::int64_t>(value));
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
    explicit Impl(Options options) {
        if (!tracing_runtime_enabled()) {
            return;
        }

        if (options.parent.has_value() && !options.parent->IsValid()) {
            return;
        }

        attributes_["ton.processing.pipeline"] =
            options.pipeline.empty() ? std::string(kDefaultPipeline) : std::move(options.pipeline);
        attributes_["ton.processing.service_stage"] = std::move(options.service_stage);
        attributes_["transaction.type"] = std::string("processing");
        if (const auto hostname = hostname_or_empty(); !hostname.empty()) {
            attributes_["service.instance.id"] = hostname;
        }

        const auto tracer = tracer_runtime(options.service_name).tracer();
        if (!tracer) {
            return;
        }

        const auto start_system_time_ns =
            options.start_system_time_ns > 0 ? options.start_system_time_ns : OtelStageSpan::system_now_ns();
        const auto start_steady_time_ns =
            options.start_steady_time_ns > 0 ? options.start_steady_time_ns : OtelStageSpan::steady_now_ns();

        trace_api::StartSpanOptions start_options;
        start_options.kind = options.kind;
        start_options.start_system_time = system_timestamp_from_ns(start_system_time_ns);
        start_options.start_steady_time = steady_timestamp_from_ns(start_steady_time_ns);
        if (options.parent.has_value()) {
            start_options.parent = *options.parent;
        } else {
            context_api::Context root_context;
            root_context = root_context.SetValue(trace_api::kIsRootSpanKey, true);
            start_options.parent = root_context;
        }

        span_ = tracer->StartSpan(options.span_name, start_options);
        apply_attributes();
    }

    void set_attribute(const std::string& key, const AttributeValue& value) {
        if (std::holds_alternative<std::string>(value) &&
            std::get<std::string>(value).empty()) {
            return;
        }

        attributes_[key] = value;
        if (span_ && !ended_) {
            set_span_attribute(span_, key, value);
        }
    }

    void mark_error(const std::string& error_type, const std::string& error_message) {
        if (error_type.empty()) {
            return;
        }

        error_type_ = error_type;
        error_message_ = error_message.empty() ? error_type : error_message;
        attributes_["ton.error.type"] = error_type;

        if (!span_ || ended_) {
            return;
        }

        span_->SetAttribute("ton.error.type", error_type);
        span_->SetStatus(trace_api::StatusCode::kError, error_message_);
    }

    std::map<std::string, std::string> propagation_fields() {
        ensure_pass_id();
        if (!span_ || ended_) {
            return {};
        }

        apply_attributes();
        apply_status();

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

    trace_api::SpanContext context() const {
        if (!span_) {
            return trace_api::SpanContext::GetInvalid();
        }
        return span_->GetContext();
    }

    void end() {
        if (!span_ || ended_) {
            return;
        }

        apply_attributes();
        apply_status();

        trace_api::EndSpanOptions end_options;
        end_options.end_steady_time = steady_timestamp_from_ns(OtelStageSpan::steady_now_ns());
        span_->End(end_options);
        ended_ = true;
    }

private:
    void ensure_pass_id() {
        if (attributes_.find("ton.processing.pass_id") != attributes_.end()) {
            return;
        }
        set_attribute("ton.processing.pass_id", random_hex(16));
    }

    void apply_attributes() {
        if (!span_ || ended_) {
            return;
        }

        for (const auto& [key, value] : attributes_) {
            set_span_attribute(span_, key, value);
        }
    }

    void apply_status() {
        if (!span_ || ended_ || !error_type_) {
            return;
        }

        span_->SetStatus(trace_api::StatusCode::kError, error_message_);
    }

    std::map<std::string, AttributeValue> attributes_;
    std::optional<std::string> error_type_;
    std::string error_message_;
    opentelemetry::nostd::shared_ptr<trace_api::Span> span_;
    bool ended_{false};
};

OtelStageSpan::OtelStageSpan(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

OtelStageSpan::OtelStageSpan(std::string service_name,
                             std::string span_name,
                             std::string service_stage,
                             std::int64_t start_system_time_ns,
                             std::int64_t start_steady_time_ns) {
    Options options;
    options.service_name = std::move(service_name);
    options.span_name = std::move(span_name);
    options.pipeline = kDefaultPipeline;
    options.service_stage = std::move(service_stage);
    options.kind = trace_api::SpanKind::kServer;
    options.start_system_time_ns = start_system_time_ns;
    options.start_steady_time_ns = start_steady_time_ns;
    impl_ = std::make_unique<Impl>(std::move(options));
}

OtelStageSpan::~OtelStageSpan() = default;
OtelStageSpan::OtelStageSpan(OtelStageSpan&&) noexcept = default;
OtelStageSpan& OtelStageSpan::operator=(OtelStageSpan&&) noexcept = default;

void OtelStageSpan::set_attribute(const std::string& key, const std::string& value) {
    impl_->set_attribute(key, value);
}

void OtelStageSpan::set_attribute(const std::string& key, const char* value) {
    if (!value || value[0] == '\0') {
        return;
    }
    impl_->set_attribute(key, std::string(value));
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

trace_api::SpanContext OtelStageSpan::context() const {
    return impl_->context();
}

void OtelStageSpan::emit() {
    end();
}

void OtelStageSpan::end() {
    if (!tracing_enabled()) {
        return;
    }
    impl_->end();
}

bool OtelStageSpan::tracing_enabled() {
    return tracing_runtime_enabled();
}

std::int64_t OtelStageSpan::system_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::int64_t OtelStageSpan::steady_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
