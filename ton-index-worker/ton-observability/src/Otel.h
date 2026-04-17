#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_metadata.h"

class OtelStageSpan {
public:
    using AttributeValue = std::variant<std::string, bool, std::int64_t>;

    struct Options {
        std::string service_name;
        std::string span_name;
        std::string pipeline;
        std::string service_stage;
        opentelemetry::trace::SpanKind kind{opentelemetry::trace::SpanKind::kInternal};
        std::optional<opentelemetry::trace::SpanContext> parent{std::nullopt};
        std::int64_t start_system_time_ns{0};
        std::int64_t start_steady_time_ns{0};
    };

    explicit OtelStageSpan(Options options);
    OtelStageSpan(std::string service_name,
                  std::string span_name,
                  std::string service_stage,
                  std::int64_t start_system_time_ns,
                  std::int64_t start_steady_time_ns);
    ~OtelStageSpan();

    OtelStageSpan(const OtelStageSpan&) = delete;
    OtelStageSpan& operator=(const OtelStageSpan&) = delete;
    OtelStageSpan(OtelStageSpan&&) noexcept;
    OtelStageSpan& operator=(OtelStageSpan&&) noexcept;

    void set_attribute(const std::string& key, const std::string& value);
    void set_attribute(const std::string& key, const char* value);
    void set_attribute(const std::string& key, bool value);
    void set_attribute(const std::string& key, std::int64_t value);
    void mark_error(const std::string& error_type, const std::string& error_message);
    std::map<std::string, std::string> propagation_fields();
    opentelemetry::trace::SpanContext context() const;
    void emit();
    void end();

    static bool tracing_enabled();
    static std::int64_t system_now_ns();
    static std::int64_t steady_now_ns();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

using OtelSpan = OtelStageSpan;
