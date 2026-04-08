#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <variant>

class OtelStageSpan {
public:
    using AttributeValue = std::variant<std::string, bool, std::int64_t>;

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
    void emit();

    static bool tracing_enabled();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
