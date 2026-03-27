#pragma once
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "common/bitstring.h"
#include "ton/ton-types.h"
#include "Otel.h"

class Measurement;

class Measurement {
    mutable std::mutex mutex_;
    std::int64_t measurement_id_;
    std::int64_t start_time_ns_;
    std::string pass_id_;
    std::optional<td::Bits256> ext_msg_hash_{std::nullopt};
    std::optional<td::Bits256> ext_msg_hash_norm_{std::nullopt};
    std::optional<td::Bits256> trace_root_tx_hash_{std::nullopt};
    std::optional<std::string> finality_{std::nullopt};
    std::optional<std::string> operation_{std::nullopt};
    std::optional<std::string> out_channel_{std::nullopt};
    std::optional<std::string> error_type_{std::nullopt};
    std::optional<std::string> error_message_{std::nullopt};

    std::map<std::string, double> timings_{};
    std::map<std::string, std::string> extra_{};
    std::unique_ptr<OtelStageSpan> otel_stage_;

    void ensure_trace_emulator_stage_locked();
public:
    Measurement();

    Measurement(const Measurement &other);
    Measurement &operator=(const Measurement &other);

    std::shared_ptr<Measurement> clone() const;

    ~Measurement() = default;

    Measurement &remove_id();
    std::int64_t id() const;

    Measurement &set_trace_root_tx_hash(const td::Bits256 &trace_root_tx_hash);

    Measurement &set_ext_msg_hash(const td::Bits256 &ext_msg_hash);

    Measurement &set_ext_msg_hash_norm(const td::Bits256 &ext_msg_hash_norm);

    Measurement &set_finality(const std::string& finality);

    Measurement &set_operation(const std::string& operation);

    Measurement &set_out_channel(const std::string& out_channel);

    Measurement &mark_otel_error(const std::string& error_type, const std::string& error_message);

    Measurement &measure_step(const std::string &step_name, std::optional<double> time = std::nullopt);

    std::map<std::string, std::string> otel_propagation_fields();

    Measurement &print_measurement();

    static std::int64_t get_new_id();
};

using MeasurementPtr = std::shared_ptr<Measurement>;
