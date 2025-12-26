#pragma once
#include <atomic>
#include <map>
#include <mutex>
#include <optional>

#include "common/bitstring.h"
#include "ton/ton-types.h"

class Measurement;

class Measurement {
    mutable std::mutex mutex_;
    std::int64_t measurement_id_;
    std::optional<td::Bits256> ext_msg_hash_{std::nullopt};
    std::optional<td::Bits256> ext_msg_hash_norm_{std::nullopt};
    std::optional<td::Bits256> trace_root_tx_hash_{std::nullopt};

    std::map<std::string, double> timings_{};
    std::map<std::string, std::string> extra_{};
public:
    Measurement() : measurement_id_(get_new_id()) {}

    Measurement(const Measurement &other);
    Measurement &operator=(const Measurement &other);

    std::shared_ptr<Measurement> clone() const;

    ~Measurement() = default;

    Measurement &remove_id();
    std::int64_t id() const;

    Measurement &set_trace_root_tx_hash(const td::Bits256 &trace_root_tx_hash);

    Measurement &set_ext_msg_hash(const td::Bits256 &ext_msg_hash);

    Measurement &set_ext_msg_hash_norm(const td::Bits256 &ext_msg_hash_norm);

    Measurement &measure_step(const std::string &step_name, std::optional<double> time = std::nullopt);

    Measurement &print_measurement();

    static std::int64_t get_new_id();
};

using MeasurementPtr = std::shared_ptr<Measurement>;
