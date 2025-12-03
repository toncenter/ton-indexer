#pragma once
#include <map>
#include <optional>

#include "common/bitstring.h"
#include "ton/ton-types.h"

class Measurement;

class Measurement {
    static std::atomic<std::int64_t> next_id;

    std::int64_t measurement_id;
    std::optional<td::Bits256> ext_msg_hash_{std::nullopt};
    std::optional<td::Bits256> ext_msg_hash_norm_{std::nullopt};
    std::optional<td::Bits256> trace_root_tx_hash_{std::nullopt};

    std::map<std::string, double> timings_{};

public:
    Measurement() : measurement_id(next_id.fetch_add(1)) {}

    Measurement(const Measurement &) = default;

    std::shared_ptr<Measurement> clone() const;

    ~Measurement() = default;

    Measurement &remove_id();
    std::int64_t id() const;

    Measurement &set_trace_root_tx_hash(const td::Bits256 &trace_root_tx_hash);

    Measurement &set_ext_msg_hash(const td::Bits256 &ext_msg_hash);

    Measurement &set_ext_msg_hash_norm(const td::Bits256 &ext_msg_hash_norm);

    Measurement &measure_step(const std::string &step_name, std::optional<double> time = std::nullopt);

    Measurement &print_measurement();
};

using MeasurementPtr = std::shared_ptr<Measurement>;
