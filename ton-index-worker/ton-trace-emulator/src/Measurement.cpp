#include "Measurement.h"

#include <memory>
#include <chrono>
#include <mutex>
#include <random>

#include "td/utils/logging.h"
#include "td/utils/base64.h"
#include "td/utils/JsonBuilder.h"


Measurement::Measurement(const Measurement &other) {
    std::lock_guard<std::mutex> lock(other.mutex_);
    measurement_id_ = other.measurement_id_;
    ext_msg_hash_ = other.ext_msg_hash_;
    ext_msg_hash_norm_ = other.ext_msg_hash_norm_;
    trace_root_tx_hash_ = other.trace_root_tx_hash_;
    timings_ = other.timings_;
}

Measurement &Measurement::operator=(const Measurement &other) {
    if (this == &other) {
        return *this;
    }
    std::lock(mutex_, other.mutex_);
    std::lock_guard<std::mutex> lock_self(mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> lock_other(other.mutex_, std::adopt_lock);
    measurement_id_ = other.measurement_id_;
    ext_msg_hash_ = other.ext_msg_hash_;
    ext_msg_hash_norm_ = other.ext_msg_hash_norm_;
    trace_root_tx_hash_ = other.trace_root_tx_hash_;
    timings_ = other.timings_;
    return *this;
}

std::shared_ptr<Measurement> Measurement::clone() const {
    std::unique_ptr<Measurement> clone(
        new Measurement(*this)
    );
    clone->measurement_id_ = get_new_id();
    return std::shared_ptr(std::move(clone));
}

Measurement &Measurement::remove_id() {
    std::lock_guard<std::mutex> lock(mutex_);
    measurement_id_ = -1;
    return *this;
}

std::int64_t Measurement::id() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return measurement_id_;
}

Measurement & Measurement::set_trace_root_tx_hash(const td::Bits256 &trace_root_tx_hash) {
    std::lock_guard<std::mutex> lock(mutex_);
    trace_root_tx_hash_ = trace_root_tx_hash;
    return *this;
}

Measurement & Measurement::set_ext_msg_hash(const td::Bits256 &ext_msg_hash) {
    std::lock_guard<std::mutex> lock(mutex_);
    ext_msg_hash_ = ext_msg_hash;
    return *this;
}

Measurement &Measurement::set_ext_msg_hash_norm(const td::Bits256 &ext_msg_hash_norm) {
    std::lock_guard<std::mutex> lock(mutex_);
    ext_msg_hash_norm_ = ext_msg_hash_norm;
    return *this;
}

Measurement &Measurement::measure_step(const std::string &step_name, std::optional<double> time) {
    const double ts = time.value_or(
        std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count());
    std::lock_guard<std::mutex> lock(mutex_);
    timings_[step_name] = ts;
    return *this;
}

std::string b64_hash_or_default(std::optional<td::Bits256> hash) {
    if (hash.has_value()) {
        return td::base64_encode(hash.value().as_slice());
    }
    return "-";
}
std::string int32_or_default(std::optional<std::int32_t> value) {
    if (value.has_value()) {
        return std::to_string(value.value());
    }
    return "-";
}
std::string block_id_or_default(std::optional<ton::BlockIdExt> block_id) {
    if (block_id.has_value()) {
        auto& blk_id = block_id.value();
        char s[33];
        snprintf(s, sizeof(s), "%llx", static_cast<long long>(blk_id.shard_full().shard));
        return std::to_string(blk_id.shard_full().workchain) + "," + s + "," + std::to_string(blk_id.seqno());
    }
    return "-,-,-";
}

Measurement &Measurement::print_measurement() {
    std::lock_guard<std::mutex> lock(mutex_);
    td::JsonBuilder extra_jb;
    auto extra_json = extra_jb.enter_object();
    for (auto &[k, v] : extra_) {
        extra_json(k, v);
    }
    extra_json.leave();
    auto extra_raw = extra_jb.string_builder().as_cslice();
    for (auto &[step_name, ts]: timings_) {
        td::JsonBuilder jb;
        auto obj = jb.enter_object();
        obj("id", measurement_id_);
        obj("msg_hash_norm", b64_hash_or_default(ext_msg_hash_norm_));
        obj("msg_hash", b64_hash_or_default(ext_msg_hash_));
        obj("trace_id", b64_hash_or_default(trace_root_tx_hash_));
        obj("step", step_name);
        obj("time", ts);
        obj("extra", td::JsonRaw{extra_raw});
        obj.leave();
        LOG(ERROR) << "MEASURE " << jb.string_builder().as_cslice() << " END";
    }
    return *this;
}

std::int64_t Measurement::get_new_id() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::int64_t> distribution(0, 1000);
    std::int64_t random_num = distribution(gen);
    return std::chrono::nanoseconds(std::chrono::system_clock::now().time_since_epoch()).count() + random_num;
}
