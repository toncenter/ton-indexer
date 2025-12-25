#include "Measurement.h"

#include <memory>
#include <chrono>
#include <mutex>

#include "td/utils/logging.h"
#include "td/utils/base64.h"


std::atomic<std::int64_t> Measurement::next_id{0};

Measurement::Measurement(const Measurement &other) {
    std::lock_guard<std::mutex> lock(other.mutex_);
    measurement_id = other.measurement_id;
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
    measurement_id = other.measurement_id;
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
    clone->measurement_id = next_id.fetch_add(1);
    return std::shared_ptr(std::move(clone));
}

Measurement &Measurement::remove_id() {
    std::lock_guard<std::mutex> lock(mutex_);
    measurement_id = -1;
    return *this;
}

std::int64_t Measurement::id() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return measurement_id;
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
    std::int64_t measurement_id_local;
    std::optional<td::Bits256> ext_msg_hash_local;
    std::optional<td::Bits256> ext_msg_hash_norm_local;
    std::optional<td::Bits256> trace_root_tx_hash_local;
    std::map<std::string, double> timings_local;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        measurement_id_local = measurement_id;
        ext_msg_hash_local = ext_msg_hash_;
        ext_msg_hash_norm_local = ext_msg_hash_norm_;
        trace_root_tx_hash_local = trace_root_tx_hash_;
        timings_local = timings_;
    }
    td::StringBuilder sb;
    sb << "MEASURE[";
    sb << "id=" << measurement_id_local << ";";
    sb << "msg_hash_norm=" << b64_hash_or_default(ext_msg_hash_norm_local) << ";";
    sb << "msg_hash=" << b64_hash_or_default(ext_msg_hash_local) << ";";
    sb << "trace_id=" << b64_hash_or_default(trace_root_tx_hash_local) << "]";
    std::string common = sb.as_cslice().str();

    for (auto &[step_name, ts]: timings_local) {
        td::StringBuilder sb2;
        sb2 << "(step=" << step_name << ";";
        sb2 << "time=" << ts << ")";
        LOG(ERROR) << common << sb2.as_cslice();
    }
    return *this;
}
