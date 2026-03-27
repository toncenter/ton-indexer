#include "Measurement.h"

#include <memory>
#include <chrono>
#include <mutex>
#include <random>
#include <sstream>

#include "td/utils/logging.h"
#include "td/utils/base64.h"
#include "td/utils/JsonBuilder.h"


namespace {

std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
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

std::string b64_hash_or_empty(std::optional<td::Bits256> hash) {
    if (hash.has_value()) {
        return td::base64_encode(hash.value().as_slice());
    }
    return {};
}

} // namespace

Measurement::Measurement()
    : measurement_id_(get_new_id()),
      start_time_ns_(now_ns()),
      pass_id_(random_hex(16)) {
}

Measurement::Measurement(const Measurement &other) {
    std::lock_guard<std::mutex> lock(other.mutex_);
    measurement_id_ = other.measurement_id_;
    start_time_ns_ = other.start_time_ns_;
    pass_id_ = other.pass_id_;
    ext_msg_hash_ = other.ext_msg_hash_;
    ext_msg_hash_norm_ = other.ext_msg_hash_norm_;
    trace_root_tx_hash_ = other.trace_root_tx_hash_;
    finality_ = other.finality_;
    operation_ = other.operation_;
    out_channel_ = other.out_channel_;
    error_type_ = other.error_type_;
    error_message_ = other.error_message_;
    timings_ = other.timings_;
    extra_ = other.extra_;
    otel_stage_.reset();
}

Measurement &Measurement::operator=(const Measurement &other) {
    if (this == &other) {
        return *this;
    }
    std::lock(mutex_, other.mutex_);
    std::lock_guard<std::mutex> lock_self(mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> lock_other(other.mutex_, std::adopt_lock);
    measurement_id_ = other.measurement_id_;
    start_time_ns_ = other.start_time_ns_;
    pass_id_ = other.pass_id_;
    ext_msg_hash_ = other.ext_msg_hash_;
    ext_msg_hash_norm_ = other.ext_msg_hash_norm_;
    trace_root_tx_hash_ = other.trace_root_tx_hash_;
    finality_ = other.finality_;
    operation_ = other.operation_;
    out_channel_ = other.out_channel_;
    error_type_ = other.error_type_;
    error_message_ = other.error_message_;
    timings_ = other.timings_;
    extra_ = other.extra_;
    otel_stage_.reset();
    return *this;
}

std::shared_ptr<Measurement> Measurement::clone() const {
    std::unique_ptr<Measurement> clone(
        new Measurement(*this)
    );
    clone->measurement_id_ = get_new_id();
    clone->start_time_ns_ = now_ns();
    clone->pass_id_ = random_hex(16);
    clone->error_type_.reset();
    clone->error_message_.reset();
    clone->otel_stage_.reset();
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
    if (otel_stage_) {
        otel_stage_->set_attribute("ton.trace.root_tx_hash", td::base64_encode(trace_root_tx_hash.as_slice()));
    }
    return *this;
}

Measurement & Measurement::set_ext_msg_hash(const td::Bits256 &ext_msg_hash) {
    std::lock_guard<std::mutex> lock(mutex_);
    ext_msg_hash_ = ext_msg_hash;
    if (otel_stage_) {
        otel_stage_->set_attribute("ton.trace.external_message_hash", td::base64_encode(ext_msg_hash.as_slice()));
    }
    return *this;
}

Measurement &Measurement::set_ext_msg_hash_norm(const td::Bits256 &ext_msg_hash_norm) {
    std::lock_guard<std::mutex> lock(mutex_);
    ext_msg_hash_norm_ = ext_msg_hash_norm;
    return *this;
}

Measurement &Measurement::set_finality(const std::string& finality) {
    std::lock_guard<std::mutex> lock(mutex_);
    finality_ = finality;
    if (otel_stage_) {
        otel_stage_->set_attribute("ton.trace.finality", finality);
    }
    return *this;
}

Measurement &Measurement::set_operation(const std::string& operation) {
    std::lock_guard<std::mutex> lock(mutex_);
    operation_ = operation;
    if (otel_stage_) {
        otel_stage_->set_attribute("ton.trace_emulator.operation", operation);
    }
    return *this;
}

Measurement &Measurement::set_out_channel(const std::string& out_channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    out_channel_ = out_channel;
    if (otel_stage_) {
        otel_stage_->set_attribute("ton.redis.out.channel", out_channel);
    }
    return *this;
}

Measurement &Measurement::mark_otel_error(const std::string& error_type, const std::string& error_message) {
    std::lock_guard<std::mutex> lock(mutex_);
    error_type_ = error_type;
    error_message_ = error_message;
    if (otel_stage_) {
        otel_stage_->mark_error(error_type, error_message);
    }
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

void Measurement::ensure_trace_emulator_stage_locked() {
    if (!otel_stage_) {
        otel_stage_ = std::make_unique<OtelStageSpan>(
            "ton-trace-emulator",
            "ton.trace_emulator.process",
            "trace_emulator",
            start_time_ns_
        );
    }
    otel_stage_->set_attribute("ton.processing.pass_id", pass_id_);
    if (!b64_hash_or_empty(ext_msg_hash_).empty()) {
        otel_stage_->set_attribute("ton.trace.external_message_hash", b64_hash_or_empty(ext_msg_hash_));
    }
    if (!b64_hash_or_empty(trace_root_tx_hash_).empty()) {
        otel_stage_->set_attribute("ton.trace.root_tx_hash", b64_hash_or_empty(trace_root_tx_hash_));
    }
    if (finality_) {
        otel_stage_->set_attribute("ton.trace.finality", *finality_);
    }
    if (operation_) {
        otel_stage_->set_attribute("ton.trace_emulator.operation", *operation_);
    }
    if (out_channel_) {
        otel_stage_->set_attribute("ton.redis.out.channel", *out_channel_);
    }
    if (error_type_) {
        otel_stage_->mark_error(*error_type_, error_message_.value_or(*error_type_));
    }
}

std::map<std::string, std::string> Measurement::otel_propagation_fields() {
    std::lock_guard<std::mutex> lock(mutex_);
    ensure_trace_emulator_stage_locked();
    return otel_stage_->propagation_fields();
}

Measurement &Measurement::print_measurement() {
    std::lock_guard<std::mutex> lock(mutex_);
    ensure_trace_emulator_stage_locked();
    if (otel_stage_) {
        otel_stage_->emit();
    }
    if (OtelStageSpan::measure_logs_enabled()) {
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
