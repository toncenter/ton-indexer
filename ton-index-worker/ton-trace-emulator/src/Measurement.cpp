#include "Measurement.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>

#include "td/utils/base64.h"

namespace {

std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::int64_t steady_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
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
    : start_system_time_ns_(now_ns()),
      start_steady_time_ns_(steady_now_ns()),
      pass_id_(random_hex(16)) {
}

Measurement::Measurement(const Measurement &other) {
    std::lock_guard<std::mutex> lock(other.mutex_);
    start_system_time_ns_ = other.start_system_time_ns_;
    start_steady_time_ns_ = other.start_steady_time_ns_;
    pass_id_ = other.pass_id_;
    ext_msg_hash_ = other.ext_msg_hash_;
    ext_msg_hash_norm_ = other.ext_msg_hash_norm_;
    trace_root_tx_hash_ = other.trace_root_tx_hash_;
    finality_ = other.finality_;
    operation_ = other.operation_;
    source_ = other.source_;
    transactions_count_ = other.transactions_count_;
    emulated_transactions_count_ = other.emulated_transactions_count_;
    out_channel_ = other.out_channel_;
    error_type_ = other.error_type_;
    error_message_ = other.error_message_;
    custom_attributes_ = other.custom_attributes_;
    otel_stage_.reset();
    otel_child_spans_.clear();
}

Measurement &Measurement::operator=(const Measurement &other) {
    if (this == &other) {
        return *this;
    }
    std::lock(mutex_, other.mutex_);
    std::lock_guard<std::mutex> lock_self(mutex_, std::adopt_lock);
    std::lock_guard<std::mutex> lock_other(other.mutex_, std::adopt_lock);
    start_system_time_ns_ = other.start_system_time_ns_;
    start_steady_time_ns_ = other.start_steady_time_ns_;
    pass_id_ = other.pass_id_;
    ext_msg_hash_ = other.ext_msg_hash_;
    ext_msg_hash_norm_ = other.ext_msg_hash_norm_;
    trace_root_tx_hash_ = other.trace_root_tx_hash_;
    finality_ = other.finality_;
    operation_ = other.operation_;
    source_ = other.source_;
    transactions_count_ = other.transactions_count_;
    emulated_transactions_count_ = other.emulated_transactions_count_;
    out_channel_ = other.out_channel_;
    error_type_ = other.error_type_;
    error_message_ = other.error_message_;
    custom_attributes_ = other.custom_attributes_;
    otel_stage_.reset();
    otel_child_spans_.clear();
    return *this;
}

std::shared_ptr<Measurement> Measurement::clone() const {
    std::unique_ptr<Measurement> clone(
        new Measurement(*this)
    );
    clone->start_system_time_ns_ = now_ns();
    clone->start_steady_time_ns_ = steady_now_ns();
    clone->pass_id_ = random_hex(16);
    clone->error_type_.reset();
    clone->error_message_.reset();
    clone->otel_stage_.reset();
    clone->otel_child_spans_.clear();
    return std::shared_ptr(std::move(clone));
}

Measurement & Measurement::set_trace_root_tx_hash(const td::Bits256 &trace_root_tx_hash) {
    std::lock_guard<std::mutex> lock(mutex_);
    trace_root_tx_hash_ = trace_root_tx_hash;
    set_attribute_locked("ton.trace.root_tx_hash", td::base64_encode(trace_root_tx_hash.as_slice()));
    return *this;
}

Measurement & Measurement::set_ext_msg_hash(const td::Bits256 &ext_msg_hash) {
    std::lock_guard<std::mutex> lock(mutex_);
    ext_msg_hash_ = ext_msg_hash;
    set_attribute_locked("ton.trace.external_message_hash", td::base64_encode(ext_msg_hash.as_slice()));
    return *this;
}

Measurement &Measurement::set_ext_msg_hash_norm(const td::Bits256 &ext_msg_hash_norm) {
    std::lock_guard<std::mutex> lock(mutex_);
    ext_msg_hash_norm_ = ext_msg_hash_norm;
    set_attribute_locked("ton.trace.external_message_hash_norm", td::base64_encode(ext_msg_hash_norm.as_slice()));
    return *this;
}

Measurement &Measurement::set_finality(const std::string& finality) {
    std::lock_guard<std::mutex> lock(mutex_);
    finality_ = finality;
    set_attribute_locked("ton.trace.finality", finality);
    return *this;
}

Measurement &Measurement::set_operation(const std::string& operation) {
    std::lock_guard<std::mutex> lock(mutex_);
    operation_ = operation;
    set_attribute_locked("ton.trace_emulator.operation", operation);
    return *this;
}

Measurement &Measurement::set_source(const std::string& source) {
    std::lock_guard<std::mutex> lock(mutex_);
    source_ = source;
    set_attribute_locked("ton.trace_emulator.source", source);
    return *this;
}

Measurement &Measurement::set_transactions_count(std::int64_t transactions_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    transactions_count_ = transactions_count;
    set_attribute_locked("ton.trace.transactions_count", transactions_count);
    return *this;
}

Measurement &Measurement::set_emulated_transactions_count(std::int64_t emulated_transactions_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    emulated_transactions_count_ = emulated_transactions_count;
    set_attribute_locked("ton.trace.emulated_transactions_count", emulated_transactions_count);
    return *this;
}

Measurement &Measurement::set_out_channel(const std::string& out_channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    out_channel_ = out_channel;
    set_attribute_locked("ton.redis.out.channel", out_channel);
    return *this;
}

Measurement &Measurement::mark_otel_error(const std::string& error_type, const std::string& error_message) {
    std::lock_guard<std::mutex> lock(mutex_);
    error_type_ = error_type;
    error_message_ = error_message;
    if (otel_stage_) {
        otel_stage_->mark_error(error_type, error_message);
    }
    for (auto& [_, child_span] : otel_child_spans_) {
        child_span->mark_error(error_type, error_message);
    }
    return *this;
}

Measurement &Measurement::set_otel_attribute(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    custom_attributes_[key] = value;
    set_attribute_locked(key, value);
    return *this;
}

Measurement &Measurement::set_otel_attribute(const std::string& key, const char* value) {
    if (!value || value[0] == '\0') {
        return *this;
    }
    return set_otel_attribute(key, std::string(value));
}

Measurement &Measurement::set_otel_attribute(const std::string& key, bool value) {
    std::lock_guard<std::mutex> lock(mutex_);
    custom_attributes_[key] = value;
    set_attribute_locked(key, value);
    return *this;
}

Measurement &Measurement::set_otel_attribute(const std::string& key, std::int64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    custom_attributes_[key] = value;
    set_attribute_locked(key, value);
    return *this;
}

Measurement &Measurement::start_otel_child_span(const std::string& service_stage) {
    if (!OtelStageSpan::tracing_enabled() || service_stage.empty()) {
        return *this;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    ensure_trace_emulator_stage_locked();

    auto existing = otel_child_spans_.find(service_stage);
    if (existing != otel_child_spans_.end()) {
        existing->second->end();
        otel_child_spans_.erase(existing);
    }

    OtelStageSpan::Options options;
    options.service_name = "ton-trace-emulator";
    options.span_name = std::string("ton.trace_emulator.") + service_stage;
    options.pipeline = "trace_to_actions_to_stream";
    options.service_stage = service_stage;
    options.kind = opentelemetry::trace::SpanKind::kInternal;
    options.parent = otel_stage_
        ? std::optional<opentelemetry::trace::SpanContext>(otel_stage_->context())
        : std::nullopt;
    options.start_system_time_ns = OtelStageSpan::system_now_ns();
    options.start_steady_time_ns = OtelStageSpan::steady_now_ns();

    auto child_span = std::make_unique<OtelStageSpan>(std::move(options));
    apply_common_attributes_locked(*child_span);
    if (error_type_) {
        child_span->mark_error(*error_type_, error_message_.value_or(*error_type_));
    }
    otel_child_spans_[service_stage] = std::move(child_span);
    return *this;
}

Measurement &Measurement::end_otel_child_span(const std::string& service_stage) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = otel_child_spans_.find(service_stage);
    if (it == otel_child_spans_.end()) {
        return *this;
    }
    apply_common_attributes_locked(*it->second);
    it->second->end();
    otel_child_spans_.erase(it);
    return *this;
}

void Measurement::ensure_trace_emulator_stage_locked() {
    if (!otel_stage_) {
        otel_stage_ = std::make_unique<OtelStageSpan>(
            "ton-trace-emulator",
            "ton.trace_emulator.process",
            "trace_emulator",
            start_system_time_ns_,
            start_steady_time_ns_
        );
    }
    apply_common_attributes_locked(*otel_stage_);
    if (error_type_) {
        otel_stage_->mark_error(*error_type_, error_message_.value_or(*error_type_));
    }
}

void Measurement::apply_common_attributes_locked(OtelStageSpan& span) {
    span.set_attribute("ton.processing.pass_id", pass_id_);
    if (!b64_hash_or_empty(ext_msg_hash_).empty()) {
        span.set_attribute("ton.trace.external_message_hash", b64_hash_or_empty(ext_msg_hash_));
    }
    if (!b64_hash_or_empty(ext_msg_hash_norm_).empty()) {
        span.set_attribute("ton.trace.external_message_hash_norm", b64_hash_or_empty(ext_msg_hash_norm_));
    }
    if (!b64_hash_or_empty(trace_root_tx_hash_).empty()) {
        span.set_attribute("ton.trace.root_tx_hash", b64_hash_or_empty(trace_root_tx_hash_));
    }
    if (finality_) {
        span.set_attribute("ton.trace.finality", *finality_);
    }
    if (operation_) {
        span.set_attribute("ton.trace_emulator.operation", *operation_);
    }
    if (source_) {
        span.set_attribute("ton.trace_emulator.source", *source_);
    }
    if (transactions_count_) {
        span.set_attribute("ton.trace.transactions_count", *transactions_count_);
    }
    if (emulated_transactions_count_) {
        span.set_attribute("ton.trace.emulated_transactions_count", *emulated_transactions_count_);
    }
    if (out_channel_) {
        span.set_attribute("ton.redis.out.channel", *out_channel_);
    }
    for (const auto& [key, value] : custom_attributes_) {
        std::visit([&](const auto& typed_value) {
            span.set_attribute(key, typed_value);
        }, value);
    }
}

void Measurement::set_attribute_locked(const std::string& key, const OtelStageSpan::AttributeValue& value) {
    if (otel_stage_) {
        std::visit([&](const auto& typed_value) {
            otel_stage_->set_attribute(key, typed_value);
        }, value);
    }
    for (auto& [_, child_span] : otel_child_spans_) {
        std::visit([&](const auto& typed_value) {
            child_span->set_attribute(key, typed_value);
        }, value);
    }
}

std::map<std::string, std::string> Measurement::otel_propagation_fields() {
    std::lock_guard<std::mutex> lock(mutex_);
    ensure_trace_emulator_stage_locked();
    return otel_stage_->propagation_fields();
}

Measurement &Measurement::emit_otel_span() {
    std::lock_guard<std::mutex> lock(mutex_);
    ensure_trace_emulator_stage_locked();
    if (otel_stage_) {
        for (auto& [_, child_span] : otel_child_spans_) {
            child_span->end();
        }
        otel_child_spans_.clear();
        otel_stage_->emit();
    }
    return *this;
}
