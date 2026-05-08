#include "RedisListener.h"

#include "Measurement.h"
#include "Statistics.h"

#include <cstdint>

void ChannelListener::setup_subscriber() {
  sw::redis::ConnectionOptions connection_options = sw::redis::Uri(redis_dsn_).connection_options();
  connection_options.socket_timeout = std::chrono::milliseconds(100);
  auto redis = sw::redis::Redis(connection_options);
  subscriber_ = redis.subscriber();
  subscriber_->subscribe(channel_name_);
  subscriber_->on_message([this](const std::string &channel, const std::string &value) {
    auto boc_decoded = td::base64_decode(td::Slice(value));
    if (boc_decoded.is_error()) {
      LOG(ERROR) << "Can't decode base64 boc: " << boc_decoded.move_as_error();
      return;
    }
    auto msg_cell_r = vm::std_boc_deserialize(boc_decoded.move_as_ok());
    if (msg_cell_r.is_error()) {
      LOG(ERROR) << "Can't deserialize message boc: " << msg_cell_r.move_as_error();
      return;
    }
    auto msg_cell = msg_cell_r.move_as_ok();

    on_new_message_(msg_cell);
  });
}

void ChannelListener::start_up() {
  setup_subscriber();
  alarm_timestamp() = td::Timestamp::now();
}

void ChannelListener::alarm() {
  while (true) {
    try {
      subscriber_->consume();
    } catch (const sw::redis::TimeoutError &e) {
      break;
    } catch (const sw::redis::ReplyError &e) {
      LOG(ERROR) << "Redis error: " << e.what();
      break;
    } catch (const std::exception &e) {
      LOG(ERROR) << "Redis error: " << e.what();
      LOG(ERROR) << "Reconnecting to Redis...";
      setup_subscriber();
      break;
    }
  }
  alarm_timestamp() = td::Timestamp::now();
}

RedisListener::RedisListener(std::string redis_dsn, std::string channel_name,
                             std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> trace_processor,
                             std::shared_ptr<ExternalMessageAdmission> external_message_admission)
        : redis_dsn_(redis_dsn), channel_name_(channel_name), trace_processor_(std::move(trace_processor)),
          external_message_admission_(std::move(external_message_admission)) {
  if (!external_message_admission_) {
    external_message_admission_ = std::make_shared<ExternalMessageAdmission>();
  }
}

void RedisListener::start_up() {
  channel_listener_ = td::actor::create_actor<ChannelListener>("RedisChannelListener", redis_dsn_, channel_name_,
    [SelfId = actor_id(this)](td::Ref<vm::Cell> msg_cell) {
      return td::actor::send_closure(SelfId, &RedisListener::on_new_message, msg_cell);
    }
  );
}

void RedisListener::on_new_message(td::Ref<vm::Cell> msg_cell) {
  if (mc_data_state_.config_ == nullptr) {
    return;
  }

  auto msg_hash_norm_r = ext_in_msg_get_normalized_hash(msg_cell);
  if (msg_hash_norm_r.is_error()) {
    LOG(ERROR) << "Failed to get normalized hash for message: " << td::base64_encode(msg_cell->get_hash().as_slice()) << ": " << msg_hash_norm_r.move_as_error();
    return;
  }
  auto msg_hash_norm = msg_hash_norm_r.move_as_ok();

  int msg_type = -1;
  auto destination_r = fetch_msg_dest_address(msg_cell, msg_type);
  if (destination_r.is_error() || msg_type != block::gen::CommonMsgInfo::ext_in_msg_info) {
    LOG(ERROR) << "Failed to get destination for external message: " << td::base64_encode(msg_hash_norm.as_slice())
               << ": " << (destination_r.is_error() ? destination_r.move_as_error().to_string() : "unexpected message type");
    return;
  }
  auto destination = destination_r.move_as_ok();

  auto admission = external_message_admission_->try_acquire(msg_hash_norm, destination);
  if (!admission.accepted) {
    if (admission.reject_reason == ExternalMessageAdmission::RejectReason::Duplicate) {
      LOG(DEBUG) << "Skipping duplicate redis external message " << td::base64_encode(msg_hash_norm.as_slice());
    } else {
      LOG(DEBUG) << "Rate-limited redis external message " << td::base64_encode(msg_hash_norm.as_slice())
                 << " for destination " << destination.workchain << ":" << destination.addr.to_hex()
                 << " (" << admission.accepted_for_destination << "/"
                 << ExternalMessageAdmission::kDefaultMaxEmulationsPerDestination << " in "
                 << ExternalMessageAdmission::kDefaultWindowSeconds << "s)";
    }
    return;
  }

  auto measurement = std::make_shared<Measurement>();
  measurement->set_finality("pending");
  measurement->set_operation("emulate");
  measurement->set_source("redis");
  measurement->set_ext_msg_hash_norm(msg_hash_norm);
  measurement->set_ext_msg_hash(msg_cell->get_hash().bits());
  measurement->start_otel_child_span("prepare_input");
  measurement->end_otel_child_span("prepare_input");
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), measurement, msg_hash_norm](td::Result<Trace> R) mutable {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &RedisListener::trace_error, msg_hash_norm, R.move_as_error(), measurement);
    } else {
      td::actor::send_closure(SelfId, &RedisListener::trace_received, R.move_as_ok(), measurement);
    }
  });

  measurement->start_otel_child_span("emulate_tail");
  td::actor::create_actor<TraceEmulator>("TraceEmu", mc_data_state_, msg_cell, false, std::move(P), measurement).release();

  g_statistics.record_count(EMULATE_SRC_REDIS);
}

void RedisListener::set_mc_data_state(schema::MasterchainBlockDataState mc_data_state) {
  shard_states_.clear();
  for (const auto& shard_state : mc_data_state.shard_blocks_) {
      shard_states_.push_back(shard_state.block_state);
  }

  mc_data_state_ = std::move(mc_data_state);
}

void RedisListener::trace_error(td::Bits256 ext_in_msg_hash_norm, td::Status error, MeasurementPtr measurement) {
  LOG(ERROR) << "Failed to emulate trace from msg " << td::base64_encode(ext_in_msg_hash_norm.as_slice()) << ": " << error;
  measurement->mark_otel_error("trace_emulator.emulation_error", error.to_string());
  measurement->end_otel_child_span("emulate_tail");
  measurement->emit_otel_span();
  external_message_admission_->release_message(ext_in_msg_hash_norm);
}

void RedisListener::trace_received(Trace trace, MeasurementPtr measurement) {
  measurement->end_otel_child_span("emulate_tail");
  external_message_admission_->mark_emulated(trace.ext_in_msg_hash_norm);
  LOG(INFO) << "Emulated trace from msg " << td::base64_encode(trace.ext_in_msg_hash_norm.as_slice()) << ": "
        << trace.transactions_count() << " transactions, " << trace.depth() << " depth";
  measurement->set_transactions_count(trace.transactions_count());
  measurement->set_emulated_transactions_count(trace.root ? trace.root->emulated_transactions_count() : 0);
  measurement->set_trace_root_tx_hash(trace.root_tx_hash);
  measurement->set_otel_attribute("ton.trace.depth", static_cast<std::int64_t>(trace.depth()));
  if constexpr (std::variant_size_v<Trace::Detector::DetectedInterface> > 0) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), measurement, ext_in_msg_hash_norm = trace.ext_in_msg_hash_norm](td::Result<Trace> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &RedisListener::trace_interfaces_error, ext_in_msg_hash_norm, R.move_as_error(), measurement);
        return;
      }
      td::actor::send_closure(SelfId, &RedisListener::finish_processing, R.move_as_ok(), measurement);
    });

    measurement->start_otel_child_span("detect_interfaces");
    td::actor::create_actor<TraceInterfaceDetector>("TraceInterfaceDetector", shard_states_, mc_data_state_.config_, std::move(trace), std::move(P), measurement).release();
  } else {
    finish_processing(std::move(trace), measurement);
  }
}

void RedisListener::trace_interfaces_error(td::Bits256 ext_in_msg_hash_norm, td::Status error, MeasurementPtr measurement) {
  LOG(ERROR) << "Failed to detect interfaces on trace from msg " << td::base64_encode(ext_in_msg_hash_norm.as_slice()) << ": " << error;
  measurement->mark_otel_error("trace_emulator.interface_error", error.to_string());
  measurement->end_otel_child_span("detect_interfaces");
  measurement->emit_otel_span();
}

void RedisListener::finish_processing(Trace trace, MeasurementPtr measurement) {
  measurement->end_otel_child_span("detect_interfaces");
  measurement->start_otel_child_span("insert_trace");
  auto P = td::PromiseCreator::lambda([ext_in_msg_hash_norm = trace.ext_in_msg_hash_norm, measurement](td::Result<td::Unit> R) {
    if (R.is_error()) {
      auto error = R.move_as_error();
      LOG(ERROR) << "Failed to insert trace from msg " << td::base64_encode(ext_in_msg_hash_norm.as_slice()) << ": " << error;
      measurement->mark_otel_error("trace_emulator.insert_error", error.to_string());
      measurement->end_otel_child_span("insert_trace");
      measurement->emit_otel_span();
      return;
    }
    LOG(DEBUG) << "Successfully inserted trace from msg " << td::base64_encode(ext_in_msg_hash_norm.as_slice());
    measurement->end_otel_child_span("insert_trace");
    measurement->emit_otel_span();
  });
  trace_processor_(std::move(trace), std::move(P), measurement);
}
