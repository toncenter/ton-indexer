#include "RedisListener.h"

#include "Measurement.h"
#include "Statistics.h"

#include <cstdint>

RedisListener::RedisListener(RedisConnectionOptions redis_options, std::string channel_name,
                             std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> trace_processor,
                             std::shared_ptr<ExternalMessageAdmission> external_message_admission)
    : redis_options_(std::move(redis_options))
    , channel_name_(channel_name)
    , trace_processor_(std::move(trace_processor))
    , external_message_admission_(std::move(external_message_admission)) {
  if (!external_message_admission_) {
    external_message_admission_ = std::make_shared<ExternalMessageAdmission>();
  }
}

void RedisListener::start_up() {
  channel_listener_ = td::actor::create_actor<ChannelListener>(
      td::actor::ActorOptions().with_name("RedisChannelListener").with_poll(), redis_options_, channel_name_,
      [self = actor_id(this)](std::vector<std::string> messages, td::Promise<td::Unit> done) {
        td::actor::send_closure(self, &RedisListener::on_messages, std::move(messages), std::move(done));
      });
}

void RedisListener::on_messages(std::vector<std::string> messages, td::Promise<td::Unit> done) {
  for (const auto& value : messages) {
    auto decoded = td::base64_decode(value);
    if (decoded.is_error()) {
      LOG(ERROR) << "Can't decode base64 boc: " << decoded.move_as_error();
      continue;
    }
    auto cell = vm::std_boc_deserialize(decoded.move_as_ok());
    if (cell.is_error()) {
      LOG(ERROR) << "Can't deserialize message boc: " << cell.move_as_error();
      continue;
    }
    on_new_message(cell.move_as_ok());
  }
  done.set_value(td::Unit());
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
  // Carry the detector's states into tier-2 lookups for untouched accounts.
  trace.shard_states = shard_states_;
  trace.config = mc_data_state_.config_;
  measurement->end_otel_child_span("detect_interfaces");
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
