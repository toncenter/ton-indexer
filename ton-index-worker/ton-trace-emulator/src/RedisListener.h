#pragma once
#include <td/actor/actor.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include "ChannelListener.h"
#include <memory>
#include "IndexData.h"
#include "ExternalMessageAdmission.h"
#include "TraceEmulator.h"
#include "TraceInterfaceDetector.h"
#include "Measurement.h"

class RedisListener : public td::actor::Actor {
private:
  using TraceProcessorFn = std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)>;
  RedisConnectionOptions redis_options_;
  std::string channel_name_;
  TraceProcessorFn trace_processor_;
  std::shared_ptr<ExternalMessageAdmission> external_message_admission_;

  schema::MasterchainBlockDataState mc_data_state_;
  std::vector<td::Ref<vm::Cell>> shard_states_;

  td::actor::ActorOwn<ChannelListener> channel_listener_;

public:
  RedisListener(RedisConnectionOptions redis_options, std::string channel_name, TraceProcessorFn trace_processor,
                std::shared_ptr<ExternalMessageAdmission> external_message_admission = nullptr);
  void start_up() override;
  void set_mc_data_state(schema::MasterchainBlockDataState mc_data_state);
private:
  void on_messages(std::vector<std::string> messages, td::Promise<td::Unit> done);
  void on_new_message(td::Ref<vm::Cell> msg_cell);
  void trace_error(td::Bits256 ext_in_msg_hash, td::Status error, MeasurementPtr measurement);
  void trace_received(Trace trace, MeasurementPtr measurement);
  void trace_interfaces_error(td::Bits256 ext_in_msg_hash, td::Status error, MeasurementPtr measurement);
  void finish_processing(Trace trace, MeasurementPtr measurement);
};
