#pragma once
#include <td/actor/actor.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include <sw/redis++/redis++.h>
#include "IndexData.h"
#include "TraceEmulator.h"
#include "TraceInterfaceDetector.h"


class RedisListener : public td::actor::Actor {
private:
  sw::redis::Redis redis_;
  std::string queue_name_;
  std::function<void(Trace, td::Promise<td::Unit>)> trace_processor_;

  MasterchainBlockDataState mc_data_state_;
  std::vector<td::Ref<vm::Cell>> shard_states_;

  std::unordered_set<td::Bits256> known_ext_msgs_; // this set grows infinitely. TODO: remove old messages

public:
  RedisListener(std::string redis_dsn, std::string queue_name, std::function<void(Trace, td::Promise<td::Unit>)> trace_processor)
      : redis_(sw::redis::Redis(redis_dsn)), queue_name_(queue_name), trace_processor_(std::move(trace_processor)) {};

  virtual void start_up() override;

  void alarm() override;
  void set_mc_data_state(MasterchainBlockDataState mc_data_state);

private:
  void trace_error(td::Bits256 ext_in_msg_hash, td::Status error);
  void trace_received(Trace trace);
  void trace_interfaces_error(td::Bits256 ext_in_msg_hash, td::Status error);
  void finish_processing(Trace trace);
};
