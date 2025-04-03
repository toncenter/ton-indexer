#pragma once
#include <td/actor/actor.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include <sw/redis++/redis++.h>
#include "IndexData.h"
#include "TraceEmulator.h"
#include "TraceInterfaceDetector.h"

class ChannelListener : public td::actor::Actor {
public:
  ChannelListener(std::string redis_dsn, std::string channel_name, std::function<void(td::Ref<vm::Cell>)> on_new_message)
    : redis_dsn_(std::move(redis_dsn)), channel_name_(std::move(channel_name)), on_new_message_(std::move(on_new_message)) {}
   
  void start_up() override;
  void alarm() override;
 
private:
  void setup_subscriber();
  std::string redis_dsn_;
  std::string channel_name_;
  std::function<void(td::Ref<vm::Cell>)> on_new_message_;

  std::optional<sw::redis::Subscriber> subscriber_;
};

class RedisListener : public td::actor::Actor {
private:
  std::string redis_dsn_;
  std::string channel_name_;
  std::function<void(Trace, td::Promise<td::Unit>)> trace_processor_;

  MasterchainBlockDataState mc_data_state_;
  std::vector<td::Ref<vm::Cell>> shard_states_;

  std::unordered_set<td::Bits256> known_ext_msgs_;

  td::actor::ActorOwn<ChannelListener> channel_listener_;

public:
  RedisListener(std::string redis_dsn, std::string channel_name, std::function<void(Trace, td::Promise<td::Unit>)> trace_processor);
  void start_up() override;
  void set_mc_data_state(MasterchainBlockDataState mc_data_state);
private:
  void on_new_message(td::Ref<vm::Cell> msg_cell);
  void trace_error(td::Bits256 ext_in_msg_hash, td::Status error);
  void trace_received(Trace trace);
  void trace_interfaces_error(td::Bits256 ext_in_msg_hash, td::Status error);
  void finish_processing(Trace trace);
};
