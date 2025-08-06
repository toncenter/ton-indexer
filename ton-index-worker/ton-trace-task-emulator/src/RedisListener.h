#pragma once
#include <td/actor/actor.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include <sw/redis++/redis++.h>
#include <msgpack.hpp>
#include "IndexData.h"
#include "TraceEmulator.h"
#include "TraceInterfaceDetector.h"
#include "TonConnectProcessor.h"

struct TraceEmulationResult {
  std::string task_id;
  td::Result<Trace> trace;
  std::optional<ton::BlockId> mc_block_id;
};

class RedisListener : public td::actor::Actor {
private:
  sw::redis::Redis redis_;
  std::string queue_name_;
  td::actor::ActorId<DbScanner> db_scanner_;
  std::function<void(TraceEmulationResult, td::Promise<td::Unit>)> trace_processor_;
  
  MasterchainBlockDataState mc_data_state_;
  ton::BlockId current_mc_block_id_;

public:
  RedisListener(std::string redis_dsn, std::string queue_name, td::actor::ActorId<DbScanner> db_scanner, std::function<void(TraceEmulationResult, td::Promise<td::Unit>)> trace_processor)
      : redis_(sw::redis::Redis(redis_dsn)), queue_name_(queue_name), db_scanner_(db_scanner), trace_processor_(std::move(trace_processor)) {};

  virtual void start_up() override;

  void alarm() override;
  void set_mc_data_state(MasterchainBlockDataState mc_data_state);

private:
  void trace_error(std::string task_id, std::optional<ton::BlockId> mc_block_id, td::Status error);
  void trace_received(TraceTask task, MasterchainBlockDataState mc_data_state, Trace trace);
  void trace_interfaces_error(std::string task_id, ton::BlockId mc_block_id, td::Status error);
  void finish_processing(std::string task_id, ton::BlockId mc_block_id, Trace trace);
  
  // Process regular trace task
  void process_trace_task(TraceTask task);
  
  // Process TonConnect task (converts to regular trace task)
  void process_tonconnect_task(TonConnectTraceTask task);
};
