#pragma once
#include <td/actor/actor.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include <sw/redis++/redis++.h>
#include <msgpack.hpp>
#include "IndexData.h"
#include "TraceEmulator.h"
#include "TraceInterfaceDetector.h"

struct TraceTask {
  std::string id;
  std::string boc;
  bool ignore_chksig;
  bool detect_interfaces;
  bool include_code_data;
  std::optional<uint32_t> mc_block_seqno;

  MSGPACK_DEFINE(id, boc, ignore_chksig, detect_interfaces, include_code_data, mc_block_seqno);
};

struct TraceEmulationResult {
  TraceTask task;
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
  RedisListener(std::string redis_dsn, std::string queue_name, td::actor::ActorId<DbScanner> db_scanner, typeof(trace_processor_) trace_processor)
      : redis_(sw::redis::Redis(redis_dsn)), queue_name_(queue_name), db_scanner_(db_scanner), trace_processor_(std::move(trace_processor)) {};

  virtual void start_up() override;

  void alarm() override;
  void set_mc_data_state(MasterchainBlockDataState mc_data_state);

private:
  void trace_error(TraceTask task, std::optional<ton::BlockId> mc_block_id, td::Status error);
  void trace_received(TraceTask task, MasterchainBlockDataState mc_data_state, Trace trace);
  void trace_interfaces_error(TraceTask task, ton::BlockId mc_block_id, td::Status error);
  void finish_processing(TraceTask task, ton::BlockId mc_block_id, Trace trace);
};