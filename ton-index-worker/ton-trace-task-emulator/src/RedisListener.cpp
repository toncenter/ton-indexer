#include "RedisListener.h"
#include "Statistics.h"


void RedisListener::start_up() {
  alarm_timestamp() = td::Timestamp::now() ;
}

void RedisListener::alarm() {
  while (auto buffer = redis_.rpop(queue_name_)) {
    // Try to parse as TraceTask first
    bool is_trace_task = false;
    bool is_tonconnect_task = false;
    TraceTask trace_task;
    TonConnectTraceTask tonconnect_task;

    msgpack::unpacked res;
    msgpack::unpack(res, buffer->data(), buffer->size());
    msgpack::object obj = res.get();

    TaskEnvelope env;
    obj.convert(env); // map keys must match member names

    if (env.type == "trace") {
        env.task.convert(trace_task);
        is_trace_task = true;
    } else if (env.type == "tonconnect") {
        env.task.convert(tonconnect_task);
        is_tonconnect_task = true;
    } else {
        LOG(ERROR) << "Unknown task type: " << env.type;
        continue;
    }

    if (is_tonconnect_task) {
        // Handle TonConnect task using the processor
        process_tonconnect_task(std::move(tonconnect_task));
        continue;
    }
    
    if (is_trace_task) {
        // Handle regular trace task
        process_trace_task(std::move(trace_task));
        continue;
    }
    
    LOG(ERROR) << "Unknown task type";
  }

  alarm_timestamp() = td::Timestamp::now();
}

void RedisListener::set_mc_data_state(schema::MasterchainBlockDataState mc_data_state) {
  mc_data_state_ = std::move(mc_data_state);
}

void RedisListener::trace_error(std::string task_id, std::optional<ton::BlockId> mc_block_id, td::Status error) {
  LOG(ERROR) << "Failed to emulate trace " << task_id << ": " << error;
  TraceEmulationResult res{std::move(task_id), error.move_as_error(), mc_block_id};
  trace_processor_(std::move(res), td::PromiseCreator::lambda([](td::Result<td::Unit> R) {}));
}

void RedisListener::trace_received(TraceTask task, schema::MasterchainBlockDataState mc_data_state, Trace trace) {
  LOG(INFO) << "Emulated trace " << task.id << ": " << trace.transactions_count() << " transactions, " << trace.depth() << " depth";
  if (task.detect_interfaces) {
    std::vector<td::Ref<vm::Cell>> shard_states;
    for (const auto& shard_state : mc_data_state.shard_blocks_) {
      shard_states.push_back(shard_state.block_state);
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), task = std::move(task), mc_block_id = mc_data_state.config_->block_id.id](td::Result<Trace> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &RedisListener::trace_interfaces_error, task.id, std::move(mc_block_id), R.move_as_error());
        return;
      }
      td::actor::send_closure(SelfId, &RedisListener::finish_processing, task.id, std::move(mc_block_id), R.move_as_ok());
    });

    auto measurement = std::make_shared<Measurement>();
    td::actor::create_actor<TraceInterfaceDetector>("TraceInterfaceDetector", std::move(shard_states), mc_data_state_.config_, std::move(trace), std::move(P), measurement).release();
  } else {
    finish_processing(task.id, mc_data_state.config_->block_id.id, std::move(trace));
  }
}

void RedisListener::trace_interfaces_error(std::string task_id, ton::BlockId mc_block_id, td::Status error) {
    LOG(ERROR) << "Failed to detect interfaces on task " << task_id << ": " << error;
    TraceEmulationResult res{std::move(task_id), error.move_as_error(), mc_block_id};
    trace_processor_(std::move(res), td::PromiseCreator::lambda([](td::Result<td::Unit> R) {}));
}

void RedisListener::finish_processing(std::string task_id, ton::BlockId mc_block_id, Trace trace) {
    LOG(INFO) << "Finished emulating trace " << task_id;
    auto P = td::PromiseCreator::lambda([task_id](td::Result<td::Unit> R) {
      if (R.is_error()) {
        LOG(ERROR) << "Failed to insert trace task " << task_id << ": " << R.move_as_error();
        return;
      }
      LOG(DEBUG) << "Successfully inserted trace task" << task_id;
    });
    TraceEmulationResult res{std::move(task_id), std::move(trace), mc_block_id};
    trace_processor_(std::move(res), std::move(P));
}

void RedisListener::process_trace_task(TraceTask task) {
    if (mc_data_state_.config_ == nullptr) {
      trace_error(task.id, std::nullopt, td::Status::Error("RedisListener not ready"));
      alarm_timestamp() = td::Timestamp::in(0.1);
      return;
    }

    auto boc_decoded = td::base64_decode(task.boc);
    if (boc_decoded.is_error()) {
      auto error = td::Status::Error(PSLICE() << "Can't decode base64 boc: " << boc_decoded.move_as_error());
      LOG(ERROR) << error;
      TraceEmulationResult res{task.id, std::move(error), std::nullopt};
      trace_processor_(std::move(res), td::PromiseCreator::lambda([](td::Result<td::Unit> R) {}));
      return;
    }
    auto msg_cell_r = vm::std_boc_deserialize(boc_decoded.move_as_ok());
    if (msg_cell_r.is_error()) {
      auto error = td::Status::Error(PSLICE() << "Can't deserialize message boc: " << msg_cell_r.move_as_error());
      LOG(ERROR) << error;
      TraceEmulationResult res{task.id, std::move(error), std::nullopt};
      trace_processor_(std::move(res), td::PromiseCreator::lambda([](td::Result<td::Unit> R) {}));
      return;
    }
    auto msg_cell = msg_cell_r.move_as_ok();

    auto ignore_chksig = task.ignore_chksig;
    if (task.mc_block_seqno.has_value()) {
      auto mc_block_seqno = task.mc_block_seqno.value();
      if (mc_block_seqno > mc_data_state_.shard_blocks_[0].block_data->block_id().id.seqno) {
        trace_error(task.id, std::nullopt, td::Status::Error("mc block seqno is too new"));
        return;
      }
      auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_block_seqno, task, msg_cell, ignore_chksig](td::Result<schema::MasterchainBlockDataState> R) mutable {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &RedisListener::trace_error,
                task.id, std::nullopt, R.move_as_error_prefix(PSLICE() << "failed to fetch mc block  " << mc_block_seqno << ": "));
            return;
        }
        auto mc_block_ds = R.move_as_ok();
        for (auto &block_ds : mc_block_ds.shard_blocks_) {
            if (block_ds.block_data->block_id().is_masterchain()) {
                mc_block_ds.config_ = block::ConfigInfo::extract_config(block_ds.block_state, 
                  block_ds.block_data->block_id(), block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries | block::ConfigInfo::needWorkchainInfo | block::ConfigInfo::needSpecialSmc).move_as_ok();
                break;
            }
        }
        if (mc_block_ds.config_ == nullptr) {
            td::actor::send_closure(SelfId, &RedisListener::trace_error, task.id, mc_block_ds.config_->block_id.id, td::Status::Error("failed to extract config"));
            return;
        }
        auto P = td::PromiseCreator::lambda([SelfId, task = std::move(task), mc_block_ds](td::Result<Trace> R) mutable {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &RedisListener::trace_error, task.id, mc_block_ds.config_->block_id.id, R.move_as_error());
          } else {
            td::actor::send_closure(SelfId, &RedisListener::trace_received, std::move(task), std::move(mc_block_ds), R.move_as_ok());
          }
        });
      auto measurement = std::make_shared<Measurement>();
        td::actor::create_actor<TraceEmulator>("TraceEmu", mc_block_ds, msg_cell, ignore_chksig, std::move(P), measurement).release();

        g_statistics.record_count(EMULATE_SRC_REDIS);
      });
      td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, mc_block_seqno, std::move(P));
    } else {
      auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), task = std::move(task), mc_data_state = mc_data_state_](td::Result<Trace> R) mutable {
        if (R.is_error()) {
          td::actor::send_closure(SelfId, &RedisListener::trace_error, task.id, mc_data_state.config_->block_id.id, R.move_as_error());
        } else {
          td::actor::send_closure(SelfId, &RedisListener::trace_received, std::move(task), mc_data_state, R.move_as_ok());
        }
      });
      auto measurement = std::make_shared<Measurement>();
      td::actor::create_actor<TraceEmulator>("TraceEmu", mc_data_state_, msg_cell, ignore_chksig, std::move(P), measurement).release();

      g_statistics.record_count(EMULATE_SRC_REDIS);
    }
}

void RedisListener::process_tonconnect_task(TonConnectTraceTask tonconnect_task) {
  LOG(INFO) << "Processing TonConnect task " << tonconnect_task.id;
  
  if (mc_data_state_.config_ == nullptr) {
    trace_error(tonconnect_task.id, std::nullopt, td::Status::Error("RedisListener not ready"));
    return;
  }

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), task_id = tonconnect_task.id](td::Result<TraceTask> R) {
    if (R.is_error()) {
      LOG(ERROR) << "Failed to convert TonConnect task: " << R.error();
      td::actor::send_closure(SelfId, &RedisListener::trace_error, task_id, std::nullopt, R.move_as_error());
      return;
    }
    auto trace_task = R.move_as_ok();
    LOG(INFO) << "Successfully converted TonConnect task " << task_id << " to TraceTask";
    
    // Process the converted TraceTask normally
    td::actor::send_closure(SelfId, &RedisListener::process_trace_task, std::move(trace_task));
  });

  td::actor::create_actor<TonConnectProcessor>("TonConnectProcessor", std::move(tonconnect_task), 
                                          mc_data_state_, std::move(P)).release();
}
