#pragma once
#include <queue>
#include <deque>
#include <unordered_set>
#include <unordered_map>
#include <memory>
#include <vector>
#include <functional>
#include <cstdint>
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "OverlayListener.h"
#include "RedisListener.h"
#include "TraceEmulator.h"
#include "TraceInserter.h"
#include "BlockEmulator.h"
#include "IndexData.h"
#include "InvalidatedTraceTracker.h"
#include "auto/tl/ton_api.h"
#include "DbEventListener.h"
#include <sw/redis++/redis++.h>


class TraceEmulatorScheduler : public td::actor::Actor {
  private: 
    td::actor::ActorId<DbScanner> db_scanner_;
    std::string global_config_path_;
    std::string inet_addr_;
    std::string redis_dsn_;
    std::string input_redis_channel_;
    std::string working_dir_;
    std::string db_event_fifo_path_;
    std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> insert_trace_;
    td::actor::ActorOwn<DbEventListener> db_event_listener_;

    ton::BlockSeqno last_known_seqno_{0};
    ton::BlockSeqno last_fetched_seqno_{0};
    ton::BlockSeqno last_emulated_seqno_{0};

    td::Timestamp next_statistics_flush_;

    std::unordered_set<ton::BlockSeqno> seqnos_to_fetch_;
    std::map<ton::BlockSeqno, schema::MasterchainBlockDataState> blocks_to_emulate_;
    std::deque<ton::BlockIdExt> signed_block_queue_;
    std::unordered_set<ton::BlockIdExt, BlockIdExtHasher> signed_blocks_inflight_;
    std::unordered_map<ton::BlockIdExt, BlockDataState, BlockIdExtHasher> signed_block_storage_;
    std::shared_ptr<block::ConfigInfo> latest_config_;
    std::vector<ShardStateSnapshot> latest_shard_states_;

    td::actor::ActorOwn<OverlayListener> overlay_listener_;
    td::actor::ActorOwn<RedisListener> redis_listener_;
    td::actor::ActorOwn<ITraceInsertManager> insert_manager_;
    td::actor::ActorOwn<InvalidatedTraceTracker> invalidated_trace_tracker_;
    std::unique_ptr<sw::redis::Redis> health_redis_;
    td::Timestamp next_health_update_;
    std::uint32_t last_finalized_mc_block_time_{0};
    std::uint32_t last_confirmed_block_time_{0};

    void handle_block_signed(ton::BlockIdExt block_id);
    void handle_block_applied(ton::BlockIdExt block_id);

    void got_last_mc_seqno(ton::BlockSeqno last_known_seqno);
    void fetch_seqnos();
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, schema::MasterchainBlockDataState mc_data_state);
    void emulate_blocks();
    void enqueue_signed_block(ton::BlockIdExt block_id);
    void signed_block_fetched(ton::BlockIdExt block_id, BlockDataState block_data_state);
    void signed_block_error(ton::BlockIdExt block_id, td::Status error);
    void process_signed_blocks();
    std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> make_signed_trace_processor(const ton::BlockIdExt& block_id_ext);
    std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> make_finalized_trace_processor(const MasterchainBlockDataState& mc_data_state);
    void publish_health();

    void alarm() override;

  public:
    TraceEmulatorScheduler(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<ITraceInsertManager> insert_manager,
                           std::string global_config_path, std::string inet_addr, 
                           std::string redis_dsn, std::string input_redis_channel, std::string working_dir,
                           std::string db_event_fifo_path) :
        db_scanner_(db_scanner), insert_manager_(insert_manager), global_config_path_(global_config_path), 
        inet_addr_(inet_addr), redis_dsn_(redis_dsn), input_redis_channel_(input_redis_channel),
        working_dir_(std::move(working_dir)), db_event_fifo_path_(std::move(db_event_fifo_path)) {
      health_redis_ = std::make_unique<sw::redis::Redis>(redis_dsn_);
      insert_trace_ = [insert_manager = insert_manager_.get()](Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) {
        td::actor::send_closure(insert_manager, &ITraceInsertManager::insert, std::move(trace), std::move(promise), measurement);
      };
      invalidated_trace_tracker_ = td::actor::create_actor<InvalidatedTraceTracker>("InvalidatedTraceTracker", redis_dsn_);
    };

    virtual void start_up() override;

    void handle_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event);
};
