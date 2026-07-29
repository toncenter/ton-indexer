#pragma once
#include <deque>
#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <memory>
#include <vector>
#include <functional>
#include <cstdint>
#include <optional>
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "OverlayListener.h"
#include "RedisListener.h"
#include "TraceEmulator.h"
#include "TraceInserter.h"
#include "BlockEmulator.h"
#include "ConfirmedRootTracker.h"
#include "OrderedResultBuffer.h"
#include "IndexData.h"
#include "ExternalMessageAdmission.h"
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
    ton::BlockSeqno last_started_finalized_seqno_{0};

    td::Timestamp next_statistics_flush_;

    std::unordered_set<ton::BlockSeqno> seqnos_to_fetch_;
    std::map<ton::BlockSeqno, schema::MasterchainBlockDataState> blocks_to_emulate_;
    std::deque<ton::BlockIdExt> signed_blocks_to_fetch_queue_;
    std::deque<ton::BlockIdExt> signed_block_queue_;
    std::deque<ton::BlockIdExt> seen_signed_block_order_;
    std::unordered_set<ton::BlockIdExt, BlockIdExtHasher> seen_signed_blocks_;
    std::unordered_set<ton::BlockIdExt, BlockIdExtHasher> signed_blocks_inflight_;
    std::unordered_map<ton::BlockIdExt, schema::BlockDataState, BlockIdExtHasher> signed_block_storage_;
    std::shared_ptr<block::ConfigInfo> latest_config_;
    std::vector<ShardStateSnapshot> latest_shard_states_;

    td::actor::ActorOwn<OverlayListener> overlay_listener_;
    td::actor::ActorOwn<RedisListener> redis_listener_;
    std::shared_ptr<ExternalMessageAdmission> external_message_admission_;
    td::actor::ActorOwn<ITraceInsertManager> insert_manager_;
    ConfirmedRootTracker confirmed_roots_;
    std::unique_ptr<sw::redis::Redis> health_redis_;
    td::Timestamp next_health_update_;
    std::uint32_t last_finalized_mc_block_time_{0};
    std::uint32_t last_confirmed_block_time_{0};
    std::optional<ton::BlockSeqno> pending_applied_mc_seqno_;
    std::deque<ton::BlockIdExt> pending_signed_blocks_;
    std::optional<ton::BlockSeqno> catch_up_applied_mc_seqno_;
    std::deque<ton::BlockIdExt> catch_up_signed_blocks_;
    bool db_catch_up_in_progress_{false};
    std::size_t finalized_blocks_in_pipeline_{0};
    std::size_t confirmed_blocks_inflight_{0};
    // Signed events are parent-first. Only this short parse/resolve stage is
    // serialized; confirmed tail emulators remain in flight in parallel.
    std::optional<ton::BlockIdExt> confirmed_head_in_progress_;

    struct FinalizedCommitState {
        ton::BlockSeqno seqno;
        std::vector<ton::BlockIdExt> finalized_blocks;
        std::size_t pending_writes;
        // Lifetime guard for lazy cells in traces queued by RedisInsertManager.
        std::vector<td::Ref<ton::validator::BlockData>> block_data_owners;
    };

    // Blocks resolve inter-block trace ids one by one, then emulate their
    // tails in parallel. Completed blocks are committed in masterchain order.
    std::optional<ton::BlockSeqno> finalized_trace_ids_in_progress_;
    OrderedResultBuffer<FinalizedBlockResult> finalized_results_;
    std::optional<FinalizedCommitState> finalized_commit_;

    std::map<ton::BlockIdExt, std::vector<ConfirmedTraceSnapshot>>
        confirmed_block_snapshots_;
    std::set<ton::BlockId> closed_confirmed_blocks_;
    std::deque<ton::BlockId> closed_confirmed_block_order_;

    void handle_block_signed(ton::BlockIdExt block_id);
    void handle_block_applied(ton::BlockIdExt block_id);
    void request_db_catch_up();
    bool has_pending_db_events() const;
    bool has_ready_finalized_block() const;
    void db_catch_up_finished(td::Result<td::Unit> result);
    void requeue_catch_up_batch();
    void process_catch_up_batch();

    void got_last_mc_seqno(ton::BlockSeqno last_known_seqno);
    void fetch_seqnos();
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, schema::MasterchainBlockDataState mc_data_state);
    void start_next_finalized_block();
    void start_finalized_emulator(
        ton::BlockSeqno seqno,
        bool reuse_confirmed_state);
    void finalized_trace_ids_resolved(ton::BlockSeqno seqno);
    void finalized_block_emulated(
        ton::BlockSeqno seqno,
        td::Result<FinalizedBlockResult> result);
    void try_commit_finalized_block();
    void commit_finalized_block(FinalizedBlockResult result);
    void finalized_trace_write_finished(ton::BlockSeqno seqno);
    void finish_finalized_commit();
    void finalized_block_done();
    void close_confirmed_block(ton::BlockId block_id);
    bool confirmed_block_is_closed(const ton::BlockIdExt& block_id) const;
    void enqueue_signed_block(ton::BlockIdExt block_id);
    void queue_signed_block_fetch(ton::BlockIdExt block_id);
    void fetch_signed_blocks();
    void signed_block_fetched(ton::BlockIdExt block_id, schema::BlockDataState block_data_state);
    void signed_block_error(ton::BlockIdExt block_id, td::Status error);
    void process_signed_blocks();
    void confirmed_block_head_finished(ton::BlockIdExt block_id);
    void confirmed_block_finished(
        ton::BlockIdExt block_id,
        td::Result<ConfirmedBlockResult> result);
    void process_confirmed_trace(
        ton::BlockIdExt block_id,
        Trace trace,
        td::Promise<ConfirmedTraceSnapshot> promise,
        MeasurementPtr measurement);
    bool remember_seen_signed_block(ton::BlockIdExt block_id);
    std::function<void(
        Trace,
        td::Promise<ConfirmedTraceSnapshot>,
        MeasurementPtr)> make_signed_trace_processor(
            const ton::BlockIdExt& block_id_ext);
    void discard_confirmed_snapshots(
        const std::vector<ton::BlockIdExt>& finalized_blocks);
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
      external_message_admission_ = std::make_shared<ExternalMessageAdmission>();
    };

    virtual void start_up() override;

    void handle_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event);
};
