#pragma once
#include <queue>
#include <deque>
#include <unordered_set>
#include <unordered_map>
#include <memory>
#include <vector>
#include <functional>
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "OverlayListener.h"
#include "RedisListener.h"
#include "TraceEmulator.h"
#include "TraceInserter.h"
#include "BlockEmulator.h"
#include "IndexData.h"
#include "InvalidatedTraceTracker.h"

class TraceEmulatorScheduler : public td::actor::Actor {
  private: 
    td::actor::ActorId<DbScanner> db_scanner_;
    std::string global_config_path_;
    std::string inet_addr_;
    std::string redis_dsn_;
    std::string input_redis_channel_;
    std::string working_dir_;
    std::function<void(Trace, td::Promise<td::Unit>)> insert_trace_;

    ton::BlockSeqno last_known_seqno_{0};
    ton::BlockSeqno last_fetched_seqno_{0};
    ton::BlockSeqno last_emulated_seqno_{0};

    td::Timestamp next_statistics_flush_;

    std::unordered_set<ton::BlockSeqno> seqnos_to_fetch_;
    std::map<ton::BlockSeqno, MasterchainBlockDataState> blocks_to_emulate_;
    std::deque<ton::BlockIdExt> confirmed_block_queue_;
    std::unordered_set<ton::BlockIdExt, BlockIdExtHasher> known_temp_blocks_;
    bool initial_temp_snapshot_taken_{false};
    std::unordered_set<ton::BlockIdExt, BlockIdExtHasher> confirmed_blocks_inflight_;
    std::unordered_map<ton::BlockIdExt, BlockDataState, BlockIdExtHasher> confirmed_block_storage_;
    std::shared_ptr<block::ConfigInfo> latest_config_;
    std::vector<ShardStateSnapshot> latest_shard_states_;

    td::actor::ActorOwn<OverlayListener> overlay_listener_;
    td::actor::ActorOwn<RedisListener> redis_listener_;
    td::actor::ActorOwn<ITraceInsertManager> insert_manager_;
    td::actor::ActorOwn<InvalidatedTraceTracker> invalidated_trace_tracker_;

    void got_last_mc_seqno(ton::BlockSeqno last_known_seqno);
    void fetch_seqnos();
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state);
    void emulate_blocks();
    void scan_unconfirmed_shards();
    void handle_temp_block(ton::BlockIdExt block_id, bool capture_only);
    void enqueue_confirmed_block(ton::BlockIdExt block_id);
    void confirmed_block_fetched(ton::BlockIdExt block_id, BlockDataState block_data_state);
    void confirmed_block_error(ton::BlockIdExt block_id, td::Status error);
    void process_confirmed_blocks();
    std::function<void(Trace, td::Promise<td::Unit>)> make_confirmed_trace_processor(const ton::BlockIdExt& block_id_ext);
    std::function<void(Trace, td::Promise<td::Unit>)> make_finalized_trace_processor(const MasterchainBlockDataState& mc_data_state);

    void alarm();

  public:
    TraceEmulatorScheduler(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<ITraceInsertManager> insert_manager,
                           std::string global_config_path, std::string inet_addr, 
                           std::string redis_dsn, std::string input_redis_channel, std::string working_dir) :
        db_scanner_(db_scanner), insert_manager_(insert_manager), global_config_path_(global_config_path), 
        inet_addr_(inet_addr), redis_dsn_(redis_dsn), input_redis_channel_(input_redis_channel), working_dir_(std::move(working_dir)) {
      insert_trace_ = [insert_manager = insert_manager_.get()](Trace trace, td::Promise<td::Unit> promise) {
        td::actor::send_closure(insert_manager, &ITraceInsertManager::insert, std::move(trace), std::move(promise));
      };
      invalidated_trace_tracker_ = td::actor::create_actor<InvalidatedTraceTracker>("InvalidatedTraceTracker", redis_dsn_);
    };

    virtual void start_up() override;
};
