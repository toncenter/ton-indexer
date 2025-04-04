#pragma once
#include <queue>
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "OverlayListener.h"
#include "RedisListener.h"
#include "TraceEmulator.h"
#include "TraceInserter.h"


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

    td::actor::ActorOwn<OverlayListener> overlay_listener_;
    td::actor::ActorOwn<RedisListener> redis_listener_;
    td::actor::ActorOwn<ITraceInsertManager> insert_manager_;

    void got_last_mc_seqno(ton::BlockSeqno last_known_seqno);
    void fetch_seqnos();
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state);
    void emulate_blocks();

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
    };

    virtual void start_up() override;
};
