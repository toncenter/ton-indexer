#pragma once
#include <queue>
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "RedisListener.h"
#include "TraceEmulator.h"
#include "TaskResultInserter.h"


class TraceTaskScheduler : public td::actor::Actor {
  private: 
    td::actor::ActorId<DbScanner> db_scanner_;
    std::string redis_dsn_;
    std::string input_redis_queue_;
    std::string working_dir_;
    std::function<void(TraceEmulationResult, td::Promise<td::Unit>)> insert_trace_;

    ton::BlockSeqno last_known_seqno_{0};
    ton::BlockSeqno last_fetched_seqno_{0};
    ton::BlockSeqno last_emulated_seqno_{0};

    td::Timestamp next_statistics_flush_;

    std::unordered_set<ton::BlockSeqno> seqnos_to_fetch_;

    td::actor::ActorOwn<RedisListener> redis_listener_;
    td::actor::ActorOwn<ITaskResultInserter> insert_manager_;

    void got_last_mc_seqno(ton::BlockSeqno last_known_seqno);
    void fetch_seqnos();
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state);

    void alarm();

  public:
    TraceTaskScheduler(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<ITaskResultInserter> insert_manager,
                           std::string redis_dsn, std::string input_redis_queue, std::string working_dir) :
        db_scanner_(db_scanner), insert_manager_(insert_manager), redis_dsn_(redis_dsn), input_redis_queue_(input_redis_queue),
        working_dir_(std::move(working_dir)) {
      insert_trace_ = [insert_manager = insert_manager_.get()](TraceEmulationResult result, td::Promise<td::Unit> promise) {
        td::actor::send_closure(insert_manager, &ITaskResultInserter::insert, std::move(result), std::move(promise));
      };
      redis_listener_ = td::actor::create_actor<RedisListener>("RedisListener", redis_dsn_, input_redis_queue_, db_scanner, insert_trace_);
    };

    virtual void start_up() override;
};
