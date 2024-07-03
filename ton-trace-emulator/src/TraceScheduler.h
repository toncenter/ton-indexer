#pragma once
#include <queue>
#include "td/actor/actor.h"
#include "DbScanner.h"
#include "OverlayListener.h"
#include "TraceEmulator.h"
#include "TraceInserter.h"


class TraceEmulatorScheduler : public td::actor::Actor {
  private: 
    td::actor::ActorId<DbScanner> db_scanner_;
    std::string global_config_path_;
    std::string inet_addr_;
    std::function<void(std::unique_ptr<Trace>)> insert_trace_;

    ton::BlockSeqno last_known_seqno_{0};

    std::queue<std::uint32_t> queued_seqnos_;
    td::Timestamp next_print_stats_;
    std::unordered_set<std::uint32_t> seqnos_fetching_;
    std::unordered_set<std::uint32_t> seqnos_emulating_;
    std::set<std::uint32_t> seqnos_processed_;
    std::queue<std::pair<std::uint32_t, MasterchainBlockDataState>> blocks_to_emulate_;
    std::uint32_t blocks_to_emulate_queue_max_size_{1000};

    td::actor::ActorOwn<OverlayListener> overlay_listener_;

    void got_last_mc_seqno(ton::BlockSeqno last_known_seqno);
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state);

    void alarm();

  public:
    TraceEmulatorScheduler(td::actor::ActorId<DbScanner> db_scanner, std::string global_config_path, std::string inet_addr) :
        db_scanner_(db_scanner), global_config_path_(global_config_path), inet_addr_(inet_addr) {
      insert_trace_ = [](std::unique_ptr<Trace> trace) {
        auto P = td::PromiseCreator::lambda([trace_id = trace->id](td::Result<td::Unit> R) {
          if (R.is_error()) {
            LOG(ERROR) << "Failed to insert trace " << trace_id.to_hex() << ": " << R.move_as_error();
          }
          LOG(DEBUG) << "Successfully inserted trace " << trace_id.to_hex();
        });
        td::actor::create_actor<TraceInserter>("TraceInserter", std::move(trace), std::move(P)).release();
      };
    };

    virtual void start_up() override;
};
