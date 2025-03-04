#pragma once
#include <any>
#include "td/actor/actor.h"
#include "DbScanner.h"


// Stub parser that does nothing for now.
class IntegrityParser: public td::actor::Actor  {
  public:
    IntegrityParser() {}

    void parse(int mc_seqno, MasterchainBlockDataState mc_block, td::Promise<td::Unit> promise) {
      promise.set_result(td::Unit());
    }
};


class IntegrityChecker : public td::actor::Actor {
  private: 
    td::actor::ActorId<DbScanner> db_scanner_;
    td::actor::ActorId<IntegrityParser> parse_manager_;
    std::string checkpoint_path_;
    size_t fetch_parallelism_;
    size_t parse_parallelism_;
    std::uint32_t stats_timeout_;
    size_t min_free_memory_;
    std::shared_ptr<td::Destructor> watcher_;

    std::uint32_t from_seqno_{0};
    std::uint32_t to_seqno_{0};
    std::queue<std::uint32_t> queued_seqnos_;
    td::Timestamp next_print_stats_;
    std::unordered_set<std::uint32_t> seqnos_fetching_;
    std::unordered_set<std::uint32_t> seqnos_parsing_;
    std::set<std::uint32_t> seqnos_processed_;
    std::queue<std::pair<std::uint32_t, MasterchainBlockDataState>> blocks_to_parse_;
    std::uint32_t blocks_to_parse_queue_max_size_{1000};

    td::Timestamp last_tps_calc_ts_ = td::Timestamp::now();
    uint32_t last_tps_calc_processed_count_{0};
    float tps_{0};

    std::uint32_t checkpoint_seqno_{0};

  public:
    IntegrityChecker(td::actor::ActorId<DbScanner> db_scanner, td::actor::ActorId<IntegrityParser> parse_manager, std::string checkpoint_path, 
      std::size_t fetch_parallelism = 1, std::size_t parse_parallelism = 1, std::uint32_t stats_timeout = 60, size_t min_free_memory = 3, std::shared_ptr<td::Destructor> watcher = nullptr) :
        db_scanner_(db_scanner), parse_manager_(parse_manager), checkpoint_path_(checkpoint_path), fetch_parallelism_(fetch_parallelism), 
        parse_parallelism_(parse_parallelism), stats_timeout_(stats_timeout), min_free_memory_(min_free_memory), watcher_(watcher) {};
    virtual ~IntegrityChecker() = default;

    virtual void start_up() override;

    void got_last_mc_seqno(std::uint32_t last_known_seqno);
    void got_block_handle(ton::validator::ConstBlockHandle handle);
    void got_min_mc_seqno(ton::BlockSeqno min_known_seqno);
    void got_oldest_mc_seqno_with_state(ton::BlockSeqno oldest_seqno_with_state);
    void find_oldest_seqno_with_state(uint32_t min_seqno, uint32_t max_seqno);
    void fetch_next_seqnos();
    void fetch_error(std::uint32_t seqno, td::Status error);
    void seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState state);
    void parse_next_seqnos();
    void parse_error(std::uint32_t seqno, td::Status error);
    void seqno_parsed(std::uint32_t seqno);
    
    void print_stats();
    void fail_if_low_memory();

    void alarm();
};
