#pragma once
#include "td/actor/actor.h"

#include "DbReader.h"
#include "DataParser.h"
#include "DbScanner.h"


class Scheduler: public td::actor::Actor {
private:
    td::actor::ActorId<DbScanner> db_scanner_;
    td::actor::ActorId<DbReader> db_reader_;
    td::actor::ActorId<ParseManager> parse_manager_;
    
    ton::BlockSeqno last_known_seqno_{2};
public:
    Scheduler(td::actor::ActorId<DbScanner> db_scanner, 
              td::actor::ActorId<DbReader> db_reader, 
              td::actor::ActorId<ParseManager> parse_manager, td::int32 last_known_seqno)
        : db_scanner_(db_scanner), db_reader_(db_reader), parse_manager_(parse_manager), last_known_seqno_(last_known_seqno) {}
    void start_up() override;
    void alarm() override;
    void run();

    void got_block_state_data(ton::BlockSeqno mc_seqno, MasterchainBlockDataState R);
    void got_block_state_data_2(ton::BlockSeqno mc_seqno, MasterchainBlockDataState R1, MasterchainBlockDataState R2);
};
