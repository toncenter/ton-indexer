#include "Scheduler.h"

#include "td/utils/StringBuilder.h"


void Scheduler::start_up() {
    LOG(INFO) << "Scheduler start_up";
}

void Scheduler::alarm() {
    LOG(INFO) << "Scheduler: alarm";
}

void Scheduler::run() {
    LOG(INFO) << "Scheduler: run";
    ton::BlockSeqno mc_seqno{1024};
}

void Scheduler::got_block_state_data(ton::BlockSeqno mc_seqno, MasterchainBlockDataState R) {
    LOG(INFO) << "Scheduler: Got first block " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno, R_ = std::move(R)](td::Result<MasterchainBlockDataState> res) {
        if(res.is_error()) {
            LOG(ERROR) << "DbScanner: Failed to fetch seqno " << mc_seqno;
            return;
        }
        td::actor::send_closure(SelfId, &Scheduler::got_block_state_data_2, mc_seqno, std::move(R_), res.move_as_ok());
    });
    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, mc_seqno, std::move(P));
}

void Scheduler::got_block_state_data_2(ton::BlockSeqno mc_seqno, MasterchainBlockDataState R1, MasterchainBlockDataState R2) {
    LOG(INFO) << "Scheduler: Got both blocks " << mc_seqno;
    std::_Exit(0);
}
