#include "SmcScanner.h"
#include "convert-utils.h"
#include "ShardBatchScanner.h"

void SmcScanner::start_up() {
    auto P = td::PromiseCreator::lambda([=, SelfId = actor_id(this)](td::Result<MasterchainBlockDataState> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to get seqno " << options_.seqno_ << ": " << R.move_as_error();
            stop();
            return;
        }
        td::actor::send_closure(SelfId, &SmcScanner::got_block, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, options_.seqno_, std::move(P));
}

void SmcScanner::got_block(MasterchainBlockDataState block) {
    LOG(INFO) << "Got block data state";
    for (const auto &shard_ds : block.shard_blocks_) {
        auto& shard_state = shard_ds.block_state;
        std::string actor_name = "SScanner:" + shard_ds.handle->id().shard_full().to_str();
        td::actor::create_actor<ShardStateScanner>(actor_name, shard_state, block, options_).release();
    }
}

