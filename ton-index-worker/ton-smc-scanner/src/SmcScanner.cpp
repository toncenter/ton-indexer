#include "SmcScanner.h"
#include "convert-utils.h"
#include "ShardBatchScanner.h"

void SmcScanner::start_up() {
    auto P = td::PromiseCreator::lambda([this, SelfId = actor_id(this)](td::Result<DataContainerPtr> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to get seqno " << this->options_.seqno_ << ": " << R.move_as_error();
            stop();
            return;
        }
        td::actor::send_closure(SelfId, &SmcScanner::got_block, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, options_.seqno_, std::move(P));
}

void SmcScanner::got_block(DataContainerPtr block) {
    LOG(INFO) << "Got block data state: " << block->mc_seqno_;
    for (const auto &shard_ds : block->mc_block_.shard_blocks_) {
        auto& shard_state = shard_ds.block_state;
        std::string actor_name = "SScanner:" + shard_ds.handle->id().shard_full().to_str();
        td::actor::create_actor<ShardStateScanner>(actor_name, shard_state, block->mc_block_, options_).release();
    }
}

