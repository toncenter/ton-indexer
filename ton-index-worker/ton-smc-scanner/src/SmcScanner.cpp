#include "SmcScanner.h"
#include "convert-utils.h"
#include "ShardBatchScanner.h"
#include <block/block.h>

void SmcScanner::start_up() {
    auto P = td::PromiseCreator::lambda([=, SelfId = actor_id(this)](td::Result<schema::MasterchainBlockDataState> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to get seqno " << options_.seqno_ << ": " << R.move_as_error();
            stop();
            return;
        }
        td::actor::send_closure(SelfId, &SmcScanner::got_block, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, options_.seqno_, std::move(P));
}

void SmcScanner::got_block(schema::MasterchainBlockDataState block) {
    LOG(INFO) << "Got block data state";
    AllShardStates shard_states;
    shard_states.reserve(block.shard_blocks_.size());
    auto reload_context = std::make_shared<ReloadShardStateContext>();
    reload_context->state_root_hashes_.reserve(block.shard_blocks_.size());
    reload_context->config_block_id_ = block.shard_blocks_[0].handle->id();
    for (const auto &shard_ds : block.shard_blocks_) {
        shard_states.push_back(shard_ds.block_state);
        reload_context->state_root_hashes_.push_back(shard_ds.handle->state());
    }

    auto config_r = block::ConfigInfo::extract_config(
        block.shard_blocks_[0].block_state,
        reload_context->config_block_id_,
        block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries);
    if (config_r.is_error()) {
        LOG(ERROR) << "Failed to extract config: " << config_r.move_as_error();
        stop();
        return;
    }
    auto config = std::shared_ptr<block::ConfigInfo>(config_r.move_as_ok());

    if (options_.account_addresses_ && !options_.account_addresses_->empty()) {
        struct ShardEntry {
            ton::ShardIdFull shard_id;
            ShardStateDataPtr data;
        };
        std::vector<ShardEntry> shard_entries;
        shard_entries.reserve(block.shard_blocks_.size());
        for (std::size_t index = 0; index < block.shard_blocks_.size(); ++index) {
            const auto &shard_ds = block.shard_blocks_[index];
            auto shard_state_data = std::make_shared<ShardStateData>();
            shard_state_data->shard_states_ = shard_states;
            shard_state_data->config_ = config;
            if (!tlb::unpack_cell(shard_ds.block_state, shard_state_data->sstate_)) {
                LOG(ERROR) << "Failed to unpack initial shard state for " << shard_ds.handle->id().to_str();
                continue;
            }
            auto shard_id = ton::ShardIdFull(block::ShardId(shard_state_data->sstate_.shard_id.write()));
            shard_entries.push_back({shard_id, std::move(shard_state_data)});
        }

        std::vector<std::vector<block::StdAddress>> per_shard_addresses(shard_entries.size());
        for (const auto &address : *options_.account_addresses_) {
            bool routed = false;
            for (std::size_t i = 0; i < shard_entries.size(); ++i) {
                if (ton::shard_contains(shard_entries[i].shard_id, ton::extract_addr_prefix(address.workchain, address.addr))) {
                    per_shard_addresses[i].push_back(address);
                    routed = true;
                    break;
                }
            }
            if (!routed) {
                LOG(WARNING) << "Listed account " << address.workchain << ":" << address.addr.to_hex()
                             << " does not belong to any scanned shard, skipping";
            }
        }

        for (std::size_t i = 0; i < shard_entries.size(); ++i) {
            if (per_shard_addresses[i].empty()) {
                continue;
            }
            std::string actor_name = "ALScanner:" + shard_entries[i].shard_id.to_str();
            td::actor::create_actor<AccountListShardScanner>(
                actor_name,
                shard_entries[i].data,
                options_,
                std::move(per_shard_addresses[i])).release();
        }
        return;
    }

    for (std::size_t index = 0; index < block.shard_blocks_.size(); ++index) {
        const auto &shard_ds = block.shard_blocks_[index];
        auto shard_state_data = std::make_shared<ShardStateData>();
        shard_state_data->shard_states_ = shard_states;
        shard_state_data->config_ = config;
        if (!tlb::unpack_cell(shard_ds.block_state, shard_state_data->sstate_)) {
            LOG(ERROR) << "Failed to unpack initial shard state for " << shard_ds.handle->id().to_str();
            continue;
        }
        std::string actor_name = "SScanner:" + shard_ds.handle->id().shard_full().to_str();
        td::actor::create_actor<ShardStateScanner>(
            actor_name,
            db_scanner_,
            index,
            reload_context,
            std::move(shard_state_data),
            options_).release();
    }
}
