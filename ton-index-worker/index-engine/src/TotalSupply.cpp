#include "TotalSupply.h"

#include <map>

#include "validator/impl/shard.hpp"

namespace gramstat {

td::Result<schema::TotalSupply> make_total_supply(
    std::uint32_t mc_seqno, std::uint32_t gen_utime,
    const ton::validator::GlobalBalanceComponents& c) {
    for (const auto& amount : {c.total, c.accounts, c.dispatch_queue,
                              c.unprocessed_messages, c.unrecovered_validator_fees}) {
        if (amount.is_null() || !amount->is_valid() || amount->sgn() < 0 ||
            amount->to_dec_string().size() > 38) {
            return td::Status::Error("Total supply amount is not a nonnegative Decimal(38, 0)");
        }
    }
    if (td::cmp(c.total, c.accounts + c.dispatch_queue + c.unprocessed_messages + c.unrecovered_validator_fees) != 0) {
        return td::Status::Error("Total supply components do not add up to total");
    }
    // c.burned is diagnostic ValueFlow, not another deduction from this snapshot.
    return schema::TotalSupply{mc_seqno, gen_utime, c.total, c.accounts, c.dispatch_queue,
                               c.unprocessed_messages, c.unrecovered_validator_fees};
}

td::actor::Task<schema::TotalSupply> calculate_total_supply(
    schema::MasterchainBlockDataState blocks, StateLoader load_state) {
    using namespace ton::validator;
    const schema::BlockDataState* mc = nullptr;
    std::map<ton::BlockIdExt, const schema::BlockDataState*> tops;
    for (const auto& entry : blocks.shard_blocks_) {
        if (entry.block_data.is_null() || entry.block_state.is_null()) {
            co_return td::Status::Error("Total supply requires block data and state for every shard tip");
        }
        auto id = entry.block_data->block_id();
        if (id.is_masterchain()) {
            if (mc != nullptr) {
                co_return td::Status::Error("Duplicate total supply masterchain state");
            }
            mc = &entry;
        } else if (!tops.emplace(id, &entry).second) {
            co_return td::Status::Error("Duplicate total supply shard tip");
        }
    }
    if (mc == nullptr || !load_state) {
        co_return td::Status::Error("Total supply requires a masterchain state and state loader");
    }

    auto mc_state = co_await MasterchainStateQ::fetch(mc->block_data->block_id(), {}, mc->block_state);
    std::vector<GlobalBalanceShardState> shards;
    for (const auto& desc : mc_state->get_shards()) {
        auto id = desc->top_block_id();
        auto it = tops.find(id);
        if (it == tops.end()) {
            if (id.seqno() != 0) {
                co_return td::Status::Error("Total supply is missing a current shard tip");
            }
            auto state = co_await load_state(id);
            if (state.is_null() || state->get_block_id() != id) {
                co_return td::Status::Error("Total supply zerostate loader returned a different block");
            }
            shards.push_back({std::move(state), {}});
        } else {
            const auto& entry = *it->second;
            auto state = co_await ShardStateQ::fetch(id, {}, entry.block_state);
            shards.push_back({std::move(state), entry.block_data->root_cell()});
            tops.erase(it);
        }
    }
    if (!tops.empty()) {
        co_return td::Status::Error("Total supply contains a shard outside the masterchain snapshot");
    }

    auto load_aux = [load_state = std::move(load_state)](ton::BlockIdExt id)
        -> td::actor::Task<td::Ref<MasterchainState>> {
        auto state = co_await load_state(id);
        if (state.is_null() || state->get_block_id() != id || !id.is_masterchain()) {
            co_return td::Status::Error("Total supply auxiliary loader returned a different block");
        }
        td::Ref<MasterchainState> mc_state{state};
        if (mc_state.is_null()) {
            co_return td::Status::Error("Total supply auxiliary state is not a masterchain state");
        }
        co_return mc_state;
    };
    auto components = co_await calculate_global_balance_at_anchor(
        mc_state, mc->block_data->root_cell(), std::move(shards), std::move(load_aux));
    co_return make_total_supply(mc_state->get_seqno(), mc_state->get_unix_time(), components);
}

}  // namespace gramstat
