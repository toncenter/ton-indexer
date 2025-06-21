#pragma once
#include <block/block.h>
#include <td/actor/actor.h>
#include <mc-config.h>


using AllShardStates = std::vector<td::Ref<vm::Cell>>;

class VestingContract: public td::actor::Actor {
public:
    struct Result {
        block::StdAddress address;
        uint32_t vesting_start_time;
        uint32_t vesting_total_duration;
        uint32_t unlock_period;
        uint32_t cliff_duration;
        td::RefInt256 vesting_total_amount;
        block::StdAddress vesting_sender_address;
        block::StdAddress owner_address;
        std::vector<block::StdAddress> whitelist;
    };

    VestingContract(block::StdAddress address,
                         td::Ref<vm::Cell> code_cell,
                         td::Ref<vm::Cell> data_cell,
                         AllShardStates shard_states,
                         std::shared_ptr<block::ConfigInfo> config,
                         td::Promise<Result> promise);

    void start_up() override;

private:
    block::StdAddress address_;
    td::Ref<vm::Cell> code_cell_;
    td::Ref<vm::Cell> data_cell_;
    AllShardStates shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    td::Promise<Result> promise_;
};
