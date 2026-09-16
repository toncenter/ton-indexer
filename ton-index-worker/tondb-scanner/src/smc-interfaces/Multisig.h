#pragma once
#include <block/block.h>
#include <td/actor/actor.h>
#include <mc-config.h>

using AllShardStates = std::vector<td::Ref<vm::Cell>>;


class MultisigContract: public td::actor::Actor {
public:
    struct Result {
        block::StdAddress address;
        td::RefInt256 next_order_seqno;
        uint32_t threshold;
        std::vector<block::StdAddress> signers;
        std::vector<block::StdAddress> proposers;
    };

    MultisigContract(block::StdAddress address,
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

class MultisigOrder: public td::actor::Actor {
public:
    struct Result {
        block::StdAddress address;
        block::StdAddress multisig_address;
        td::RefInt256 order_seqno;
        uint32_t threshold;
        bool sent_for_execution;
        std::vector<block::StdAddress> signers;
        td::RefInt256 approvals_mask;
        uint32_t approvals_num;
        td::RefInt256 expiration_date;
        td::Ref<vm::Cell> order;
    };

    MultisigOrder(block::StdAddress address,
                         td::Ref<vm::Cell> code_cell,
                         td::Ref<vm::Cell> data_cell,
                         AllShardStates shard_states,
                         std::shared_ptr<block::ConfigInfo> config,
                         td::Promise<Result> promise);

    void start_up() override;

    static td::Result<Result> detect(const block::StdAddress& address,
                                     const td::Ref<vm::Cell>& code_cell,
                                     const td::Ref<vm::Cell>& data_cell,
                                     const AllShardStates& shard_states,
                                     const std::shared_ptr<block::ConfigInfo>& config);

    static td::Status verify_multisig_order(const block::StdAddress& order_address,
        block::StdAddress multisig_address, td::Ref<vm::Cell> multisig_code,
        td::Ref<vm::Cell> multisig_data, td::RefInt256 order_seqno,
        const std::shared_ptr<block::ConfigInfo>& config);

private:
    block::StdAddress address_;
    td::Ref<vm::Cell> code_cell_;
    td::Ref<vm::Cell> data_cell_;
    AllShardStates shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    td::Promise<Result> promise_;
};