#pragma once
#include <td/actor/actor.h>
#include <tdutils/td/utils/JsonBuilder.h>
#include <tl/generate/auto/tl/ton_api_json.h>
#include <dht/dht.hpp>
#include <overlay/overlays.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include "IndexData.h"
#include "TraceEmulator.h"


class OverlayListener : public td::actor::Actor {
private:
    std::string global_config_path_;
    std::string inet_addr_;
    std::function<void(std::unique_ptr<Trace>)> trace_processor_;

    td::actor::ActorOwn<ton::overlay::Overlays> overlays_;
    td::actor::ActorOwn<ton::adnl::Adnl> adnl_;
    td::actor::ActorOwn<ton::keyring::Keyring> keyring_;
    td::actor::ActorOwn<ton::dht::Dht> dht_;
    td::actor::ActorOwn<ton::adnl::AdnlNetworkManager> adnl_network_manager_;

    std::unordered_set<td::Bits256, BitArrayHasher> known_ext_msgs_; // this set grows infinitely. TODO: remove old messages
    std::vector<td::Ref<vm::Cell>> shard_states_;
    std::shared_ptr<emulator::TransactionEmulator> emulator_;
    std::shared_ptr<block::ConfigInfo> config_;

    int traces_cnt_{0};

    void process_external_message(td::Ref<ton::validator::ExtMessageQ> message);
    void trace_error(TraceId trace_id, td::Status error);
    void trace_received(TraceId trace_id, Trace *trace);
    void trace_interfaces_error(TraceId trace_id, td::Status error);
    void finish_processing(std::unique_ptr<Trace> trace);

public:
    OverlayListener(std::string global_config_path, std::string inet_addr, std::function<void(std::unique_ptr<Trace>)> trace_processor)
        : global_config_path_(std::move(global_config_path)), inet_addr_(std::move(inet_addr)), trace_processor_(std::move(trace_processor)) {};

    virtual void start_up() override;

    void set_mc_data_state(MasterchainBlockDataState mc_data_state) {
        for (const auto& shard_state : mc_data_state.shard_blocks_) {
            shard_states_.push_back(shard_state.block_state);
        }

        auto libraries_root = mc_data_state.config_->get_libraries_root();
        emulator_ = std::make_shared<emulator::TransactionEmulator>(mc_data_state.config_, 0);
        emulator_->set_libs(vm::Dictionary(libraries_root, 256));

        config_ = mc_data_state.config_;
    }
};