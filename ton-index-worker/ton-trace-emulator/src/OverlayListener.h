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
    std::function<void(Trace, td::Promise<td::Unit>)> trace_processor_;

    td::actor::ActorOwn<ton::overlay::Overlays> overlays_;
    td::actor::ActorOwn<ton::adnl::Adnl> adnl_;
    td::actor::ActorOwn<ton::keyring::Keyring> keyring_;
    td::actor::ActorOwn<ton::dht::Dht> dht_;
    td::actor::ActorOwn<ton::adnl::AdnlNetworkManager> adnl_network_manager_;

    MasterchainBlockDataState mc_data_state_;
    std::unordered_set<td::Bits256> known_ext_msgs_;
    
    int traces_cnt_{0};

    void process_external_message(td::Ref<ton::validator::ExtMessageQ> message);
    void trace_error(td::Bits256 ext_in_msg_hash, td::Status error);
    void trace_received(Trace trace);
    void trace_interfaces_error(td::Bits256 ext_in_msg_hash, td::Status error);
    void finish_processing(Trace trace);

public:
    OverlayListener(std::string global_config_path, std::string inet_addr, std::function<void(Trace, td::Promise<td::Unit>)> trace_processor)
        : global_config_path_(std::move(global_config_path)), inet_addr_(std::move(inet_addr)), trace_processor_(std::move(trace_processor)) {};

    virtual void start_up() override;

    void set_mc_data_state(MasterchainBlockDataState mc_data_state) {
        mc_data_state_ = std::move(mc_data_state);
        known_ext_msgs_.clear();
    }
};