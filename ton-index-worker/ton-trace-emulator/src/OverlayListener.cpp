
#include "OverlayListener.h"
#include <tdutils/td/utils/filesystem.h>
#include "TraceInterfaceDetector.h"


void OverlayListener::start_up() {
    auto pk = ton::PrivateKey{ton::privkeys::Ed25519::random()};
    auto pub = pk.compute_public_key();
    keyring_ = ton::keyring::Keyring::create("");
    td::actor::send_closure(keyring_, &ton::keyring::Keyring::add_key, std::move(pk), true, [](td::Unit) {});

    adnl_ = ton::adnl::Adnl::create("", keyring_.get());

    class Callback : public ton::overlay::Overlays::Callback {
    private:
        td::actor::ActorId<OverlayListener> parent_;
    public:
        Callback(td::actor::ActorId<OverlayListener> parent) : parent_(parent) {};

        void receive_message(ton::adnl::AdnlNodeIdShort src, ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data) override {
        }
        void receive_query(ton::adnl::AdnlNodeIdShort src, ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                        td::Promise<td::BufferSlice> promise) override {
            auto B = ton::fetch_tl_object<ton::ton_api::tonNode_getCapabilities>(std::move(data), true);
            if (B.is_error()) {
                promise.set_error(td::Status::Error("not implemented"));
                return;
            }
            
            promise.set_value(ton::create_serialize_tl_object<ton::ton_api::tonNode_capabilities>(0, 0, 0));
        }
        void receive_broadcast(ton::PublicKeyHash src, ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data) override {
            auto B = ton::fetch_tl_object<ton::ton_api::tonNode_externalMessageBroadcast>(std::move(data), true);
            if (B.is_error()) {
                LOG(WARNING) << "Failed to fetch externalMessageBroadcast";
                return;
            }
            auto msg_data = std::move(B.move_as_ok()->message_->data_);
            auto message_r = ton::validator::ExtMessageQ::create_ext_message(std::move(msg_data), block::SizeLimitsConfig::ExtMsgLimits{});
            if (message_r.is_ok()) {
                auto message = message_r.move_as_ok();
                td::actor::send_closure(parent_, &OverlayListener::process_external_message, std::move(message));
            }
        }
        void check_broadcast(ton::PublicKeyHash src, ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                            td::Promise<td::Unit> promise) override {
            auto B = ton::fetch_tl_object<ton::ton_api::tonNode_externalMessageBroadcast>(std::move(data), true);
            if (B.is_error()) {
                LOG(WARNING) << "Failed to fetch externalMessageBroadcast";
                promise.set_error(td::Status::Error("Failed to fetch externalMessageBroadcast"));
                return;
            }

            promise.set_value(td::Unit());
        }
    };
    auto conf_data = td::read_file(global_config_path_).move_as_ok();
    auto conf_json = td::json_decode(conf_data.as_slice()).move_as_ok();
    ton::ton_api::config_global conf;
    if (ton::ton_api::from_json(conf, conf_json.get_object()).is_error()) {
        LOG(ERROR) << "Failed to parse global config";
        return;
    }
    if (!conf.dht_) {
        LOG(ERROR) << "does not contain [dht] section";
        return;
    }
    auto dht_config = ton::dht::Dht::create_global_config(std::move(conf.dht_)).move_as_ok();

    td::Bits256 zero_state_file_hash = conf.validator_->zero_state_->file_hash_;
    auto X = ton::create_hash_tl_object<ton::ton_api::tonNode_shardPublicOverlayId>(0, ton::shardIdAll, zero_state_file_hash);
    td::BufferSlice b{32};
    b.as_slice().copy_from(as_slice(X));
    auto overlay_id_full = ton::overlay::OverlayIdFull{std::move(b)};
    auto overlay_id = overlay_id_full.compute_short_id();
    auto rules = ton::overlay::OverlayPrivacyRules{ton::overlay::Overlays::max_fec_broadcast_size()};

    td::IPAddress addr;
    if (addr.init_host_port(inet_addr_).is_error()) {
        LOG(ERROR) << "Failed to parse inet_addr: " << inet_addr_ << ". OverlayListener stopped.";
        return;
    }
    adnl_network_manager_ = ton::adnl::AdnlNetworkManager::create(td::narrow_cast<td::uint16>(addr.get_port()));
    ton::adnl::AdnlCategoryMask cat_mask;
    cat_mask[0] = true;
    td::actor::send_closure(adnl_network_manager_, &ton::adnl::AdnlNetworkManager::add_self_addr, addr, std::move(cat_mask), 0);


    td::actor::send_closure(adnl_, &ton::adnl::Adnl::register_network_manager, adnl_network_manager_.get());

    ton::adnl::AdnlAddress x = ton::adnl::AdnlAddressImpl::create(
        ton::create_tl_object<ton::ton_api::adnl_address_udp>(addr.get_ipv4(), addr.get_port()));
    ton::adnl::AdnlAddressList addr_list;
    addr_list.add_addr(x);
    addr_list.set_version(static_cast<td::int32>(td::Clocks::system()));
    addr_list.set_reinit_date(ton::adnl::Adnl::adnl_start_time());
    td::actor::send_closure(adnl_, &ton::adnl::Adnl::add_id, ton::adnl::AdnlNodeIdFull{pub}, std::move(addr_list),
                        static_cast<td::uint8>(0));

    dht_ = ton::dht::Dht::create(ton::adnl::AdnlNodeIdShort{pub.compute_short_id()}, "",
            dht_config, keyring_.get(), adnl_.get()).move_as_ok();

    td::actor::send_closure(adnl_, &ton::adnl::Adnl::register_dht_node, dht_.get());

    overlays_ = ton::overlay::Overlays::create("", keyring_.get(), adnl_.get(), dht_.get());
    td::actor::send_closure(overlays_, &ton::overlay::Overlays::create_public_overlay, ton::adnl::AdnlNodeIdShort{pub.compute_short_id()}, overlay_id_full.clone(),
                            std::make_unique<Callback>(actor_id(this)), rules, PSTRING() << "{ \"type\": \"shard\", \"shard_id\": " << ton::shardIdAll << ", \"workchain_id\": " << 0 << " }");
}

void OverlayListener::process_external_message(td::Ref<ton::validator::ExtMessageQ> message) {
    if (mc_data_state_.config_ == nullptr) {
        return;
    }
    auto msg_hash = TraceId{message->root_cell()->get_hash().bits()};
    if (!known_ext_msgs_.insert(msg_hash).second) {
        return;
    }

    LOG(DEBUG) << "Starting processing ExtMessageQ hash: " << td::base64_encode(msg_hash.as_slice()) << " addr: " << message->wc() << ":" << message->addr().to_hex();

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), msg_hash](td::Result<Trace> R) mutable {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &OverlayListener::trace_error, std::move(msg_hash), R.move_as_error());
        } else {
            td::actor::send_closure(SelfId, &OverlayListener::trace_received, R.move_as_ok());
        }
    });
    td::actor::create_actor<TraceEmulator>("TraceEmu", mc_data_state_, message->root_cell(), false, std::move(P)).release();

    g_statistics.record_count(EMULATE_SRC_OVERLAY);
}

void OverlayListener::trace_error(td::Bits256 ext_in_msg_hash, td::Status error) {
    LOG(ERROR) << "Failed to emulate trace from msg " << td::base64_encode(ext_in_msg_hash.as_slice()) << ": " << error;
    known_ext_msgs_.erase(ext_in_msg_hash);
}

void OverlayListener::trace_received(Trace trace) {
    LOG(INFO) << "Emulated trace from msg " << td::base64_encode(trace.ext_in_msg_hash.as_slice()) << ": " 
        << trace.transactions_count() << " transactions, " << trace.depth() << " depth";
    if constexpr (std::variant_size_v<Trace::Detector::DetectedInterface> > 0) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), ext_in_msg_hash = trace.ext_in_msg_hash](td::Result<Trace> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &OverlayListener::trace_interfaces_error, ext_in_msg_hash, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &OverlayListener::finish_processing, R.move_as_ok());
        });

        std::vector<td::Ref<vm::Cell>> shard_states;
        for (const auto& shard_state : mc_data_state_.shard_blocks_) {
            shard_states.push_back(shard_state.block_state);
        }

        td::actor::create_actor<TraceInterfaceDetector>("TraceInterfaceDetector", std::move(shard_states), mc_data_state_.config_, std::move(trace), std::move(P)).release();
    } else {
        finish_processing(std::move(trace));
    }
}

void OverlayListener::trace_interfaces_error(td::Bits256 ext_in_msg_hash, td::Status error) {
    LOG(ERROR) << "Failed to detect interfaces on trace from msg " << td::base64_encode(ext_in_msg_hash.as_slice()) << ": " << error;
}

void OverlayListener::finish_processing(Trace trace) {
    auto P = td::PromiseCreator::lambda([ext_in_msg_hash = trace.ext_in_msg_hash](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to insert trace from msg " << td::base64_encode(ext_in_msg_hash.as_slice()) << ": " << R.move_as_error();
            return;
        }
        LOG(DEBUG) << "Successfully inserted trace from msg " << td::base64_encode(ext_in_msg_hash.as_slice());
    });
    trace_processor_(std::move(trace), std::move(P));
    traces_cnt_++;
}
