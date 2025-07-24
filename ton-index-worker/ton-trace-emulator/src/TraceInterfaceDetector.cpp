#include "TraceInterfaceDetector.h"
#include "smc-interfaces/InterfacesDetector.h"
#include "smc-interfaces/FetchAccountFromShard.h"

void TraceInterfaceDetector::start_up() {
    td::MultiPromise mp;
    auto ig = mp.init_guard();
    auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
        td::actor::send_closure(SelfId, &TraceInterfaceDetector::finish, std::move(res));
    });
    ig.add_promise(std::move(P));

    // Detect interfaces for final state of each emulated account
    std::unordered_set<block::StdAddress> processed_addresses;
    for (auto it = trace_.emulated_accounts.rbegin(); it != trace_.emulated_accounts.rend(); it++) {
        const auto& [address, account] = *it;
        if (processed_addresses.count(address)) {
            continue;
        }
        processed_addresses.insert(address);
        trace_.interfaces[address] = {};
        td::actor::create_actor<Trace::Detector>
            ("InterfacesDetector", address, account.code, account.data, shard_states_, config_, 
            td::PromiseCreator::lambda([SelfId = actor_id(this), address, promise = ig.get_promise()](std::vector<typename Trace::Detector::DetectedInterface> interfaces) mutable {
                td::actor::send_closure(SelfId, &TraceInterfaceDetector::got_interfaces, address, std::move(interfaces), std::move(promise));
        })).release();
    }
    // Then detect interfaces for all the rest accounts in trace (non-emulated)
    for (const auto& address : trace_.get_addresses()) {
        if (processed_addresses.count(address)) {
            continue;
        }
        processed_addresses.insert(address);
        trace_.interfaces[address] = {};
        auto P = td::PromiseCreator::lambda([SelfId=actor_id(this), address, shard_states=shard_states_, 
                config=config_, promise = ig.get_promise()](td::Result<schema::AccountState> res) mutable {
            if (res.is_error()) {
                // probably account is uninit, so we just skip it
                promise.set_value(td::Unit());
                return;
            }
            auto account_state = res.move_as_ok();
            td::actor::create_actor<Trace::Detector>
                ("InterfacesDetector", address, account_state.code, account_state.data, shard_states, config,
                td::PromiseCreator::lambda([SelfId, address, promise = std::move(promise)](std::vector<typename Trace::Detector::DetectedInterface> interfaces) mutable {
                    td::actor::send_closure(SelfId, &TraceInterfaceDetector::got_interfaces, address, std::move(interfaces), std::move(promise));
            })).release();
        });

        td::actor::create_actor<FetchAccountFromShardV2>("fetchaccountfromshard", shard_states_, address, std::move(P)).release();
    }
    
}

void TraceInterfaceDetector::got_interfaces(block::StdAddress address, std::vector<typename Trace::Detector::DetectedInterface> interfaces, td::Promise<td::Unit> promise) {
    trace_.interfaces[address] = std::move(interfaces);
    promise.set_value(td::Unit());
}

void TraceInterfaceDetector::finish(td::Result<td::Unit> status) {
    if (status.is_error()) {
        promise_.set_error(status.move_as_error_prefix("Failed to detect interfaces: "));
    } else {
        promise_.set_value(std::move(trace_));
    }
    stop();
}