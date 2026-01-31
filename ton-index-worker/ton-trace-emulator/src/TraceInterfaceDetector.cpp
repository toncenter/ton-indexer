#include "TraceInterfaceDetector.h"
#include "smc-interfaces/InterfacesDetector.h"
#include "smc-interfaces/FetchAccountFromShard.h"

void TraceInterfaceDetector::start_up() {
    measurement_->measure_step("trace_interface_detector__start");
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
                td::actor::send_closure(SelfId, &TraceInterfaceDetector::got_interfaces, address, std::move(interfaces), false, std::move(promise));
        })).release();
    }

    // For committed accounts fetch block::Account and detect interfaces
    for (const auto& address : trace_.get_addresses(true)) {
        std::optional<block::Account> account_state;
        
        for (const auto& shard_state : shard_states_) {
            block::gen::ShardStateUnsplit::Record sstate;
            if (!tlb::unpack_cell(shard_state, sstate)) {
                continue;
            }

            if (!ton::shard_contains(ton::ShardIdFull(block::ShardId(sstate.shard_id)),
                    ton::extract_addr_prefix(address.workchain, address.addr))) {
                continue;
            }

            vm::AugmentedDictionary accounts_dict(vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts);
            account_state = block::Account(address.workchain, address.addr.cbits());
            auto account_cell = accounts_dict.lookup(address.addr);

            if (account_cell.is_null()) {
                if (!account_state->init_new(sstate.gen_utime)) {
                    LOG(ERROR) << "Failed to initialize new account for " << std::to_string(address.workchain) << ":" << address.addr.to_hex();
                    continue;
                }
            } else {
                if (!account_state->unpack(std::move(account_cell), sstate.gen_utime,
                            address.workchain == ton::masterchainId && config_->is_special_smartcontract(address.addr))) {
                    LOG(ERROR) << "Failed to unpack account for " << std::to_string(address.workchain) << ":" << address.addr.to_hex();
                    continue;
                }
            }
            break;
        }

        if (!account_state) {
            LOG(ERROR) << "Account " << std::to_string(address.workchain) << ":" << address.addr.to_hex() << " not found in shard states";
            continue;
        }

        trace_.committed_accounts[address] = *account_state;

        if (account_state->status == block::Account::acc_active && account_state->code.not_null() && account_state->data.not_null()) {
            td::actor::create_actor<Trace::Detector>("InterfacesDetector", address, account_state->code, account_state->data, shard_states_, config_,
                td::PromiseCreator::lambda([SelfId = actor_id(this), address, promise = ig.get_promise()](std::vector<typename Trace::Detector::DetectedInterface> interfaces) mutable {
                    td::actor::send_closure(SelfId, &TraceInterfaceDetector::got_interfaces, address, std::move(interfaces), true, std::move(promise));
            })).release();
        } else {
            // Account is not active, skip interface detection
            LOG(DEBUG) << "Account " << std::to_string(address.workchain) << ":" << address.addr.to_hex() << " is not active, skipping interface detection";
            got_interfaces(address, {}, true, ig.get_promise());
        }
    }
}

void TraceInterfaceDetector::got_interfaces(block::StdAddress address, std::vector<typename Trace::Detector::DetectedInterface> interfaces, bool is_committed, td::Promise<td::Unit> promise) {
    trace_.interfaces[address] = std::move(interfaces);
    if (is_committed) {
        trace_.committed_interfaces[address] = trace_.interfaces[address];
    }
    measurement_->measure_step("trace_interface_detector__complete");
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