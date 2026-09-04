#include <cstdint>
#include <unordered_set>

#include "smc-interfaces/FetchAccountFromShard.h"
#include "smc-interfaces/InterfacesDetector.h"

#include "TraceInterfaceDetector.h"

void TraceInterfaceDetector::start_up() {
    td::MultiPromise mp;
    auto ig = mp.init_guard();
    auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
        td::actor::send_closure(SelfId, &TraceInterfaceDetector::finish, std::move(res));
    });
    ig.add_promise(std::move(P));

    // Detect interfaces for final state of each emulated account
    std::int64_t emulated_detector_tasks = 0;
    std::int64_t committed_detector_tasks = 0;
    for (auto it = trace_.emulated_accounts.rbegin(); it != trace_.emulated_accounts.rend(); it++) {
        const auto& [address, account] = *it;
        if (!emulated_addresses_.insert(address).second) {
          continue;
        }
        trace_.interfaces[address] = {};
        td::actor::create_actor<Trace::Detector>
            ("InterfacesDetector", address, account.code, account.data, shard_states_, config_,
            td::PromiseCreator::lambda([SelfId = actor_id(this), address, promise = ig.get_promise()](td::Result<std::vector<typename Trace::Detector::DetectedInterface>> interfaces) mutable {
                if (interfaces.is_error()) {
                    promise.set_error(interfaces.move_as_error());
                    return;
                }
                td::actor::send_closure(SelfId, &TraceInterfaceDetector::got_interfaces, address, interfaces.move_as_ok(), false, std::move(promise));
            })).release();
        emulated_detector_tasks++;
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
                td::PromiseCreator::lambda([SelfId = actor_id(this), address, promise = ig.get_promise()](td::Result<std::vector<typename Trace::Detector::DetectedInterface>> interfaces) mutable {
                    if (interfaces.is_error()) {
                        promise.set_error(interfaces.move_as_error());
                        return;
                    }
                    td::actor::send_closure(SelfId, &TraceInterfaceDetector::got_interfaces, address, interfaces.move_as_ok(), true, std::move(promise));
                })).release();
        } else {
            // Account is not active, skip interface detection
            LOG(DEBUG) << "Account " << std::to_string(address.workchain) << ":" << address.addr.to_hex() << " is not active, skipping interface detection";
            got_interfaces(address, {}, true, ig.get_promise());
        }
        committed_detector_tasks++;
    }
    if (measurement_) {
        measurement_->set_otel_attribute("ton.interfaces.emulated_accounts_count", emulated_detector_tasks);
        measurement_->set_otel_attribute("ton.interfaces.committed_accounts_count", committed_detector_tasks);
        measurement_->set_otel_attribute("ton.interfaces.detector_tasks_count", emulated_detector_tasks + committed_detector_tasks);
    }
}

void TraceInterfaceDetector::got_interfaces(block::StdAddress address, std::vector<typename Trace::Detector::DetectedInterface> interfaces, bool is_committed, td::Promise<td::Unit> promise) {
    if (is_committed) {
      trace_.committed_interfaces[address] = interfaces;
      if (emulated_addresses_.count(address) == 0) {
        trace_.interfaces[address] = std::move(interfaces);
      }
    } else {
      trace_.interfaces[address] = std::move(interfaces);
    }
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

void TraceUpdateInterfaceDetector::start_up() {
  if (update_.empty()) {
    promise_.set_value(std::move(update_));
    stop();
    return;
  }

  if (update_.measurement) {
    update_.measurement->start_otel_child_span("detect_interfaces");
  }

  remaining_ = update_.size();
  for (std::size_t index = 0; index < update_.fragments.size(); ++index) {
    auto promise = td::PromiseCreator::lambda([SelfId = actor_id(this), index](td::Result<Trace> result) mutable {
      td::actor::send_closure(SelfId, &TraceUpdateInterfaceDetector::fragment_finished, index, std::move(result));
    });
    td::actor::create_actor<TraceInterfaceDetector>("TraceFragmentInterfaceDetector", shard_states_, config_,
                                                    std::move(update_.fragments[index]), std::move(promise),
                                                    MeasurementPtr{})
        .release();
  }
}

void TraceUpdateInterfaceDetector::fragment_finished(std::size_t index, td::Result<Trace> result) {
  CHECK(remaining_ > 0);
  if (result.is_error()) {
    auto error = result.move_as_error();
    if (!first_error_) {
      first_error_.emplace(std::move(error));
    }
  } else {
    update_.fragments[index] = result.move_as_ok();
  }

  --remaining_;
  if (remaining_ == 0) {
    finish();
  }
}

void TraceUpdateInterfaceDetector::finish() {
  if (update_.measurement) {
    if (first_error_) {
      update_.measurement->mark_otel_error("trace_emulator.interface_error", first_error_->to_string());
    } else {
      std::int64_t emulated_detector_tasks = 0;
      std::int64_t committed_detector_tasks = 0;
      for (const auto& trace : update_.fragments) {
        std::unordered_set<block::StdAddress> emulated_addresses;
        for (const auto& [address, _] : trace.emulated_accounts) {
          emulated_addresses.insert(address);
        }
        emulated_detector_tasks += static_cast<std::int64_t>(emulated_addresses.size());
        committed_detector_tasks += static_cast<std::int64_t>(trace.committed_accounts.size());
      }
      update_.measurement->set_otel_attribute("ton.interfaces.emulated_accounts_count", emulated_detector_tasks);
      update_.measurement->set_otel_attribute("ton.interfaces.committed_accounts_count", committed_detector_tasks);
      update_.measurement->set_otel_attribute("ton.interfaces.detector_tasks_count",
                                              emulated_detector_tasks + committed_detector_tasks);
    }
    update_.measurement->end_otel_child_span("detect_interfaces");
  }
  if (first_error_) {
    promise_.set_error(std::move(*first_error_));
  } else {
    promise_.set_value(std::move(update_));
  }
  stop();
}
