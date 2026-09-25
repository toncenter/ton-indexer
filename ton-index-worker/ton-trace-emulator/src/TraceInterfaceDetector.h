#pragma once
#include <optional>
#include <td/actor/actor.h>
#include <unordered_set>

#include "TraceEmulator.h"
#include "TraceUpdate.h"

struct DetectedAccounts {
    std::unordered_map<block::StdAddress, block::Account> states;
    std::unordered_map<block::StdAddress, std::vector<Trace::Detector::DetectedInterface>> interfaces;
};

// Detect committed account states without constructing a trace tree.
class AccountStatesDetector : public td::actor::Actor {
    AllShardStates shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    std::unordered_set<block::StdAddress> addresses_;
    DetectedAccounts accounts_;
    td::Promise<DetectedAccounts> promise_;
    void start_up() override;
    void got_interfaces(block::StdAddress address, std::vector<Trace::Detector::DetectedInterface> interfaces,
                        td::Promise<td::Unit> promise);
    void finish(td::Result<td::Unit> result);

public:
    AccountStatesDetector(AllShardStates states, std::shared_ptr<block::ConfigInfo> config,
                          std::unordered_set<block::StdAddress> addresses, td::Promise<DetectedAccounts> promise)
        : shard_states_(std::move(states)), config_(std::move(config)), addresses_(std::move(addresses)),
          promise_(std::move(promise)) {}
};

class TraceInterfaceDetector: public td::actor::Actor {
private:
    void got_committed_accounts(DetectedAccounts accounts, td::Promise<td::Unit> promise);
    AllShardStates shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    Trace trace_;
    td::Promise<Trace> promise_;
    MeasurementPtr measurement_;
    std::unordered_set<block::StdAddress> emulated_addresses_;

   public:
    TraceInterfaceDetector(AllShardStates shard_states, std::shared_ptr<block::ConfigInfo> config,
                           Trace trace, td::Promise<Trace> promise, const MeasurementPtr& measurement) :
        shard_states_(shard_states), config_(config), trace_(std::move(trace)), promise_(std::move(promise)), measurement_(measurement) {}

    void start_up() override;
private:
    void got_emulated_interfaces(block::StdAddress address,
        std::vector<typename Trace::Detector::DetectedInterface> interfaces, td::Promise<td::Unit> promise);
    void finish(td::Result<td::Unit> status);
};

// Runs fragment-local interface detectors while keeping the logical trace as
// one pipeline item and one telemetry span.
class TraceUpdateInterfaceDetector : public td::actor::Actor {
 private:
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;
  TraceUpdate update_;
  td::Promise<TraceUpdate> promise_;
  std::size_t remaining_{0};
  std::optional<td::Status> first_error_;

  void start_up() override;
  void fragment_finished(std::size_t index, td::Result<Trace> result);
  void finish();

 public:
  TraceUpdateInterfaceDetector(AllShardStates shard_states, std::shared_ptr<block::ConfigInfo> config,
                               TraceUpdate update, td::Promise<TraceUpdate> promise)
      : shard_states_(std::move(shard_states))
      , config_(std::move(config))
      , update_(std::move(update))
      , promise_(std::move(promise)) {
  }
};
