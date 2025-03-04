#pragma once
#include <td/actor/actor.h>
#include "TraceEmulator.h"

class TraceInterfaceDetector: public td::actor::Actor {
private:
    AllShardStates shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    Trace trace_;
    td::Promise<Trace> promise_;
public:
    TraceInterfaceDetector(AllShardStates shard_states, std::shared_ptr<block::ConfigInfo> config,
                           Trace trace, td::Promise<Trace> promise) :
        shard_states_(shard_states), config_(config), trace_(std::move(trace)), promise_(std::move(promise)) {
        
    }

    void start_up() override;

private:
    void got_interfaces(block::StdAddress address, std::vector<typename Trace::Detector::DetectedInterface> interfaces, td::Promise<td::Unit> promise);
    void finish(td::Result<td::Unit> status);
};