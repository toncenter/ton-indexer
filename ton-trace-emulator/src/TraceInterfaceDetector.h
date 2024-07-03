#pragma once
#include <td/actor/actor.h>
#include "TraceEmulator.h"

class TraceInterfaceDetector: public td::actor::Actor {
private:
    AllShardStates shard_states_;
    std::shared_ptr<block::ConfigInfo> config_;
    std::unique_ptr<Trace> trace_;
    td::Promise<std::unique_ptr<Trace>> promise_;
public:
    TraceInterfaceDetector(AllShardStates shard_states, std::shared_ptr<block::ConfigInfo> config,
                        std::unique_ptr<Trace> trace, td::Promise<std::unique_ptr<Trace>> promise) :
        shard_states_(shard_states), config_(config), trace_(std::move(trace)), promise_(std::move(promise)) {
        
    }

    void start_up() override;

private:
    void finish(td::Result<td::Unit> status);
};