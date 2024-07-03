#pragma once
#include <sw/redis++/redis++.h>
#include "td/actor/actor.h"
#include "crypto/common/bitstring.h"
#include "TraceEmulator.h"


class TraceInserter: public td::actor::Actor {
private:
    sw::redis::Redis redis_;
    std::unique_ptr<Trace> trace_;
    td::Promise<td::Unit> promise_;

public:
    inline static std::string redis_uri = "tcp://127.0.0.1:6379";

    TraceInserter(std::unique_ptr<Trace> trace, td::Promise<td::Unit> promise) :
        redis_(sw::redis::Redis(redis_uri)), trace_(std::move(trace)), promise_(std::move(promise)) {
    }

    void start_up() override;
    void delete_db_subtree(std::string key);
};