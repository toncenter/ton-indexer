#pragma once
#include <sw/redis++/redis++.h>
#include "td/actor/actor.h"
#include "crypto/common/bitstring.h"
#include "TraceEmulator.h"


class ITraceInsertManager : public td::actor::Actor {
public:
    virtual void insert(Trace trace, td::Promise<td::Unit> promise) = 0;
};

class RedisInsertManager: public ITraceInsertManager {
private:
    sw::redis::Redis redis_;

public:
    RedisInsertManager(std::string redis_dsn) :
        redis_(sw::redis::Redis(redis_dsn)) {}

    void insert(Trace trace, td::Promise<td::Unit> promise);
};