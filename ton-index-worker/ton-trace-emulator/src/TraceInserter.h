#pragma once

#include "Measurement.h"
#include "TraceEmulator.h"
#include "crypto/common/bitstring.h"
#include "td/actor/actor.h"

#include <sw/redis++/redis++.h>

#include <memory>
#include <string>

struct RedisWriteBatch;

class ITraceInsertManager : public td::actor::Actor {
public:
    virtual void insert(Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) = 0;
};

class RedisInsertManager : public ITraceInsertManager {
    struct Impl;
    std::unique_ptr<Impl> impl_;

    void start_next_writes();
    void schedule_trace(const std::string& trace_key);
    void start_up() override;
    void alarm() override;
    void tear_down() override;

public:
    explicit RedisInsertManager(const std::string& redis_dsn);
    ~RedisInsertManager() override;

    void insert(Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) override;
    void write_finished(std::string trace_key, td::Status status, RedisWriteBatch batch);
};
