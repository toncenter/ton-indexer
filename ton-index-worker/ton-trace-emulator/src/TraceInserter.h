#pragma once

#include "Measurement.h"
#include "TraceEmulator.h"
#include "TraceLifecycle.h"
#include "crypto/common/bitstring.h"
#include "td/actor/actor.h"
#include "td/utils/Status.h"

#include <sw/redis++/redis++.h>

#include <memory>
#include <string>
#include <vector>

struct RedisWriteBatch;

enum class TraceCleanupMode {
    Retention,
    PendingTimeout,
    Invalidation,
};

// Clears the entire logical Redis database selected by redis_dsn.
td::Status flush_pending_redis_database(const std::string& redis_dsn);

class ITraceInsertManager : public td::actor::Actor {
public:
    virtual void insert(Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) = 0;
    virtual void invalidate(std::vector<td::Bits256> trace_hashes) = 0;
};

class RedisInsertManager : public ITraceInsertManager {
    struct Impl;
    std::unique_ptr<Impl> impl_;

    void start_next_writes();
    void schedule_trace(const std::string& trace_key);
    void request_cleanup(const std::string& trace_key, TraceCleanupMode mode);
    void update_lifecycle(const std::string& trace_key);
    void start_up() override;
    void alarm() override;
    void tear_down() override;

public:
    RedisInsertManager(const std::string& redis_dsn, TraceRetentionConfig retention);
    ~RedisInsertManager() override;

    void insert(Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) override;
    void invalidate(std::vector<td::Bits256> trace_hashes) override;
    void write_finished(std::string trace_key, td::Status status, RedisWriteBatch batch);
};
