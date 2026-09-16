#pragma once

#include "TraceState.h"

#include <map>
#include <set>
#include <string>
#include <vector>

enum class TraceLifecycle {
    RootPending,
    Open,
    AwaitingFinalization,
    Finalized,
    UnknownRoot,
};

struct TraceRetentionConfig {
    double root_pending_seconds{30.0};
    double root_replaced_confirmed_seconds{30.0};
    double open_seconds{300.0};
    double completed_seconds{30.0};
};

TraceLifecycle classify_trace_lifecycle(const TraceState& state,
                                        const std::string& root_node_key);

double trace_retention_seconds(TraceLifecycle lifecycle,
                               const TraceRetentionConfig& config);

bool trace_root_became_real(TraceLifecycle previous,
                            TraceLifecycle current);

bool wallet_external_messages_compete(const std::string& root_account_code_hash);

class CompetingTraceSet {
public:
    void remember(const std::string& account, const std::string& trace_key);
    void forget(const std::string& account, const std::string& trace_key);

    // The accepted trace remains valid. Every other pending candidate for the
    // same account is returned and forgotten.
    std::vector<std::string> accept(const std::string& account,
                                    const std::string& accepted_trace_key);

    bool contains(const std::string& account, const std::string& trace_key) const;

private:
    std::map<std::string, std::set<std::string>> traces_by_account_;
};
