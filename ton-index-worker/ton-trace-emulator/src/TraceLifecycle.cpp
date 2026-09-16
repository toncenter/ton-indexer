#include "TraceLifecycle.h"

namespace {

constexpr const char* kHighloadWalletV3CodeHash =
    "EayteVWEQJDyg78ji8FEmHH3g+fMCXlAjT9IWUg+hSU=";

}  // namespace

TraceLifecycle classify_trace_lifecycle(const TraceState& state,
                                        const std::string& root_node_key) {
    const auto* root = state.find(root_node_key);
    if (!root) {
        return TraceLifecycle::UnknownRoot;
    }
    if (root->finality == TraceStateFinality::Emulated) {
        return TraceLifecycle::RootPending;
    }

    bool has_emulated = false;
    bool has_confirmed = false;
    for (const auto& [_, node] : state.nodes()) {
        has_emulated = has_emulated || node.finality == TraceStateFinality::Emulated;
        has_confirmed = has_confirmed || node.finality == TraceStateFinality::Confirmed;
    }
    if (has_emulated) {
        return TraceLifecycle::Open;
    }
    if (has_confirmed) {
        return TraceLifecycle::AwaitingFinalization;
    }
    return TraceLifecycle::Finalized;
}

double trace_retention_seconds(TraceLifecycle lifecycle,
                               const TraceRetentionConfig& config) {
    switch (lifecycle) {
        case TraceLifecycle::RootPending:
            return config.root_pending_seconds;
        case TraceLifecycle::Open:
        case TraceLifecycle::UnknownRoot:
            return config.open_seconds;
        case TraceLifecycle::AwaitingFinalization:
        case TraceLifecycle::Finalized:
            return config.completed_seconds;
    }
    return config.open_seconds;
}

bool trace_root_became_real(TraceLifecycle previous,
                            TraceLifecycle current) {
    const auto has_real_root = [](TraceLifecycle lifecycle) {
        return lifecycle == TraceLifecycle::Open ||
               lifecycle == TraceLifecycle::AwaitingFinalization ||
               lifecycle == TraceLifecycle::Finalized;
    };
    return !has_real_root(previous) && has_real_root(current);
}

bool wallet_external_messages_compete(const std::string& root_account_code_hash) {
    return root_account_code_hash != kHighloadWalletV3CodeHash;
}

void CompetingTraceSet::remember(const std::string& account,
                                 const std::string& trace_key) {
    traces_by_account_[account].insert(trace_key);
}

void CompetingTraceSet::forget(const std::string& account,
                               const std::string& trace_key) {
    auto it = traces_by_account_.find(account);
    if (it == traces_by_account_.end()) {
        return;
    }
    it->second.erase(trace_key);
    if (it->second.empty()) {
        traces_by_account_.erase(it);
    }
}

std::vector<std::string> CompetingTraceSet::accept(
    const std::string& account,
    const std::string& accepted_trace_key) {
    auto it = traces_by_account_.find(account);
    if (it == traces_by_account_.end()) {
        return {};
    }

    std::vector<std::string> invalidated;
    invalidated.reserve(it->second.size());
    for (const auto& trace_key : it->second) {
        if (trace_key != accepted_trace_key) {
            invalidated.push_back(trace_key);
        }
    }
    traces_by_account_.erase(it);
    return invalidated;
}

bool CompetingTraceSet::contains(const std::string& account,
                                 const std::string& trace_key) const {
    auto it = traces_by_account_.find(account);
    return it != traces_by_account_.end() &&
           it->second.count(trace_key) != 0;
}
