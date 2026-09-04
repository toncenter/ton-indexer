#include <iterator>
#include <unordered_map>
#include <utility>

#include "TraceUpdate.h"

namespace {

using DetectedInterfaces = std::vector<typename Trace::Detector::DetectedInterface>;

struct InterfaceCandidate {
  std::size_t fragment_index;
  bool emulated;
  ton::LogicalTime state_lt;
  DetectedInterfaces interfaces;
};

std::pair<bool, ton::LogicalTime> account_state_rank(const Trace& trace, const block::StdAddress& address) {
  auto emulated = trace.emulated_accounts.equal_range(address);
  if (emulated.first != emulated.second) {
    return {true, std::prev(emulated.second)->second.last_trans_lt_};
  }
  auto committed = trace.committed_accounts.find(address);
  if (committed != trace.committed_accounts.end()) {
    return {false, committed->second.last_trans_lt_};
  }
  return {false, 0};
}

bool should_replace(const InterfaceCandidate& current, bool emulated, ton::LogicalTime state_lt) {
  if (current.emulated != emulated) {
    return emulated;
  }
  return state_lt > current.state_lt;
}

}  // namespace

void normalize_trace_update_interfaces(TraceUpdate& update) {
  if (update.size() < 2) {
    return;
  }

  std::unordered_map<block::StdAddress, InterfaceCandidate> selected;

  for (std::size_t fragment_index = 0; fragment_index < update.fragments.size(); ++fragment_index) {
    auto& fragment = update.fragments[fragment_index];
    for (auto& [address, interfaces] : fragment.interfaces) {
      auto [emulated, state_lt] = account_state_rank(fragment, address);
      auto candidate = selected.find(address);
      if (candidate == selected.end()) {
        selected.emplace(address, InterfaceCandidate{
                                      .fragment_index = fragment_index,
                                      .emulated = emulated,
                                      .state_lt = state_lt,
                                      .interfaces = std::move(interfaces),
                                  });
      } else if (should_replace(candidate->second, emulated, state_lt)) {
        candidate->second = InterfaceCandidate{
            .fragment_index = fragment_index,
            .emulated = emulated,
            .state_lt = state_lt,
            .interfaces = std::move(interfaces),
        };
      }
    }
    fragment.interfaces.clear();
  }

  for (auto& [address, candidate] : selected) {
    update.fragments[candidate.fragment_index].interfaces.emplace(address, std::move(candidate.interfaces));
  }
}
