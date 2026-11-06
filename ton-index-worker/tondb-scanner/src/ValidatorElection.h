#pragma once

#include "IndexData.h"
#include "MemoCache.h"

namespace elector {

// Memoizes only the audited Elector implementation. Unknown code continues to
// execute against its full historical environment on every call.
td::Result<std::optional<schema::ValidatorElection>> parse_election(
    const schema::AccountState& state, const std::shared_ptr<block::ConfigInfo>& config,
    uint32_t now, uint32_t mc_seqno);
td::Result<std::optional<schema::ValidatorElection>> parse_election_uncached(
    const schema::AccountState& state, const std::shared_ptr<block::ConfigInfo>& config,
    uint32_t now, uint32_t mc_seqno);

memo::Stats election_cache_stats();
std::string election_cache_report();

}  // namespace elector
