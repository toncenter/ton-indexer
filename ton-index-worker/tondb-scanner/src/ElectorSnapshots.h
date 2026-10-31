#pragma once

#include "IndexData.h"

namespace staking_history {

td::Result<std::vector<schema::FrozenElection>> parse_elections(const std::vector<vm::StackEntry>& stack);
std::string serialize(const schema::ElectorSnapshot& snapshot);
std::string serialize(const schema::StakeHolder& holder);
void collect_changed_holders(uint32_t mc_seqno, uint32_t gen_utime, ParsedBlock& block);
// Resolve only elected holders, using loaded MC/shard states and a per-MC cache.
void detect_holders(schema::ElectorSnapshot& snapshot, ParsedBlock& block);

}  // namespace staking_history
