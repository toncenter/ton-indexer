#pragma once

#include "IndexData.h"

namespace elector {

// The executor is shared with the existing validator snapshots.
td::Result<std::vector<vm::StackEntry>> run_get_method(
    const schema::AccountState& state, const std::shared_ptr<block::ConfigInfo>& config,
    const std::string& method, std::vector<vm::StackEntry> input, uint32_t now);

struct RoundAmounts {
  uint32_t election_id;
  uint32_t unfreeze_at;
  std::string vset_hash;
  td::RefInt256 total_stake;
  td::RefInt256 bonuses;
};

// Both historical past_elections ABIs; does not walk frozen positions/complaints.
td::Result<std::vector<RoundAmounts>> parse_round_amounts(const std::vector<vm::StackEntry>& stack);

struct CollectionStats {
  unsigned id_getters = 0;
  unsigned amount_getters = 0;
  unsigned replayed_transactions = 0;
  unsigned unsupported_blocks = 0;
  unsigned legacy_id_getters = 0;
};

td::Result<std::vector<schema::ValidatorRoundResult>> collect_round_results(
    const ParsedBlock& parsed, CollectionStats* stats = nullptr);

}  // namespace elector
