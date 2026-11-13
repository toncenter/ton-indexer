#pragma once

#include "IndexData.h"

namespace account_history {

using FinalStates = std::unordered_map<block::StdAddress, const schema::AccountState*>;
// Shared with interface detection: select the last state before filtering code/data.
FinalStates final_states(const ParsedBlock& block);

struct TokenStates {
    std::vector<schema::JettonWalletState> jetton_wallets;
    std::vector<schema::NFTItemState> nft_items;
};

// Both histories share one transaction walk, boundary-state read and method cache.
td::Result<TokenStates> collect_states(std::uint32_t mc_seqno, const ParsedBlock& block);

}  // namespace account_history
