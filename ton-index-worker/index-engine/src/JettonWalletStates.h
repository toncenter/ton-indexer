#pragma once

#include "IndexData.h"

namespace jetton_history {

td::Result<std::vector<schema::JettonWalletState>> collect_states(
    std::uint32_t mc_seqno, const ParsedBlock& block);

}  // namespace jetton_history
