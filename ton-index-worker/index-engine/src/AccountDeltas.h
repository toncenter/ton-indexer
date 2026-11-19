#pragma once

#include "IndexData.h"

namespace gramstat {

td::Result<schema::AccountDeltas> calculate_account_deltas(
    std::uint32_t mc_seqno, const ParsedBlock& parsed_block);

}  // namespace gramstat
