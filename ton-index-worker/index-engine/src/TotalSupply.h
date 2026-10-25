#pragma once

#include "IndexData.h"
#include "validator/global-balance-calculator.hpp"

namespace gramstat {

using StateLoader = std::function<td::actor::Task<td::Ref<ton::validator::ShardState>>(ton::BlockIdExt)>;

// Uses the current MC and all its top shard states from the existing pipeline.
// The loader is only for auxiliary MC states and seqno-zero shard tips omitted
// by DbScanner::fetch_seqno. Errors propagate; no partial snapshot is returned.
td::actor::Task<schema::TotalSupply> calculate_total_supply(
    schema::MasterchainBlockDataState blocks, StateLoader load_state);

td::Result<schema::TotalSupply> make_total_supply(
    std::uint32_t mc_seqno, std::uint32_t gen_utime,
    const ton::validator::GlobalBalanceComponents& components);

}  // namespace gramstat
