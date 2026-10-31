#pragma once

#include "IndexData.h"

namespace gramstat {

td::Result<std::vector<schema::Deployment>> collect_deployments(
    std::uint32_t mc_seqno, const ParsedBlock& parsed_block);

}  // namespace gramstat
