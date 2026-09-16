#pragma once

#include "crypto/common/bitstring.h"
#include "ton/ton-types.h"

#include <map>
#include <set>
#include <vector>

class ConfirmedRootTracker {
public:
    void add_confirmed_root(ton::BlockIdExt block_id, td::Bits256 trace_hash);
    void add_finalized_root(ton::BlockIdExt block_id, td::Bits256 trace_hash);

    // Returns roots whose latest confirmed inclusion belonged to a discarded
    // block. An empty finalized block is still meaningful and must be passed in.
    std::vector<td::Bits256> finalize_block(ton::BlockIdExt finalized_block);

private:
    struct ConfirmedBlock {
        ton::BlockIdExt block_id;
        std::set<td::Bits256> roots;
    };

    std::map<ton::BlockId, std::vector<ConfirmedBlock>> confirmed_blocks_;
    std::map<ton::BlockId, std::set<td::Bits256>> finalized_roots_;
    std::map<td::Bits256, std::set<ton::BlockIdExt>> latest_confirmed_roots_;
};
