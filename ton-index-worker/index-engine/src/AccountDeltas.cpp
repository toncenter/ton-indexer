#include "AccountDeltas.h"

namespace gramstat {

td::Result<schema::AccountDeltas> calculate_account_deltas(
    std::uint32_t mc_seqno, const ParsedBlock& parsed_block) {
    const schema::Block* mc = nullptr;
    for (const auto& block : parsed_block.blocks_) {
        if (block.workchain == -1) {
            if (mc || block.seqno != mc_seqno || mc_seqno == 0) {
                return td::Status::Error("Account deltas require exactly one matching masterchain block");
            }
            mc = &block;
        }
        if (block.mc_block_workchain != -1 || block.mc_block_seqno != mc_seqno) {
            return td::Status::Error("Account deltas contain a block from a different masterchain snapshot");
        }
    }
    if (!mc) {
        return td::Status::Error("Account deltas are missing the masterchain block");
    }

    schema::AccountDeltas result{mc_seqno, static_cast<std::uint32_t>(mc->gen_utime)};
    auto apply = [&result](schema::AccountStatus status, std::int64_t delta) -> td::Status {
        switch (status) {
            case schema::AccountStatus::uninit: result.delta_uninit += delta; break;
            case schema::AccountStatus::active: result.delta_active += delta; break;
            case schema::AccountStatus::frozen: result.delta_frozen += delta; break;
            case schema::AccountStatus::nonexist: break;
            default: return td::Status::Error("Unknown account status in account deltas");
        }
        return td::Status::OK();
    };
    // blocks_ is parsed from shard_blocks_diff_: MC plus ONLY newly included
    // shard blocks, not the current tips (which can recur across many MCs).
    for (const auto& block : parsed_block.blocks_) {
        for (const auto& tx : block.transactions) {
            if (tx.mc_seqno != mc_seqno || tx.account.workchain != block.workchain) {
                return td::Status::Error("Account deltas contain a transaction from a different block");
            }
            // Include aborted transactions too: their final status is authoritative.
            TRY_STATUS(apply(tx.orig_status, -1));
            TRY_STATUS(apply(tx.end_status, 1));
        }
    }
    return result;
}

}  // namespace gramstat
