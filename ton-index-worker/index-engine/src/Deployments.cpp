#include "Deployments.h"

#include "crypto/block/block.h"

namespace gramstat {

td::Result<std::vector<schema::Deployment>> collect_deployments(
    std::uint32_t mc_seqno, const ParsedBlock& parsed_block) {
    const schema::Block* mc = nullptr;
    for (const auto& block : parsed_block.blocks_) {
        if (block.workchain == -1) {
            if (mc || block.seqno != mc_seqno || mc_seqno == 0) {
                return td::Status::Error("Deployments require exactly one matching masterchain block");
            }
            mc = &block;
        }
        if (block.mc_block_workchain != -1 || block.mc_block_seqno != mc_seqno) {
            return td::Status::Error("Deployments contain a block from a different masterchain snapshot");
        }
    }
    if (!mc) {
        return td::Status::Error("Deployments are missing the masterchain block");
    }

    std::vector<schema::Deployment> result;
    try {
        // blocks_ contains the MC and only newly included shard blocks. No
        // account-state lookup is needed, including for intermediate transactions.
        for (const auto& block : parsed_block.blocks_) {
            for (const auto& tx : block.transactions) {
                if (tx.mc_seqno != mc_seqno || tx.account.workchain != block.workchain) {
                    return td::Status::Error("Deployments contain a transaction from a different block");
                }
                if ((tx.orig_status != schema::AccountStatus::nonexist &&
                     tx.orig_status != schema::AccountStatus::uninit) ||
                    tx.end_status != schema::AccountStatus::active) {
                    continue;
                }
                // Do not interpret a descriptor without an aborted flag as success.
                auto aborted = std::visit([](const auto& descr) -> std::optional<bool> {
                    if constexpr (requires { descr.aborted; }) {
                        return descr.aborted;
                    }
                    return std::nullopt;
                }, tx.description);
                if (!aborted) {
                    return td::Status::Error("Unsupported transaction description for deployment");
                }
                if (*aborted) {
                    continue;
                }
                if (!tx.in_msg || tx.in_msg->init_state.is_null()) {
                    return td::Status::Error("Successful deployment is missing its incoming StateInit");
                }
                auto cs = vm::load_cell_slice(tx.in_msg->init_state);
                block::gen::StateInit::Record init;
                if (!tlb::unpack_exact(cs, init)) {
                    return td::Status::Error("Invalid incoming deployment StateInit");
                }
                std::optional<td::Bits256> code_hash;
                auto& code = init.code.write();
                if (code.fetch_ulong(1)) {
                    code_hash = code.fetch_ref()->get_hash().bits();
                }
                // NULL means a valid StateInit with no code, never a failed lookup.
                // SETCODE in this transaction does not change the deployment code.
                result.push_back({mc_seqno, tx.now, tx.account, tx.lt, tx.hash,
                                  code_hash, static_cast<std::uint32_t>(tx.out_msgs.size())});
            }
        }
    } catch (const vm::VmError& error) {
        return td::Status::Error(PSLICE() << "Failed to parse deployment StateInit: " << error.get_msg());
    }
    return result;
}

}  // namespace gramstat
