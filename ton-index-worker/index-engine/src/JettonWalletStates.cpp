#include "JettonWalletStates.h"

namespace jetton_history {

td::Result<std::vector<schema::JettonWalletState>> collect_states(
    std::uint32_t mc_seqno, const ParsedBlock& parsed) {
    const schema::Block* mc = nullptr;
    std::unordered_map<block::StdAddress, const schema::Transaction*> last_transactions;
    for (const auto& block : parsed.blocks_) {
        if (block.workchain == -1) {
            if (mc || block.seqno != mc_seqno || mc_seqno == 0) {
                return td::Status::Error("Jetton history requires one matching masterchain block");
            }
            mc = &block;
        }
        if (block.mc_block_workchain != -1 || block.mc_block_seqno != mc_seqno) {
            return td::Status::Error("Jetton history contains a block from another MC");
        }
        for (const auto& tx : block.transactions) {
            if (tx.mc_seqno != mc_seqno || tx.account.workchain != block.workchain) {
                return td::Status::Error("Jetton history contains a transaction from another block");
            }
            auto& last = last_transactions[tx.account];
            if (!last || last->lt < tx.lt) {
                last = &tx;
            }
        }
    }
    if (!mc) {
        return td::Status::Error("Jetton history is missing its masterchain block");
    }
    std::unordered_map<block::StdAddress, const schema::AccountState*> states;
    for (const auto& state : parsed.account_states_) {
        auto& last = states[state.account];
        if (!last || last->last_trans_lt < state.last_trans_lt) {
            last = &state;
        }
    }

    std::vector<schema::JettonWalletState> result;
    result.reserve(last_transactions.size());
    for (const auto& [address, tx] : last_transactions) {
        schema::JettonWalletState row{address, mc_seqno, static_cast<std::uint32_t>(mc->gen_utime),
                                     tx->lt, std::nullopt, {}};
        switch (tx->end_status) {
        case schema::AccountStatus::nonexist:
        case schema::AccountStatus::uninit:
        case schema::AccountStatus::frozen:
            // The transaction's final status is authoritative, including aborted
            // transactions. No prior-state lookup is necessary for a tombstone.
            row.existing = false;
            break;
        case schema::AccountStatus::active: {
            auto state = states.find(address);
            if (state == states.end() || state->second->last_trans_lt != tx->lt ||
                state->second->hash != tx->account_state_hash_after) {
                break;  // Failed state load: never reuse an earlier balance.
            }
            if (state->second->code.is_null()) {
                row.existing = false;
                break;
            }
            auto detection = parsed.jetton_wallet_detection_.find(address);
            if (detection == parsed.jetton_wallet_detection_.end()) {
                break;
            }
            row.existing = detection->second;
            if (row.existing.value_or(false)) {
                auto interfaces = parsed.account_interfaces_.find(address);
                if (interfaces != parsed.account_interfaces_.end()) {
                    for (const auto& interface : interfaces->second) {
                        auto wallet = std::get_if<schema::JettonWalletDataV2>(&interface);
                        if (wallet && wallet->last_transaction_lt == tx->lt &&
                            state->second->code_hash == wallet->code_hash &&
                            state->second->data_hash == wallet->data_hash) {
                            row.balance = wallet->balance;
                            break;
                        }
                    }
                }
                if (!row.valid()) {
                    row.existing.reset();
                    row.balance.clear();
                }
            }
            break;
        }
        default:
            return td::Status::Error("Invalid final account status in jetton history");
        }
        result.push_back(std::move(row));
    }
    return result;
}

}  // namespace jetton_history
