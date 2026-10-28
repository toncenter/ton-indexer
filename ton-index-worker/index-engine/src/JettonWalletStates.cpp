#include "JettonWalletStates.h"

#include <algorithm>
#include "convert-utils.h"
#include "parse_contract_methods.h"
#include "safe-parse.h"
#include "vm/db/DynamicBagOfCellsDb.h"

namespace jetton_history {

namespace {

struct AccountInterval {
    const schema::Transaction* first = nullptr;
    const schema::Transaction* last = nullptr;
};

// Read only Account/AccountStorage/StateInit headers and the code reference;
// neither account data nor the account's currency dictionaries are traversed.
td::Result<td::Ref<vm::Cell>> previous_active_code(const ParsedBlock& parsed,
                                                const schema::Transaction& first) {
    return safe_parse::guard("Jetton history pre-state", [&]() -> td::Result<td::Ref<vm::Cell>> {
        if (!parsed.cell_db_reader_) {
            return td::Status::Error("Missing CellDB reader for active-to-inactive transition");
        }
        TRY_RESULT(root, parsed.cell_db_reader_->load_cell(first.account_state_hash_before.as_slice()));
        if (td::Bits256(root->get_hash().bits()) != first.account_state_hash_before) {
            return td::Status::Error("Jetton history pre-state hash mismatch");
        }
        block::gen::Account::Record_account account;
        block::gen::AccountStorage::Record storage;
        block::gen::AccountState::Record_account_active active;
        block::gen::StateInit::Record init;
        if (!(tlb::unpack_cell(root, account) && tlb::csr_unpack(account.storage, storage) &&
              tlb::csr_unpack(storage.state, active) && tlb::csr_unpack(active.x, init))) {
            return td::Status::Error("Invalid active account in jetton history pre-state");
        }
        TRY_RESULT(address, convert::to_std_address(account.addr));
        if (!(address == first.account)) {
            return td::Status::Error("Jetton history pre-state address mismatch");
        }
        auto& code = init.code.write();
        return code.fetch_long(1) ? code.fetch_ref() : td::Ref<vm::Cell>{};
    });
}

}  // namespace

td::Result<std::vector<schema::JettonWalletState>> collect_states(
    std::uint32_t mc_seqno, const ParsedBlock& parsed) {
    const schema::Block* mc = nullptr;
    std::unordered_map<block::StdAddress, AccountInterval> intervals;
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
            auto& interval = intervals[tx.account];
            if (!interval.first || tx.lt < interval.first->lt) {
                interval.first = &tx;
            }
            if (!interval.last || interval.last->lt < tx.lt) {
                interval.last = &tx;
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

    // Cache within this MC/config, including an unclassifiable method set. A
    // parser failure is never treated as a proven absent get_wallet_data method.
    std::unordered_map<td::Bits256, std::optional<bool>> candidates;
    auto has_wallet_method = [&](const td::Ref<vm::Cell>& code) -> std::optional<bool> {
        if (code.is_null()) {
            return false;
        }
        const td::Bits256 hash = code->get_hash().bits();
        if (auto found = candidates.find(hash); found != candidates.end()) {
            return found->second;
        }
        std::optional<bool> candidate;
        auto methods = parse_contract_methods(code, parsed.mc_block_.config_.get());
        if (methods.is_ok()) {
            candidate = std::find(methods.ok().begin(), methods.ok().end(), 97026ULL) != methods.ok().end();
        } else {
            LOG(DEBUG) << "Cannot establish jetton history eligibility for code " << hash.to_hex()
                       << ": " << methods.move_as_error();
        }
        candidates.emplace(hash, candidate);
        return candidate;
    };

    std::vector<schema::JettonWalletState> result;
    result.reserve(intervals.size());
    for (const auto& [address, interval] : intervals) {
        const auto* tx = interval.last;
        schema::JettonWalletState row{address, mc_seqno, static_cast<std::uint32_t>(mc->gen_utime),
                                      tx->lt, std::nullopt, {}};
        switch (tx->end_status) {
        case schema::AccountStatus::nonexist:
        case schema::AccountStatus::uninit:
        case schema::AccountStatus::frozen: {
            if (interval.first->orig_status != schema::AccountStatus::active) {
                continue;
            }
            // This is the state BEFORE the first transaction of this address in
            // the MC, not an intermediate before-state inside a shard block.
            TRY_RESULT(code, previous_active_code(parsed, *interval.first));
            if (has_wallet_method(code) != true) {
                continue;
            }
            row.existing = false;
            break;
        }
        case schema::AccountStatus::active: {
            auto state = states.find(address);
            if (state == states.end() || state->second->last_trans_lt != tx->lt ||
                state->second->hash != tx->account_state_hash_after) {
                // Without final code, a failed load cannot establish eligibility.
                // Retry the load rather than emit unknown rows for arbitrary accounts.
                return td::Status::Error("Missing final active state for jetton history eligibility");
            }
            auto detection = parsed.jetton_wallet_detection_.find(address);
            if (detection != parsed.jetton_wallet_detection_.end()) {
                row.existing = detection->second;
            }
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
            // Verified wallets also support nonstandard dispatchers. Otherwise
            // require the method in the CURRENT code before recording false/NULL.
            // Deliberately no previous-state reads for active-to-active changes.
            if (row.existing != true && has_wallet_method(state->second->code) != true) {
                continue;
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
