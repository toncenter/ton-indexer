#include "AccountStateHistory.h"

#include <algorithm>
#include "convert-utils.h"
#include "parse_contract_methods.h"
#include "safe-parse.h"
#include "vm/db/DynamicBagOfCellsDb.h"

namespace account_history {

FinalStates final_states(const ParsedBlock& parsed) {
    FinalStates states;
    for (const auto& state : parsed.account_states_) {
        auto& last = states[state.account];
        if (!last || last->last_trans_lt < state.last_trans_lt) {
            last = &state;
        }
    }
    return states;
}

namespace {

struct AccountInterval {
    const schema::Transaction* first = nullptr;
    const schema::Transaction* last = nullptr;
};

// Read only Account/AccountStorage/StateInit headers and the code reference;
// neither account data nor the account's currency dictionaries are traversed.
td::Result<td::Ref<vm::Cell>> previous_active_code(const ParsedBlock& parsed,
                                                const schema::Transaction& first) {
    return safe_parse::guard("Token history pre-state", [&]() -> td::Result<td::Ref<vm::Cell>> {
        if (!parsed.cell_db_reader_) {
            return td::Status::Error("Missing CellDB reader for active-to-inactive transition");
        }
        TRY_RESULT(root, parsed.cell_db_reader_->load_cell(first.account_state_hash_before.as_slice()));
        if (td::Bits256(root->get_hash().bits()) != first.account_state_hash_before) {
            return td::Status::Error("Token history pre-state hash mismatch");
        }
        block::gen::Account::Record_account account;
        block::gen::AccountStorage::Record storage;
        block::gen::AccountState::Record_account_active active;
        block::gen::StateInit::Record init;
        if (!(tlb::unpack_cell(root, account) && tlb::csr_unpack(account.storage, storage) &&
              tlb::csr_unpack(storage.state, active) && tlb::csr_unpack(active.x, init))) {
            return td::Status::Error("Invalid active account in token history pre-state");
        }
        TRY_RESULT(address, convert::to_std_address(account.addr));
        if (!(address == first.account)) {
            return td::Status::Error("Token history pre-state address mismatch");
        }
        auto& code = init.code.write();
        return code.fetch_long(1) ? code.fetch_ref() : td::Ref<vm::Cell>{};
    });
}

void assign_payload(schema::JettonWalletState& row, const schema::JettonWalletDataV2& data) {
    row.balance = data.balance;
    if (!row.valid()) {
        row.existing.reset();
        row.balance.clear();
    }
}

void assign_payload(schema::NFTItemState& row, const schema::NFTItemDataV2& data) {
    row.existing = data.init;
    if (data.init) {
        row.owner = data.owner_address;
    }
}

template<class Interface, class Row, class HasMethod>
void append_observation(std::vector<Row>& rows, Row row, const ParsedBlock& parsed,
                        const schema::AccountState* state, const td::Ref<vm::Cell>& code,
                        unsigned long long method, HasMethod&& has_method) {
    bool verified = false;
    if (state && row.existing == true) {
        auto found = parsed.account_interfaces_.find(row.account);
        if (found != parsed.account_interfaces_.end()) {
            for (const auto& interface : found->second) {
                const auto* data = std::get_if<Interface>(&interface);
                if (data && data->address == row.account && data->last_transaction_lt == row.last_tx_lt &&
                    state->code_hash == data->code_hash && state->data_hash == data->data_hash) {
                    assign_payload(row, *data);
                    verified = row.existing.has_value();
                    break;
                }
            }
        }
        if (!verified) {
            row.existing.reset();
        }
    }
    // A successful probe supports nonstandard dispatchers too. Otherwise only
    // a declared token getter establishes eligibility for false/unknown rows.
    if (verified || has_method(code, method) == true) {
        rows.push_back(std::move(row));
    }
}

}  // namespace

td::Result<TokenStates> collect_states(
    std::uint32_t mc_seqno, const ParsedBlock& parsed) {
    const schema::Block* mc = nullptr;
    std::unordered_map<block::StdAddress, AccountInterval> intervals;
    for (const auto& block : parsed.blocks_) {
        if (block.workchain == -1) {
            if (mc || block.seqno != mc_seqno || mc_seqno == 0) {
                return td::Status::Error("Token history requires one matching masterchain block");
            }
            mc = &block;
        }
        if (block.mc_block_workchain != -1 || block.mc_block_seqno != mc_seqno) {
            return td::Status::Error("Token history contains a block from another MC");
        }
        for (const auto& tx : block.transactions) {
            if (tx.mc_seqno != mc_seqno || tx.account.workchain != block.workchain) {
                return td::Status::Error("Token history contains a transaction from another block");
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
        return td::Status::Error("Token history is missing its masterchain block");
    }
    auto states = final_states(parsed);

    // Cache within this MC/config, including an unclassifiable method set. A
    // Parser failure is never treated as a proven absent token data getter.
    std::unordered_map<td::Bits256, std::optional<std::vector<unsigned long long>>> candidates;
    auto has_method = [&](const td::Ref<vm::Cell>& code, unsigned long long method) -> std::optional<bool> {
        if (code.is_null()) {
            return false;
        }
        const td::Bits256 hash = code->get_hash().bits();
        auto [found, inserted] = candidates.try_emplace(hash);
        if (inserted) {
            auto methods = parse_contract_methods(code, parsed.mc_block_.config_.get());
            if (methods.is_ok()) {
                found->second = methods.move_as_ok();
            } else {
                LOG(DEBUG) << "Cannot establish token history eligibility for code " << hash.to_hex()
                           << ": " << methods.move_as_error();
            }
        }
        if (!found->second) {
            return std::nullopt;
        }
        const auto& methods = *found->second;
        return std::find(methods.begin(), methods.end(), method) != methods.end();
    };

    TokenStates result;
    for (const auto& [address, interval] : intervals) {
        const auto* tx = interval.last;
        const schema::AccountState* state = nullptr;
        td::Ref<vm::Cell> code;
        TokenDetectionStatus detection;
        switch (tx->end_status) {
        case schema::AccountStatus::nonexist:
        case schema::AccountStatus::uninit:
        case schema::AccountStatus::frozen: {
            if (interval.first->orig_status != schema::AccountStatus::active) {
                continue;
            }
            // This is the state BEFORE the first transaction of this address in
            // the MC, not an intermediate before-state inside a shard block.
            TRY_RESULT_ASSIGN(code, previous_active_code(parsed, *interval.first));
            detection.jetton_wallet = detection.nft_item = false;
            break;
        }
        case schema::AccountStatus::active: {
            auto found = states.find(address);
            if (found == states.end() || found->second->last_trans_lt != tx->lt ||
                found->second->hash != tx->account_state_hash_after) {
                // Without final code, a failed load cannot establish eligibility.
                // Retry the load rather than emit unknown rows for arbitrary accounts.
                return td::Status::Error("Missing final active state for token history eligibility");
            }
            state = found->second;
            code = state->code;
            if (auto probe = parsed.token_detection_.find(address); probe != parsed.token_detection_.end()) {
                detection = probe->second;
            }
            break;
        }
        default:
            return td::Status::Error("Invalid final account status in token history");
        }
        const auto time = static_cast<std::uint32_t>(mc->gen_utime);
        append_observation<schema::JettonWalletDataV2>(result.jetton_wallets,
            schema::JettonWalletState{address, mc_seqno, time, tx->lt, detection.jetton_wallet, {}},
            parsed, state, code, JettonWalletDetectorR::DATA_METHOD_ID, has_method);
        append_observation<schema::NFTItemDataV2>(result.nft_items,
            schema::NFTItemState{address, mc_seqno, time, tx->lt, detection.nft_item, {}},
            parsed, state, code, NftItemDetectorR::DATA_METHOD_ID, has_method);
    }
    return result;
}

}  // namespace account_history
