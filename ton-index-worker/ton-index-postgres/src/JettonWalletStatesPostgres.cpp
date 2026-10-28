#include "JettonWalletStatesPostgres.h"
#include "postgresql_tools.h"

namespace jetton_history::postgres {

void insert_states(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
    std::initializer_list<std::string_view> columns = {
        "account", "mc_seqno", "gen_utime", "last_tx_lt", "existing", "balance"};
    PopulateTableStream stream(txn, "public.jetton_wallet_states", columns, 1000);
    stream.setConflictDoUpdate({"account", "mc_seqno"},
        {"gen_utime", "last_tx_lt", "existing", "balance"}, "");
    for (const auto& task : tasks) {
        if (!task.parsed_block_->jetton_wallet_states_) {
            throw std::runtime_error("Missing jetton wallet history in PostgreSQL batch");
        }
        for (const auto& state : *task.parsed_block_->jetton_wallet_states_) {
            if (state.mc_seqno != task.mc_seqno_ || !state.valid()) {
                throw std::runtime_error("Invalid jetton wallet history in PostgreSQL batch");
            }
            stream.insert_row(std::make_tuple(state.account, state.mc_seqno, state.gen_utime,
                                              state.last_tx_lt, state.existing, state.balance));
        }
    }
    stream.finish();
}

}  // namespace jetton_history::postgres
