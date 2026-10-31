#include "TokenStatesPostgres.h"
#include "StateHistoryBatch.h"
#include "postgresql_tools.h"

namespace {

template<class Row, class Payload>
void insert_history(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks,
                    const char* table, const char* payload_column,
                    std::optional<std::vector<Row>> ParsedBlock::* member, Payload&& payload) {
    std::initializer_list<std::string_view> columns = {
        "account", "mc_seqno", "gen_utime", "last_tx_lt", "existing", payload_column};
    PopulateTableStream stream(txn, table, columns, 1000);
    stream.setConflictDoUpdate({"account", "mc_seqno"},
        {"gen_utime", "last_tx_lt", "existing", payload_column}, "");
    account_history::for_each_row(tasks, member, [&](const Row& row) {
        stream.insert_row(std::make_tuple(row.account, row.mc_seqno, row.gen_utime,
                                          row.last_tx_lt, row.existing, payload(row)));
    });
    stream.finish();
}

}  // namespace

void jetton_history::postgres::insert_states(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
    insert_history(txn, tasks, "public.jetton_wallet_states", "balance", &ParsedBlock::jetton_wallet_states_,
        [](const schema::JettonWalletState& row) { return row.balance; });
}

void nft_history::postgres::insert_states(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks) {
    insert_history(txn, tasks, "public.nft_item_states", "owner", &ParsedBlock::nft_item_states_,
        [](const schema::NFTItemState& row) { return row.owner; });
}
