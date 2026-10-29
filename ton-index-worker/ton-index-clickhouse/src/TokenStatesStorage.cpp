#include "TokenStatesStorage.h"
#include "StateHistoryBatch.h"

#include <stdexcept>
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "convert-utils.h"

namespace {

template<class Row, class Payload>
clickhouse::Block history_block(const std::vector<Row>& states, const char* payload_column, Payload&& payload) {
    using namespace clickhouse;
    auto account = std::make_shared<ColumnString>();
    auto mc = std::make_shared<ColumnUInt32>();
    auto time = std::make_shared<ColumnDateTime>("UTC");
    auto lt = std::make_shared<ColumnUInt64>();
    auto existing = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto values = std::make_shared<ColumnNullableT<ColumnString>>();
    for (const auto& state : states) {
        if (!state.valid()) {
            throw std::runtime_error("Invalid token history observation");
        }
        account->Append(convert::to_raw_address(state.account));
        mc->Append(state.mc_seqno);
        time->AppendRaw(state.gen_utime);
        lt->Append(state.last_tx_lt);
        existing->Append(state.existing ? std::optional<std::uint8_t>(*state.existing) : std::nullopt);
        values->Append(payload(state));
    }
    Block block;
    block.AppendColumn("account", account);
    block.AppendColumn("mc_seqno", mc);
    block.AppendColumn("gen_utime", time);
    block.AppendColumn("last_tx_lt", lt);
    block.AppendColumn("existing", existing);
    block.AppendColumn(payload_column, values);
    return block;
}

template<class Row, class Serializer>
void insert_history(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks,
                    const char* table, std::optional<std::vector<Row>> ParsedBlock::* member,
                    Serializer&& serialize) {
    std::vector<Row> rows;
    account_history::for_each_row(tasks, member, [&](const Row& row) { rows.push_back(row); });
    if (!rows.empty()) {
        client.Insert(table, serialize(rows));
    }
}

}  // namespace

clickhouse::Block jetton_history::states_block(const std::vector<schema::JettonWalletState>& states) {
    return history_block(states, "balance", [](const schema::JettonWalletState& row) {
        return row.balance.not_null() ? std::optional<std::string>(row.balance->to_dec_string()) : std::nullopt;
    });
}

clickhouse::Block nft_history::states_block(const std::vector<schema::NFTItemState>& states) {
    return history_block(states, "owner", [](const schema::NFTItemState& row) {
        return convert::to_raw_address(row.owner);
    });
}

void account_history::insert_states(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks) {
    insert_history(client, tasks, "jetton_wallet_states", &ParsedBlock::jetton_wallet_states_, jetton_history::states_block);
    insert_history(client, tasks, "nft_item_states", &ParsedBlock::nft_item_states_, nft_history::states_block);
}
