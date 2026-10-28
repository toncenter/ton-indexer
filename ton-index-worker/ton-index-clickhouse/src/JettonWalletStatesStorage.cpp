#include "JettonWalletStatesStorage.h"

#include <stdexcept>
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "convert-utils.h"

namespace jetton_history {

clickhouse::Block states_block(const std::vector<schema::JettonWalletState>& states) {
    using namespace clickhouse;
    auto account = std::make_shared<ColumnString>();
    auto mc = std::make_shared<ColumnUInt32>();
    auto time = std::make_shared<ColumnDateTime>("UTC");
    auto lt = std::make_shared<ColumnUInt64>();
    auto existing = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto balance = std::make_shared<ColumnNullableT<ColumnString>>();
    for (const auto& state : states) {
        if (!state.valid()) {
            throw std::runtime_error("Invalid jetton wallet history observation");
        }
        account->Append(convert::to_raw_address(state.account));
        mc->Append(state.mc_seqno);
        time->AppendRaw(state.gen_utime);
        lt->Append(state.last_tx_lt);
        existing->Append(state.existing ? std::optional<std::uint8_t>(*state.existing) : std::nullopt);
        balance->Append(state.balance.not_null() ? std::optional<std::string>(state.balance->to_dec_string()) : std::nullopt);
    }
    Block block;
    block.AppendColumn("account", account);
    block.AppendColumn("mc_seqno", mc);
    block.AppendColumn("gen_utime", time);
    block.AppendColumn("last_tx_lt", lt);
    block.AppendColumn("existing", existing);
    block.AppendColumn("balance", balance);
    return block;
}

}  // namespace jetton_history
