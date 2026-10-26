#include "GramstatStorage.h"

#include <stdexcept>

#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/string.h"
#include "convert-utils.h"

namespace gramstat {

clickhouse::Block total_supply_block(const std::vector<schema::TotalSupply>& snapshots) {
    using namespace clickhouse;
    auto seqno = std::make_shared<ColumnUInt32>();
    auto time = std::make_shared<ColumnDateTime>("UTC");
    auto total = std::make_shared<ColumnDecimal>(38, 0);
    auto accounts = std::make_shared<ColumnDecimal>(38, 0);
    auto dispatch = std::make_shared<ColumnDecimal>(38, 0);
    auto messages = std::make_shared<ColumnDecimal>(38, 0);
    auto fees = std::make_shared<ColumnDecimal>(38, 0);
    auto append_amount = [](ColumnDecimal& column, const td::RefInt256& amount) {
        if (amount.is_null() || !amount->is_valid() || amount->sgn() < 0) {
            throw std::runtime_error("Invalid total supply amount");
        }
        auto decimal = amount->to_dec_string();
        if (decimal.size() > 38) {
            throw std::runtime_error("Total supply amount exceeds Decimal(38, 0)");
        }
        column.Append(decimal);
    };
    for (const auto& snapshot : snapshots) {
        seqno->Append(snapshot.mc_seqno);
        time->AppendRaw(snapshot.gen_utime);
        append_amount(*total, snapshot.total);
        append_amount(*accounts, snapshot.accounts);
        append_amount(*dispatch, snapshot.dispatch_queue);
        append_amount(*messages, snapshot.unprocessed_messages);
        append_amount(*fees, snapshot.unrecovered_validator_fees);
    }
    Block block;
    block.AppendColumn("mc_seqno", seqno);
    block.AppendColumn("gen_utime", time);
    block.AppendColumn("total", total);
    block.AppendColumn("accounts", accounts);
    block.AppendColumn("dispatch_queue", dispatch);
    block.AppendColumn("unprocessed_messages", messages);
    block.AppendColumn("unrecovered_validator_fees", fees);
    return block;
}

clickhouse::Block account_deltas_block(const std::vector<schema::AccountDeltas>& deltas) {
    using namespace clickhouse;
    auto seqno = std::make_shared<ColumnUInt32>();
    auto time = std::make_shared<ColumnDateTime>("UTC");
    auto uninit = std::make_shared<ColumnInt64>();
    auto active = std::make_shared<ColumnInt64>();
    auto frozen = std::make_shared<ColumnInt64>();
    for (const auto& delta : deltas) {
        seqno->Append(delta.mc_seqno);
        time->AppendRaw(delta.gen_utime);
        uninit->Append(delta.delta_uninit);
        active->Append(delta.delta_active);
        frozen->Append(delta.delta_frozen);
    }
    Block block;
    block.AppendColumn("mc_seqno", seqno);
    block.AppendColumn("gen_utime", time);
    block.AppendColumn("delta_uninit", uninit);
    block.AppendColumn("delta_active", active);
    block.AppendColumn("delta_frozen", frozen);
    return block;
}

clickhouse::Block deployments_block(const std::vector<schema::Deployment>& deployments) {
    using namespace clickhouse;
    auto seqno = std::make_shared<ColumnUInt32>();
    auto time = std::make_shared<ColumnDateTime>("UTC");
    auto account = std::make_shared<ColumnString>();
    auto lt = std::make_shared<ColumnUInt64>();
    auto hash = std::make_shared<ColumnFixedString>(44);
    auto code = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto out_count = std::make_shared<ColumnUInt32>();
    for (const auto& event : deployments) {
        seqno->Append(event.mc_seqno);
        time->AppendRaw(event.gen_utime);
        account->Append(convert::to_raw_address(event.account));
        lt->Append(event.lt);
        hash->Append(td::base64_encode(event.tx_hash.as_slice()));
        code->Append(event.init_code_hash
            ? std::optional<std::string>(td::base64_encode(event.init_code_hash->as_slice())) : std::nullopt);
        out_count->Append(event.out_msgs_count);
    }
    Block block;
    block.AppendColumn("mc_seqno", seqno);
    block.AppendColumn("gen_utime", time);
    block.AppendColumn("account", account);
    block.AppendColumn("lt", lt);
    block.AppendColumn("tx_hash", hash);
    block.AppendColumn("init_code_hash", code);
    block.AppendColumn("out_msgs_count", out_count);
    return block;
}

std::string completed_seqnos_query(bool collect_gramstat, std::int32_t from_seqno, std::int32_t to_seqno) {
    auto range = [from_seqno, to_seqno](const char* column) {
        std::string sql;
        if (from_seqno > 0) {
            sql += " AND " + std::string(column) + " >= " + std::to_string(from_seqno);
        }
        if (to_seqno > 0) {
            sql += " AND " + std::string(column) + " <= " + std::to_string(to_seqno);
        }
        return sql;
    };
    std::string sql = "SELECT DISTINCT seqno FROM blocks WHERE workchain = -1" + range("seqno");
    if (collect_gramstat) {
        sql += " AND toUInt32(seqno) IN (SELECT mc_seqno FROM gramstat.total_supply WHERE 1" +
               range("mc_seqno") + ")";
        sql += " AND toUInt32(seqno) IN (SELECT mc_seqno FROM gramstat.account_deltas WHERE 1" +
               range("mc_seqno") + ")";
    }
    return sql;
}

}  // namespace gramstat
