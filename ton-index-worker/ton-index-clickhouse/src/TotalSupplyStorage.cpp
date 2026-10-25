#include "TotalSupplyStorage.h"

#include <stdexcept>

#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/numeric.h"

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
    }
    return sql;
}

}  // namespace gramstat
