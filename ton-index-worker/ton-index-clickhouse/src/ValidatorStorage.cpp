#include "ValidatorStorage.h"

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "td/utils/base64.h"

#include <functional>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>

namespace validator_storage {

const std::vector<std::string>& schema_queries() {
    static const std::vector<std::string> queries = {
R"SQL(CREATE TABLE IF NOT EXISTS validator_events (
    tx_hash FixedString(44), tx_lt UInt64, tx_now UInt32, mc_seqno UInt32,
    event_type String, stake_holder_address String,
    validator_pubkey Nullable(String), adnl_addr Nullable(String), election_id Nullable(UInt32),
    query_id Nullable(UInt64), amount Decimal(38,0), reason Nullable(Int32)
) ENGINE=ReplacingMergeTree
PARTITION BY toYYYYMM(toDateTime(tx_now, 'UTC')) ORDER BY (mc_seqno, tx_lt, tx_hash))SQL",
R"SQL(CREATE TABLE IF NOT EXISTS validator_elections (
    election_id UInt32, elect_close UInt32, min_stake Decimal(38,0), total_stake Decimal(38,0),
    failed Bool, finished Bool, source_mc_seqno UInt32
) ENGINE=ReplacingMergeTree(source_mc_seqno) ORDER BY election_id)SQL",
R"SQL(CREATE TABLE IF NOT EXISTS validator_election_participants (
    election_id UInt32, validator_pubkey String, stake Decimal(38,0), max_factor UInt32,
    stake_holder_address String, adnl_addr String, source_mc_seqno UInt32
) ENGINE=ReplacingMergeTree(source_mc_seqno)
ORDER BY (election_id, validator_pubkey, stake_holder_address))SQL",
// A late NULL/fined observation must not erase the maximum selected stake.
// Keep the existing key-block observations; fold them by cycle/member when reading.
R"SQL(CREATE TABLE IF NOT EXISTS validator_cycles (
    election_id Nullable(UInt32), utime_since UInt32, utime_until UInt32,
    total UInt32, main UInt32, total_weight UInt64, total_stake Nullable(Decimal(38,0)),
    source_mc_seqno UInt32, validators_elected_for UInt32, elections_start_before UInt32,
    elections_end_before UInt32, stake_held_for UInt32, max_validators UInt32,
    max_main_validators UInt32, min_validators UInt32, min_stake Decimal(38,0),
    max_stake Decimal(38,0), min_total_stake Decimal(38,0), max_stake_factor UInt32
) ENGINE=ReplacingMergeTree ORDER BY (utime_since, source_mc_seqno))SQL",
R"SQL(CREATE TABLE IF NOT EXISTS validator_cycle_members (
    utime_since UInt32, validator_index UInt32, validator_pubkey String, adnl_addr String,
    weight UInt64, stake Nullable(Decimal(38,0)), source_mc_seqno UInt32
) ENGINE=ReplacingMergeTree ORDER BY (utime_since, validator_index, source_mc_seqno))SQL",
R"SQL(CREATE TABLE IF NOT EXISTS validator_complaints (
    election_id UInt32, complaint_hash String, validator_pubkey String, adnl_addr Nullable(String),
    description_boc String, created_at UInt32, severity UInt32, reward_address String,
    paid Decimal(38,0), suggested_fine Decimal(38,0), suggested_fine_part UInt32,
    voted_validators String, vset_id String, weight_remaining Int64, approved_percent Float64,
    is_passed Bool, source_mc_seqno UInt32
) ENGINE=ReplacingMergeTree(source_mc_seqno) ORDER BY (election_id, complaint_hash))SQL",
R"SQL(CREATE TABLE IF NOT EXISTS contract_methods (
    code_hash FixedString(44), methods Array(UInt64)
) ENGINE=ReplacingMergeTree ORDER BY code_hash)SQL"
    };
    return queries;
}

namespace {

template<class Row>
class Columns {
    const std::vector<const Row*>& rows_;
public:
    clickhouse::Block block;
    explicit Columns(const std::vector<const Row*>& rows) : rows_(rows) {}

    template<class Column, class Getter, class... Args>
    void add(const char* name, Getter get, Args&&... args) {
        auto column = std::make_shared<Column>(std::forward<Args>(args)...);
        for (const auto* row : rows_) column->Append(std::invoke(get, *row));
        block.AppendColumn(name, column);
    }

    template<class Getter>
    void amount(const char* name, Getter get, bool nullable = false) {
        auto values = std::make_shared<clickhouse::ColumnDecimal>(38, 0);
        auto nulls = std::make_shared<clickhouse::ColumnUInt8>();
        for (const auto* row : rows_) {
            td::RefInt256 value = std::invoke(get, *row);
            if (value.is_null()) {
                if (!nullable) throw std::runtime_error(std::string("Missing validator amount: ") + name);
                values->Append("0");
                nulls->Append(1);
            } else {
                if (!value->is_valid() || value->sgn() < 0 || value->bit_size(false) > 120) {
                    throw std::runtime_error(std::string("Invalid validator amount: ") + name);
                }
                values->Append(value->to_dec_string());
                nulls->Append(0);
            }
        }
        if (nullable) block.AppendColumn(name, std::make_shared<clickhouse::ColumnNullable>(values, nulls));
        else block.AppendColumn(name, values);
    }
};

clickhouse::Block events_block(const std::vector<const schema::ValidatorEvent*>& rows) {
    using namespace clickhouse;
    using Row = schema::ValidatorEvent;
    Columns<Row> c(rows);
    c.add<ColumnFixedString>("tx_hash", [](const Row& row) { return td::base64_encode(row.transaction_hash.as_slice()); }, 44);
    c.add<ColumnUInt64>("tx_lt", &Row::transaction_lt);
    c.add<ColumnUInt32>("tx_now", &Row::transaction_now);
    c.add<ColumnUInt32>("mc_seqno", &Row::mc_seqno);
    c.add<ColumnString>("event_type", &Row::event_type);
    c.add<ColumnString>("stake_holder_address", &Row::stake_holder_address);
    c.add<ColumnNullableT<ColumnString>>("validator_pubkey", &Row::validator_pubkey);
    c.add<ColumnNullableT<ColumnString>>("adnl_addr", &Row::adnl_addr);
    c.add<ColumnNullableT<ColumnUInt32>>("election_id", &Row::election_id);
    c.add<ColumnNullableT<ColumnUInt64>>("query_id", &Row::query_id);
    c.amount("amount", &Row::amount);
    c.add<ColumnNullableT<ColumnInt32>>("reason", &Row::reason);
    return c.block;
}

clickhouse::Block elections_block(const std::vector<const schema::ValidatorElection*>& rows) {
    using namespace clickhouse;
    using Row = schema::ValidatorElection;
    Columns<Row> c(rows);
    c.add<ColumnUInt32>("election_id", &Row::election_id);
    c.add<ColumnUInt32>("elect_close", &Row::elect_close);
    c.amount("min_stake", &Row::min_stake);
    c.amount("total_stake", &Row::total_stake);
    c.add<ColumnUInt8>("failed", &Row::failed);
    c.add<ColumnUInt8>("finished", &Row::finished);
    c.add<ColumnUInt32>("source_mc_seqno", &Row::source_mc_seqno);
    return c.block;
}

clickhouse::Block participants_block(const std::vector<const schema::ValidatorElectionParticipant*>& rows) {
    using namespace clickhouse;
    using Row = schema::ValidatorElectionParticipant;
    Columns<Row> c(rows);
    c.add<ColumnUInt32>("election_id", &Row::election_id);
    c.add<ColumnString>("validator_pubkey", &Row::validator_pubkey);
    c.amount("stake", &Row::stake);
    c.add<ColumnUInt32>("max_factor", &Row::max_factor);
    c.add<ColumnString>("stake_holder_address", &Row::stake_holder_address);
    c.add<ColumnString>("adnl_addr", &Row::adnl_addr);
    c.add<ColumnUInt32>("source_mc_seqno", &Row::source_mc_seqno);
    return c.block;
}

clickhouse::Block cycles_block(const std::vector<const schema::ValidatorCycle*>& rows) {
    using namespace clickhouse;
    using Row = schema::ValidatorCycle;
    Columns<Row> c(rows);
    c.add<ColumnNullableT<ColumnUInt32>>("election_id", &Row::election_id);
    c.add<ColumnUInt32>("utime_since", &Row::utime_since);
    c.add<ColumnUInt32>("utime_until", &Row::utime_until);
    c.add<ColumnUInt32>("total", &Row::total);
    c.add<ColumnUInt32>("main", &Row::main);
    c.add<ColumnUInt64>("total_weight", &Row::total_weight);
    c.amount("total_stake", [](const Row& row) { return row.total_stake.value_or(td::RefInt256{}); }, true);
    c.add<ColumnUInt32>("source_mc_seqno", &Row::source_mc_seqno);
    c.add<ColumnUInt32>("validators_elected_for", &Row::validators_elected_for);
    c.add<ColumnUInt32>("elections_start_before", &Row::elections_start_before);
    c.add<ColumnUInt32>("elections_end_before", &Row::elections_end_before);
    c.add<ColumnUInt32>("stake_held_for", &Row::stake_held_for);
    c.add<ColumnUInt32>("max_validators", &Row::max_validators);
    c.add<ColumnUInt32>("max_main_validators", &Row::max_main_validators);
    c.add<ColumnUInt32>("min_validators", &Row::min_validators);
    c.amount("min_stake", &Row::min_stake);
    c.amount("max_stake", &Row::max_stake);
    c.amount("min_total_stake", &Row::min_total_stake);
    c.add<ColumnUInt32>("max_stake_factor", &Row::max_stake_factor);
    return c.block;
}

clickhouse::Block members_block(const std::vector<const schema::ValidatorCycleMember*>& rows) {
    using namespace clickhouse;
    using Row = schema::ValidatorCycleMember;
    Columns<Row> c(rows);
    c.add<ColumnUInt32>("utime_since", &Row::utime_since);
    c.add<ColumnUInt32>("validator_index", &Row::validator_index);
    c.add<ColumnString>("validator_pubkey", &Row::validator_pubkey);
    c.add<ColumnString>("adnl_addr", &Row::adnl_addr);
    c.add<ColumnUInt64>("weight", &Row::weight);
    c.amount("stake", [](const Row& row) { return row.stake.value_or(td::RefInt256{}); }, true);
    c.add<ColumnUInt32>("source_mc_seqno", &Row::source_mc_seqno);
    return c.block;
}

clickhouse::Block complaints_block(const std::vector<const schema::ValidatorComplaint*>& rows) {
    using namespace clickhouse;
    using Row = schema::ValidatorComplaint;
    Columns<Row> c(rows);
    c.add<ColumnUInt32>("election_id", &Row::election_id);
    c.add<ColumnString>("complaint_hash", &Row::complaint_hash);
    c.add<ColumnString>("validator_pubkey", &Row::validator_pubkey);
    c.add<ColumnNullableT<ColumnString>>("adnl_addr", &Row::adnl_addr);
    c.add<ColumnString>("description_boc", &Row::description_boc);
    c.add<ColumnUInt32>("created_at", &Row::created_at);
    c.add<ColumnUInt32>("severity", &Row::severity);
    c.add<ColumnString>("reward_address", &Row::reward_address);
    c.amount("paid", &Row::paid);
    c.amount("suggested_fine", &Row::suggested_fine);
    c.add<ColumnUInt32>("suggested_fine_part", &Row::suggested_fine_part);
    c.add<ColumnString>("voted_validators", &Row::voted_validators);
    c.add<ColumnString>("vset_id", &Row::vset_id);
    c.add<ColumnInt64>("weight_remaining", &Row::weight_remaining);
    c.add<ColumnFloat64>("approved_percent", &Row::approved_percent);
    c.add<ColumnUInt8>("is_passed", &Row::is_passed);
    c.add<ColumnUInt32>("source_mc_seqno", &Row::source_mc_seqno);
    return c.block;
}

template<class Map, class Key, class Row>
void keep_latest(Map& rows, Key key, const Row& row) {
    auto [it, inserted] = rows.emplace(std::move(key), &row);
    if (!inserted && it->second->source_mc_seqno <= row.source_mc_seqno) it->second = &row;
}

template<class Key, class Row>
std::vector<const Row*> values(const std::map<Key, const Row*>& rows) {
    std::vector<const Row*> result;
    result.reserve(rows.size());
    for (const auto& [key, row] : rows) result.push_back(row);
    return result;
}

void check_source(uint32_t source, const InsertTaskStruct& task) {
    if (source != task.mc_seqno_) throw std::runtime_error("Validator snapshot MC mismatch");
}

}  // namespace

std::vector<TableBlock> prepare_blocks(const std::vector<InsertTaskStruct>& tasks) {
    std::map<std::tuple<uint32_t, uint64_t, td::Bits256>, const schema::ValidatorEvent*> events;
    std::map<uint32_t, const schema::ValidatorElection*> elections;
    std::map<std::tuple<uint32_t, std::string, std::string>, const schema::ValidatorElectionParticipant*> participants;
    std::map<std::pair<uint32_t, uint32_t>, const schema::ValidatorCycle*> cycles;
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, const schema::ValidatorCycleMember*> members;
    std::map<std::pair<uint32_t, std::string>, const schema::ValidatorComplaint*> complaints;
    std::map<td::Bits256, std::set<uint64_t>> methods;
    for (const auto& task : tasks) {
        if (!task.parsed_block_) throw std::runtime_error("Missing parsed block for validator storage");
        const auto& parsed = *task.parsed_block_;
        for (const auto& event : parsed.events_) {
            if (auto row = std::get_if<schema::ValidatorEvent>(&event)) {
                check_source(row->mc_seqno, task);
                events[{row->mc_seqno, row->transaction_lt, row->transaction_hash}] = row;
            }
        }
        for (const auto& row : parsed.validator_elections_) {
            check_source(row.source_mc_seqno, task);
            keep_latest(elections, row.election_id, row);
            for (const auto& participant : row.participants) {
                check_source(participant.source_mc_seqno, task);
                if (participant.election_id != row.election_id) throw std::runtime_error("Validator participant election mismatch");
                keep_latest(participants, std::make_tuple(participant.election_id, participant.validator_pubkey,
                                                        participant.stake_holder_address), participant);
            }
        }
        for (const auto& row : parsed.validator_cycles_) {
            check_source(row.source_mc_seqno, task);
            cycles[{row.utime_since, row.source_mc_seqno}] = &row;
            for (const auto& member : row.members) {
                check_source(member.source_mc_seqno, task);
                if (member.utime_since != row.utime_since) throw std::runtime_error("Validator member cycle mismatch");
                members[{member.utime_since, member.validator_index, member.source_mc_seqno}] = &member;
            }
        }
        for (const auto& row : parsed.validator_complaints_) {
            check_source(row.source_mc_seqno, task);
            keep_latest(complaints, std::make_pair(row.election_id, row.complaint_hash), row);
        }
        for (const auto& [hash, method] : parsed.contract_methods_) methods[hash].insert(method);
    }

    auto code_hash = std::make_shared<clickhouse::ColumnFixedString>(44);
    auto method_ids = std::make_shared<clickhouse::ColumnArrayT<clickhouse::ColumnUInt64>>();
    for (const auto& [hash, ids] : methods) {
        code_hash->Append(td::base64_encode(hash.as_slice()));
        method_ids->Append(std::vector<uint64_t>(ids.begin(), ids.end()));
    }
    clickhouse::Block method_block;
    method_block.AppendColumn("code_hash", code_hash);
    method_block.AppendColumn("methods", method_ids);
    return {{"validator_events", events_block(values(events))},
            {"validator_elections", elections_block(values(elections))},
            {"validator_election_participants", participants_block(values(participants))},
            {"validator_cycles", cycles_block(values(cycles))},
            {"validator_cycle_members", members_block(values(members))},
            {"validator_complaints", complaints_block(values(complaints))},
            {"contract_methods", std::move(method_block)}};
}

void insert(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks) {
    for (const auto& table : prepare_blocks(tasks)) {
        if (table.block.GetRowCount() != 0) client.Insert(table.table, table.block);
    }
}

}  // namespace validator_storage
