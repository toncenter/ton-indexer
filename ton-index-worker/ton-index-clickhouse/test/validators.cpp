#include "ValidatorStorage.h"

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"

#include <iostream>
#include <limits>
#include <sstream>

namespace {

constexpr uint32_t ROUND = 1788220800;
constexpr uint32_t ELECTION = 1788000000;
constexpr const char* LARGE = "900719925474099312345";
constexpr const char* FINED = "900719925474099312300";
constexpr const char* MAX_COINS = "1329227995784915872903807060280344575";
const std::string PUBKEY(64, 'A');
const std::string ADNL(64, 'B');
const std::string HOLDER = "-1:" + std::string(64, 'C');

td::RefInt256 coins(const char* value) { return td::dec_string_to_int256(std::string(value)); }

InsertTaskStruct fixture(uint32_t source) {
    InsertTaskStruct task;
    task.mc_seqno_ = source;
    task.parsed_block_ = std::make_shared<ParsedBlock>();
    auto& block = *task.parsed_block_;
    auto stake = coins(source == 100 ? LARGE : FINED);
    schema::ValidatorEvent event{};
    event.transaction_hash = td::Bits256::zero();
    event.transaction_lt = 9007199254740993ULL + source;
    event.transaction_now = ROUND;
    event.mc_seqno = source;
    event.event_type = source == 100 ? "stake_accepted" : "recover_failed";
    event.stake_holder_address = HOLDER;
    event.amount = source == 100 ? stake : td::make_refint(0);
    event.query_id = std::numeric_limits<uint64_t>::max();
    if (source == 100) {
        event.validator_pubkey = PUBKEY;
        event.adnl_addr = ADNL;
        event.election_id = ELECTION;
    } else event.reason = -17;
    block.events_.push_back(event);

    schema::ValidatorElection election{};
    election.election_id = ELECTION;
    election.elect_close = ELECTION + 100;
    election.min_stake = td::make_refint(1);
    election.total_stake = stake;
    election.finished = source >= 200;
    election.source_mc_seqno = source;
    election.participants.push_back({ELECTION, PUBKEY, stake, 65536, HOLDER, ADNL, source});
    if (source == 100) {
        // A different holder with the same pubkey has a distinct participant key.
        election.participants.push_back({ELECTION, PUBKEY, td::make_refint(7), 65536, "-1:other", ADNL, source});
    }
    block.validator_elections_.push_back(election);

    schema::ValidatorCycle cycle{};
    if (source != 300) {
        cycle.election_id = ELECTION;
        cycle.total_stake = stake;
    }
    cycle.utime_since = ROUND;
    cycle.utime_until = ROUND + 65536;
    cycle.total = cycle.main = 1;
    cycle.total_weight = std::numeric_limits<uint64_t>::max();
    cycle.source_mc_seqno = source;
    cycle.validators_elected_for = 65536;
    cycle.elections_start_before = 32768;
    cycle.elections_end_before = 8192;
    cycle.stake_held_for = source == 300 ? 32769 : 32768;
    cycle.max_validators = 400;
    cycle.max_main_validators = 100;
    cycle.min_validators = 13;
    cycle.min_stake = td::make_refint(1);
    cycle.max_stake = coins(MAX_COINS);
    cycle.min_total_stake = td::make_refint(2);
    cycle.max_stake_factor = 196608;
    cycle.members.push_back({ROUND, 0, PUBKEY, ADNL, cycle.total_weight, cycle.total_stake, source});
    block.validator_cycles_.push_back(cycle);
    if (source == 300) {
        // Entirely unknown enrichment must stay NULL, while a known zero stays zero.
        cycle.utime_since += 65536;
        cycle.utime_until += 65536;
        cycle.members[0].utime_since = cycle.utime_since;
        block.validator_cycles_.push_back(cycle);
        cycle.utime_since += 65536;
        cycle.utime_until += 65536;
        cycle.total_stake = td::make_refint(0);
        cycle.members[0].utime_since = cycle.utime_since;
        cycle.members[0].stake = td::make_refint(0);
        block.validator_cycles_.push_back(cycle);
    }

    schema::ValidatorComplaint complaint{};
    complaint.election_id = ELECTION;
    complaint.complaint_hash = std::string(64, 'D');
    complaint.validator_pubkey = PUBKEY;
    if (source != 300) complaint.adnl_addr = ADNL;
    complaint.description_boc = "description'\\line\n";
    complaint.created_at = ROUND;
    complaint.severity = 10;
    complaint.reward_address = HOLDER;
    complaint.paid = td::make_refint(0);
    complaint.suggested_fine = coins(LARGE);
    complaint.suggested_fine_part = 1234;
    complaint.voted_validators = source == 100 ? "[0]" : "[0,1]";
    complaint.vset_id = std::string(64, 'E');
    complaint.weight_remaining = source == 100 ? 42 : -17;
    complaint.approved_percent = source == 100 ? 10 : 100;
    complaint.is_passed = source != 100;
    complaint.source_mc_seqno = source;
    block.validator_complaints_.push_back(complaint);
    block.contract_methods_.emplace(td::Bits256::zero(), 97026);
    block.contract_methods_.emplace(td::Bits256::zero(), 85143);
    block.contract_methods_.emplace(td::Bits256::zero(), 97026);
    return task;
}

const clickhouse::Block& table(const std::vector<validator_storage::TableBlock>& tables, const char* name) {
    for (const auto& row : tables) if (row.table == name) return row.block;
    throw std::runtime_error("Missing fixture table");
}

template<class... Tasks>
std::vector<validator_storage::TableBlock> prepare(Tasks&&... tasks) {
    std::vector<InsertTaskStruct> batch;
    (batch.push_back(std::forward<Tasks>(tasks)), ...);
    return validator_storage::prepare_blocks(batch);
}

std::string quote(std::string_view value) {
    std::string result = "'";
    for (char c : value) {
        if (c == '\\' || c == '\'') result += '\\';
        if (c == '\n') result += "\\n";
        else result += c;
    }
    return result + "'";
}

// Export the actual writer's columns, rather than a separate hand-written SQL fixture.
std::string sql_value(const clickhouse::ColumnRef& column, size_t row) {
    using namespace clickhouse;
    std::ostringstream out;
    switch (column->Type()->GetCode()) {
        case Type::Nullable: {
            auto c = column->As<ColumnNullable>();
            return c->IsNull(row) ? "NULL" : sql_value(c->Nested(), row);
        }
        case Type::String: return quote(column->As<ColumnString>()->At(row));
        case Type::FixedString: return quote(column->As<ColumnFixedString>()->At(row));
        case Type::UInt8: out << unsigned(column->As<ColumnUInt8>()->At(row)); break;
        case Type::UInt32: out << column->As<ColumnUInt32>()->At(row); break;
        case Type::UInt64: out << column->As<ColumnUInt64>()->At(row); break;
        case Type::Int32: out << column->As<ColumnInt32>()->At(row); break;
        case Type::Int64: out << column->As<ColumnInt64>()->At(row); break;
        case Type::Float64: out << column->As<ColumnFloat64>()->At(row); break;
        case Type::Decimal: case Type::Decimal128:
            out << column->As<ColumnDecimal>()->At(row); break;
        case Type::Array: {
            out << '[';
            bool first = true;
            for (auto value : column->As<ColumnArrayT<ColumnUInt64>>()->At(row)) {
                if (!first) out << ',';
                out << value;
                first = false;
            }
            out << ']';
            break;
        }
        default: throw std::runtime_error("Unexpected fixture column type");
    }
    return out.str();
}

void print_inserts(const std::vector<validator_storage::TableBlock>& tables) {
    for (const auto& row : tables) {
        const auto& b = row.block;
        if (!b.GetRowCount()) continue;
        std::cout << "INSERT INTO " << row.table << " (";
        for (size_t col = 0; col < b.GetColumnCount(); ++col) {
            std::cout << (col ? "," : "") << b.GetColumnName(col);
        }
        std::cout << ") VALUES ";
        for (size_t i = 0; i < b.GetRowCount(); ++i) {
            std::cout << (i ? ",(" : "(");
            for (size_t col = 0; col < b.GetColumnCount(); ++col) {
                std::cout << (col ? "," : "") << sql_value(b[col], i);
            }
            std::cout << ')';
        }
        std::cout << ";\n";
    }
}

}  // namespace

void print_validator_schema() {
    for (const auto& query : validator_storage::schema_queries()) std::cout << query << ";\n";
}

void print_validator_fixture(bool corrected) {
    if (corrected) {
        auto task = fixture(100);
        auto& cycle = task.parsed_block_->validator_cycles_[0];
        cycle.total_stake = coins("900719925474099312320");
        cycle.members[0].stake = cycle.total_stake;
        print_inserts(prepare(std::move(task)));
    } else {
        // Reverse completion order, then duplicate retries in separate batches.
        for (auto source : {300, 200, 100, 300, 200}) {
            print_inserts(prepare(fixture(source)));
        }
    }
}

void check_validator_storage() {
    using namespace clickhouse;
    for (const auto& t : validator_storage::prepare_blocks({})) CHECK(t.block.GetRowCount() == 0);
    auto tables = prepare(fixture(300), fixture(100), fixture(200), fixture(100));
    const auto& events = table(tables, "validator_events");
    CHECK(events.GetRowCount() == 3);
    CHECK(events[9]->As<ColumnNullableT<ColumnUInt64>>()->At(0).value() == std::numeric_limits<uint64_t>::max());
    CHECK(sql_value(events[10], 0) == LARGE);
    CHECK(sql_value(events[8], 1) == "NULL" && sql_value(events[11], 1) == "-17");
    const auto& elections = table(tables, "validator_elections");
    CHECK(elections.GetRowCount() == 1 && elections[6]->As<ColumnUInt32>()->At(0) == 300);
    CHECK(table(tables, "validator_election_participants").GetRowCount() == 2);
    const auto& cycles = table(tables, "validator_cycles");
    CHECK(cycles.GetRowCount() == 5 && cycles.GetColumnCount() == 19);
    CHECK(sql_value(cycles[6], 0) == LARGE && sql_value(cycles[6], 2) == "NULL");
    CHECK(sql_value(cycles[6], 3) == "NULL" && sql_value(cycles[6], 4) == "0");
    CHECK(sql_value(cycles[16], 0) == MAX_COINS);
    const auto& members = table(tables, "validator_cycle_members");
    CHECK(members.GetRowCount() == 5 && sql_value(members[4], 0) == "18446744073709551615");
    const auto& complaints = table(tables, "validator_complaints");
    CHECK(complaints.GetRowCount() == 1 && sql_value(complaints[3], 0) == "NULL");
    CHECK(sql_value(complaints[13], 0) == "-17" && sql_value(complaints[16], 0) == "300");
    const auto& methods = table(tables, "contract_methods");
    CHECK(methods.GetRowCount() == 1 && sql_value(methods[1], 0) == "[85143,97026]");

    for (int defect = 0; defect < 5; ++defect) {
        auto bad = fixture(100);
        if (defect == 0) bad.mc_seqno_ = 101;
        if (defect == 1) bad.parsed_block_->validator_elections_[0].participants[0].election_id++;
        if (defect == 2) bad.parsed_block_->validator_cycles_[0].members[0].utime_since++;
        if (defect == 3) bad.parsed_block_->validator_complaints_[0].paid = td::make_refint(-1);
        if (defect == 4) bad.parsed_block_->validator_elections_[0].total_stake.clear();
        bool rejected = false;
        try { prepare(std::move(bad)); } catch (const std::runtime_error&) { rejected = true; }
        CHECK(rejected);
    }
}
