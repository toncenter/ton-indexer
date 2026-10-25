#include <iostream>
#include <sstream>
#include <stdexcept>

#include "TotalSupply.h"
#include "TotalSupplyStorage.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "td/actor/TestScheduler.h"

namespace {

void check_amounts_and_columns() {
    ton::validator::GlobalBalanceComponents c;
    // Larger than UInt64 and far above Float64's exact integer range.
    c.accounts = td::dec_string_to_int256(std::string("100000000000000000001"));
    c.dispatch_queue = td::make_refint(23);
    c.unprocessed_messages = td::make_refint(31);
    c.unrecovered_validator_fees = td::make_refint(47);
    c.total = td::dec_string_to_int256(std::string("100000000000000000102"));
    c.burned = td::make_refint(7);
    auto snapshot = gramstat::make_total_supply(100, 1788220800, c).move_as_ok();
    CHECK(snapshot.total->to_dec_string() == "100000000000000000102");

    auto block = gramstat::total_supply_block({snapshot});
    CHECK(block.GetRowCount() == 1 && block.GetColumnCount() == 7);
    CHECK(block[0]->As<clickhouse::ColumnUInt32>()->At(0) == 100);
    auto time = block[1]->As<clickhouse::ColumnDateTime>();
    CHECK(time->Timezone() == "UTC" && time->RawAt(0) == 1788220800);
    const char* expected[] = {"100000000000000000102", "100000000000000000001", "23", "31", "47"};
    for (size_t i = 0; i < 5; ++i) {
        auto column = block[i + 2]->As<clickhouse::ColumnDecimal>();
        CHECK(column->GetPrecision() == 38 && column->GetScale() == 0);
        std::ostringstream text;
        text << column->At(0);
        CHECK(text.str() == expected[i]);
    }

    c.total = c.total + 1;
    CHECK(gramstat::make_total_supply(100, 1788220800, c).is_error());
    c.total = td::dec_string_to_int256(std::string("100000000000000000102"));
    c.accounts.clear();
    CHECK(gramstat::make_total_supply(100, 1788220800, c).is_error());
    c.accounts = td::make_refint(-1);
    CHECK(gramstat::make_total_supply(100, 1788220800, c).is_error());

    c = {};
    c.total = c.accounts = td::dec_string_to_int256(std::string("99999999999999999999999999999999999999"));
    auto max_snapshot = gramstat::make_total_supply(101, 1788220801, c).move_as_ok();
    auto max_block = gramstat::total_supply_block({max_snapshot});
    std::ostringstream maximum;
    maximum << max_block[2]->As<clickhouse::ColumnDecimal>()->At(0);
    CHECK(maximum.str() == "99999999999999999999999999999999999999");
    c.total = c.accounts = td::dec_string_to_int256(std::string("100000000000000000000000000000000000000"));
    CHECK(gramstat::make_total_supply(101, 1788220801, c).is_error());
    max_snapshot.total = c.total;
    bool rejected = false;
    try {
        gramstat::total_supply_block({max_snapshot});
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    CHECK(rejected);
}

void check_incomplete_input() {
    td::actor::TestScheduler scheduler;
    bool called = false;
    gramstat::StateLoader loader = [&called](ton::BlockIdExt)
        -> td::actor::Task<td::Ref<ton::validator::ShardState>> {
        called = true;
        co_return td::Status::Error("Unexpected loader call");
    };
    scheduler.run([&]() -> td::actor::Task<> {
        auto empty = co_await gramstat::calculate_total_supply({}, loader).wrap();
        CHECK(empty.is_error());
        schema::MasterchainBlockDataState incomplete;
        incomplete.shard_blocks_.push_back({});
        auto missing = co_await gramstat::calculate_total_supply(std::move(incomplete), loader).wrap();
        CHECK(missing.is_error());
        co_return td::Unit{};
    });
    CHECK(!called);
}

}  // namespace

int main(int argc, char** argv) {
    td::Time::allow_freezes();
    if (argc == 2 && std::string(argv[1]) == "--schema") {
        std::cout << gramstat::CREATE_DATABASE << ";\n" << gramstat::CREATE_TOTAL_SUPPLY << ";\n";
        return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--completed-query") {
        std::cout << gramstat::completed_seqnos_query(true, 100, 105);
        return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--legacy-query") {
        std::cout << gramstat::completed_seqnos_query(false, 100, 105);
        return 0;
    }
    CHECK(argc == 1);
    check_amounts_and_columns();
    check_incomplete_input();
    std::cout << "Total supply adapter and exact ClickHouse columns: OK\n";
}
