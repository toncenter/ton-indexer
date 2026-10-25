#include <iostream>

#include "TotalSupplyPostgres.h"
#include "postgresql_tools.h"

namespace {

InsertTaskStruct task(std::uint32_t seqno, std::uint32_t time, const char* total) {
    auto amount = td::dec_string_to_int256(std::string(total));
    InsertTaskStruct result;
    result.mc_seqno_ = seqno;
    result.parsed_block_ = std::make_shared<ParsedBlock>();
    result.parsed_block_->total_supply_ = schema::TotalSupply{
        seqno, time, amount, amount - 101, td::make_refint(23), td::make_refint(31), td::make_refint(47)};
    return result;
}

long long count(pqxx::connection& connection, const char* table) {
    pqxx::read_transaction txn(connection);
    return txn.query_value<long long>(std::string("SELECT count(*) FROM ") + table);
}

}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);  // The test runner supplies a fresh, private PostgreSQL cluster.
    pqxx::connection connection(argv[1]);
    pqxx::connection observer(argv[1]);
    {
        pqxx::work txn(connection);
        gramstat::postgres::ensure_schema(txn);
        gramstat::postgres::ensure_schema(txn);
        txn.exec0("CREATE TABLE blocks (workchain int, seqno bigint, shard bigint DEFAULT 0, "
                  "mc_block_seqno bigint DEFAULT 0, transaction_fees numeric(38,0), import_fees numeric(38,0), "
                  "created numeric(38,0), burned numeric(38,0), "
                  "PRIMARY KEY (workchain, shard, seqno, mc_block_seqno))");
        txn.commit();
    }
    std::vector<InsertTaskStruct> tasks;
    tasks.push_back(task(101, 1788224399, "100000000000000000102"));
    tasks.push_back(task(100, 1788224399, "100000000000000000101"));
    tasks.push_back(task(103, 1788228000, "100000000000000000104"));

    // Both writes are invisible to other connections and roll back together.
    {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 100), (-1, 101), (-1, 103)");
        gramstat::postgres::insert_total_supply(txn, tasks);
        CHECK(count(observer, "blocks") == 0);
        CHECK(count(observer, "gramstat.total_supply") == 0);
    }
    CHECK(count(observer, "blocks") == 0);
    CHECK(count(observer, "gramstat.total_supply") == 0);
    {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 100), (-1, 101), (-1, 103)");
        gramstat::postgres::insert_total_supply(txn, tasks);
        txn.commit();
    }
    {
        pqxx::work txn(connection);
        gramstat::postgres::insert_total_supply(txn, tasks);  // retry/prewarm
        txn.commit();
    }
    CHECK(count(observer, "gramstat.total_supply") == 3);

    // Legacy block without metrics: bounded backfill / standby must not skip it.
    const auto completed = std::string("SELECT count(*) FROM blocks WHERE workchain = -1") +
                           gramstat::postgres::completed_block_condition(true);
    {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 104), (0, 105)");
        CHECK(txn.query_value<long long>(completed) == 3);
        CHECK(std::string(gramstat::postgres::completed_block_condition(false)).empty());
        txn.commit();
    }
    std::vector<InsertTaskStruct> repair;
    repair.push_back(task(104, 1788307200, "100000000000000000105"));
    {
        pqxx::work txn(connection);
        gramstat::postgres::insert_total_supply(txn, repair);
        CHECK(txn.query_value<long long>(completed) == 4);
        txn.commit();
    }
    {
        pqxx::read_transaction txn(connection);
        // UTC buckets, deterministic ordering for equal-second MCs, exact values.
        const std::string points = R"SQL(
SELECT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), total::text
FROM (
    SELECT DISTINCT ON (bucket)
        date_trunc($1, to_timestamp(gen_utime), 'UTC') AS bucket, total
    FROM gramstat.total_supply
    WHERE gen_utime >= 1788220800 AND gen_utime < 1790812800
    ORDER BY bucket, gen_utime DESC, mc_seqno DESC
) points ORDER BY bucket
)SQL";
        auto hours = txn.exec(points, pqxx::params{"hour"});
        CHECK(hours.size() == 3);
        CHECK(hours[0][0].as<std::string>() == "2026-09-01 00:00:00");
        CHECK(hours[0][1].as<std::string>() == "100000000000000000102");
        CHECK(hours[1][0].as<std::string>() == "2026-09-01 02:00:00");
        auto days = txn.exec(points, pqxx::params{"day"});
        CHECK(days.size() == 2);
        CHECK(days[0][1].as<std::string>() == "100000000000000000104");
        CHECK(days[1][1].as<std::string>() == "100000000000000000105");
        auto components = txn.exec("SELECT accounts::text, dispatch_queue, unprocessed_messages, "
                                   "unrecovered_validator_fees FROM gramstat.total_supply WHERE mc_seqno = 100").one_row();
        CHECK(components[0].as<std::string>() == "100000000000000000000");
        CHECK(components[1].as<int>() == 23 && components[2].as<int>() == 31 && components[3].as<int>() == 47);
    }

    // An application-level missing snapshot and a database write failure must
    // both roll back the block insert, not leave an apparently completed MC.
    std::vector<InsertTaskStruct> bad;
    bad.push_back(task(106, 1788307201, "100000000000000000106"));
    bad[0].parsed_block_->total_supply_.reset();
    bool rejected = false;
    try {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 106)");
        gramstat::postgres::insert_total_supply(txn, bad);
        txn.commit();
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    CHECK(rejected && count(observer, "blocks") == 5);
    bad.clear();
    bad.push_back(task(106, 1788307201, "100000000000000000000000000000000000000"));
    rejected = false;
    try {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 106)");
        gramstat::postgres::insert_total_supply(txn, bad);
        txn.commit();
    } catch (const pqxx::sql_error&) {
        rejected = true;
    }
    CHECK(rejected && count(observer, "blocks") == 5);
    CHECK(count(observer, "gramstat.total_supply") == 4);
    {
        pqxx::work txn(connection);
        CHECK(txn.query_value<long long>("SELECT count(*) FROM blocks WHERE transaction_fees IS NULL") == 5);
        // A previous fees-only replay must not prevent filling created/burned.
        txn.exec0("UPDATE blocks SET transaction_fees=100000000000000000001, import_fees=23 WHERE seqno=100");
        std::initializer_list<std::string_view> columns = {
            "workchain", "shard", "seqno", "mc_block_seqno", "transaction_fees", "import_fees", "created", "burned"};
        for (int retry = 0; retry < 2; ++retry) {
            PopulateTableStream stream(txn, "blocks", columns, 1000);
            configure_block_aggregates_upsert(stream);
            stream.insert_row(std::make_tuple(-1, 0, 100, 0,
                td::dec_string_to_int256(std::string("100000000000000000001")), td::make_refint(23),
                td::dec_string_to_int256(std::string("100000000000000000007")), td::make_refint(0)));
            stream.finish();
        }
        CHECK(txn.query_value<std::string>("SELECT (transaction_fees + import_fees)::text "
                                           "FROM blocks WHERE seqno = 100") == "100000000000000000024");
        CHECK(txn.query_value<long long>("SELECT count(*) FROM blocks WHERE transaction_fees IS NULL") == 4);
        CHECK(txn.query_value<std::string>("SELECT created::text FROM blocks WHERE seqno=100") == "100000000000000000007");
        CHECK(txn.query_value<int>("SELECT burned FROM blocks WHERE seqno=100") == 0);
        CHECK(txn.query_value<long long>("SELECT count(*) FROM blocks WHERE created IS NULL OR burned IS NULL") == 4);
        CHECK(txn.query_value<long long>("SELECT count(*) FROM blocks") == 5);
        txn.commit();
    }
    std::cout << "PostgreSQL Gramstat exact amounts, atomic writes, retries, backfill and charts: OK\n";
}
