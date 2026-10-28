#include <iostream>

#include "GramstatPostgres.h"
#include "JettonWalletStatesPostgres.h"
#include "JettonWalletStatesSchema.h"
#include "postgresql_tools.h"

namespace {

InsertTaskStruct task(std::uint32_t seqno, std::uint32_t time, const char* total) {
    auto amount = td::dec_string_to_int256(std::string(total));
    InsertTaskStruct result;
    result.mc_seqno_ = seqno;
    result.parsed_block_ = std::make_shared<ParsedBlock>();
    result.parsed_block_->total_supply_ = schema::TotalSupply{
        seqno, time, amount, amount - 101, td::make_refint(23), td::make_refint(31), td::make_refint(47)};
    result.parsed_block_->account_deltas_ = schema::AccountDeltas{seqno, time, -1, 2, 0};
    result.parsed_block_->deployments_.emplace();
    result.parsed_block_->jetton_wallet_states_.emplace();
    return result;
}

long long count(pqxx::connection& connection, const char* table) {
    pqxx::read_transaction txn(connection);
    return txn.query_value<long long>(std::string("SELECT count(*) FROM ") + table);
}

void check_jetton_history(pqxx::connection& connection, pqxx::connection& observer) {
    {
        pqxx::work txn(connection);
        txn.exec0(jetton_history::postgres::SCHEMA_SQL);
        txn.exec0(jetton_history::postgres::SCHEMA_SQL);
        txn.commit();
    }
    const auto address = block::StdAddress(0, td::Bits256::zero());
    const std::string amount = "100000000000000000000000000000000000000000000000001";
    std::vector<InsertTaskStruct> tasks;
    for (auto seqno : {101U, 100U, 102U, 103U}) {
        auto item = task(seqno, 1788220800, "1");
        // History has no dependency on optional Gramstat snapshots or Kvrocks.
        item.parsed_block_->total_supply_.reset();
        item.parsed_block_->account_deltas_.reset();
        schema::JettonWalletState row{address, seqno, 1788220800, 9007199254740993ULL + seqno, std::nullopt, {}};
        if (seqno == 100 || seqno == 103) {
            row.existing = true;
            row.balance = seqno == 100 ? td::dec_string_to_int256(amount) : td::make_refint(0);
        } else if (seqno == 102) {
            row.existing = false;
        }
        item.parsed_block_->jetton_wallet_states_->push_back(row);
        tasks.push_back(std::move(item));
    }
    {
        pqxx::work txn(connection);
        jetton_history::postgres::insert_states(txn, tasks);
        CHECK(count(observer, "jetton_wallet_states") == 0);
    }
    CHECK(count(observer, "jetton_wallet_states") == 0);
    {
        pqxx::work txn(connection);
        jetton_history::postgres::insert_states(txn, tasks);
        jetton_history::postgres::insert_states(txn, tasks);
        auto rows = txn.exec("SELECT existing, balance::text, nonzero FROM jetton_wallet_states ORDER BY mc_seqno");
        CHECK(rows.size() == 4 && rows[0][1].as<std::string>() == amount && rows[0][2].as<bool>());
        CHECK(rows[1][0].is_null() && rows[1][1].is_null() && rows[1][2].is_null());
        CHECK(!rows[2][0].as<bool>() && rows[2][1].is_null() && !rows[2][2].as<bool>());
        CHECK(rows[3][0].as<bool>() && rows[3][1].as<std::string>() == "0" && !rows[3][2].as<bool>());
        txn.commit();
    }
    for (int failure = 0; failure < 3; ++failure) {
        auto bad = task(104, 1788220800, "1");
        if (failure == 0) {
            bad.parsed_block_->jetton_wallet_states_.reset();
        } else {
            bad.parsed_block_->jetton_wallet_states_->push_back({address, failure == 1 ? 99U : 104U,
                1788220800, 200, false, failure == 2 ? td::make_refint(1) : td::RefInt256{}});
        }
        bool rejected = false;
        std::vector<InsertTaskStruct> bad_tasks;
        bad_tasks.push_back(std::move(bad));
        try {
            pqxx::work txn(connection);
            jetton_history::postgres::insert_states(txn, bad_tasks);
            txn.commit();
        } catch (const std::exception&) {
            rejected = true;
        }
        CHECK(rejected && count(observer, "jetton_wallet_states") == 4);
    }
    {
        pqxx::work txn(connection);
        auto& repaired = tasks[0].parsed_block_->jetton_wallet_states_->front();
        repaired.existing = true;
        repaired.balance = td::make_refint(5);
        jetton_history::postgres::insert_states(txn, tasks);
        CHECK(txn.query_value<bool>("SELECT nonzero FROM jetton_wallet_states WHERE mc_seqno=101"));
        std::vector<InsertTaskStruct> empty;
        empty.push_back(task(104, 1788220800, "1"));
        jetton_history::postgres::insert_states(txn, empty);
        txn.commit();
    }
    CHECK(count(observer, "jetton_wallet_states") == 4);
}

}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);  // The test runner supplies a fresh, private PostgreSQL cluster.
    pqxx::connection connection(argv[1]);
    pqxx::connection observer(argv[1]);
    {
        pqxx::work txn(connection);
        // The ordinary migrations create these domains before Gramstat's 1.3.2 DDL.
        txn.exec0("CREATE DOMAIN tonaddr AS varchar; CREATE DOMAIN tonhash AS char(44);");
        gramstat::postgres::ensure_schema(txn);
        gramstat::postgres::ensure_schema(txn);
        txn.exec0("CREATE TABLE blocks (workchain int, seqno bigint, shard bigint DEFAULT 0, "
                  "mc_block_seqno bigint DEFAULT 0, transaction_fees numeric(38,0), import_fees numeric(38,0), "
                  "created numeric(38,0), burned numeric(38,0), gen_utime bigint DEFAULT 0, "
                  "PRIMARY KEY (workchain, shard, seqno, mc_block_seqno))");
        txn.commit();
    }
    check_jetton_history(connection, observer);
    std::vector<InsertTaskStruct> tasks;
    tasks.push_back(task(101, 1788224399, "100000000000000000102"));
    tasks.push_back(task(100, 1788224399, "100000000000000000101"));
    tasks.push_back(task(103, 1788228000, "100000000000000000104"));
    const auto address = block::StdAddress(0, td::Bits256::zero());
    const auto hash = td::Bits256::zero();
    tasks[0].parsed_block_->deployments_->push_back({101, 1788220799, address, 9007199254740993ULL, hash, hash, 0});
    tasks[0].parsed_block_->deployments_->push_back({101, 1788220800, address, 9007199254740994ULL, hash, std::nullopt, 2});

    // Both writes are invisible to other connections and roll back together.
    {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 100), (-1, 101), (-1, 103)");
        gramstat::postgres::insert_total_supply(txn, tasks);
        gramstat::postgres::insert_deployments(txn, tasks);
        gramstat::postgres::insert_account_deltas(txn, tasks);
        CHECK(count(observer, "blocks") == 0);
        CHECK(count(observer, "gramstat.total_supply") == 0);
        CHECK(count(observer, "gramstat.account_deltas") == 0);
        CHECK(count(observer, "gramstat.deployments") == 0);
    }
    CHECK(count(observer, "blocks") == 0);
    CHECK(count(observer, "gramstat.total_supply") == 0);
    CHECK(count(observer, "gramstat.account_deltas") == 0);
    CHECK(count(observer, "gramstat.deployments") == 0);
    {
        pqxx::work txn(connection);
        txn.exec0("INSERT INTO blocks VALUES (-1, 100), (-1, 101), (-1, 103)");
        gramstat::postgres::insert_total_supply(txn, tasks);
        gramstat::postgres::insert_deployments(txn, tasks);
        gramstat::postgres::insert_account_deltas(txn, tasks);
        txn.commit();
    }
    {
        pqxx::work txn(connection);
        gramstat::postgres::insert_total_supply(txn, tasks);  // retry/prewarm
        gramstat::postgres::insert_deployments(txn, tasks);
        gramstat::postgres::insert_account_deltas(txn, tasks);
        txn.commit();
    }
    CHECK(count(observer, "gramstat.total_supply") == 3);
    CHECK(count(observer, "gramstat.account_deltas") == 3);
    CHECK(count(observer, "gramstat.deployments") == 2);
    {
        pqxx::work txn(connection);
        auto rows = txn.exec("SELECT lt, gen_utime, init_code_hash::text, out_msgs_count "
                             "FROM gramstat.deployments ORDER BY lt");
        CHECK(rows[0][0].as<long long>() == 9007199254740993LL);
        CHECK(rows[0][1].as<int>() == 1788220799);
        CHECK(rows[0][2].as<std::string>() == td::base64_encode(hash.as_slice()));
        CHECK(rows[1][2].is_null() && rows[1][3].as<int>() == 2);
        tasks[0].parsed_block_->deployments_->front().out_msgs_count = 3;
        gramstat::postgres::insert_deployments(txn, tasks);
        CHECK(txn.query_value<int>("SELECT out_msgs_count FROM gramstat.deployments WHERE lt=9007199254740993") == 3);
        CHECK(txn.query_value<int>("SELECT count(*) FROM gramstat.deployments") == 2);
        txn.commit();
    }

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
        CHECK(txn.query_value<long long>(completed) == 3);  // supply alone is insufficient
        gramstat::postgres::insert_deployments(txn, repair);  // calculated zero events
        CHECK(txn.query_value<long long>(completed) == 3);
        gramstat::postgres::insert_account_deltas(txn, repair);
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
    // Missing or mismatched deltas must roll back both the block and Supply.
    for (bool missing : {true, false}) {
        bad.clear();
        bad.push_back(task(106, 1788307201, "100000000000000000106"));
        if (missing) {
            bad[0].parsed_block_->account_deltas_.reset();
        } else {
            bad[0].parsed_block_->account_deltas_->gen_utime++;
        }
        rejected = false;
        try {
            pqxx::work txn(connection);
            txn.exec0("INSERT INTO blocks VALUES (-1, 106)");
            gramstat::postgres::insert_total_supply(txn, bad);
            gramstat::postgres::insert_account_deltas(txn, bad);
            txn.commit();
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        CHECK(rejected && count(observer, "blocks") == 5);
        CHECK(count(observer, "gramstat.total_supply") == 4);
        CHECK(count(observer, "gramstat.account_deltas") == 4);
    }
    // Missing results, wrong MC ownership and a DB error all roll back deployments
    // with the rest of the batch, never leaving the completion marker behind.
    for (int failure = 0; failure < 3; ++failure) {
        bad.clear();
        bad.push_back(task(106, 1788307201, "100000000000000000106"));
        if (failure == 0) {
            bad[0].parsed_block_->deployments_.reset();
        } else {
            bad[0].parsed_block_->deployments_->push_back({106, 1788307201, address, 20, hash, hash, 0});
            bad[0].parsed_block_->deployments_->push_back({failure == 1 ? 99U : 106U, 1788307201,
                address, 21, hash, hash, failure == 2 ? UINT32_MAX : 0});
        }
        rejected = false;
        try {
            pqxx::work txn(connection);
            txn.exec0("INSERT INTO blocks VALUES (-1, 106)");
            gramstat::postgres::insert_total_supply(txn, bad);
            gramstat::postgres::insert_deployments(txn, bad);
            gramstat::postgres::insert_account_deltas(txn, bad);
            txn.commit();
        } catch (const std::exception&) {
            rejected = true;
        }
        CHECK(rejected && count(observer, "blocks") == 5);
        CHECK(count(observer, "gramstat.total_supply") == 4);
        CHECK(count(observer, "gramstat.account_deltas") == 4);
        CHECK(count(observer, "gramstat.deployments") == 2);
    }
    // A complete small prefix for the documented chart SQL (run by the runner).
    std::vector<InsertTaskStruct> history;
    for (const auto& delta : std::vector<schema::AccountDeltas>{
        {1, 1788220799, 10, 20, 1}, {3, 1788307199, 0, -1, 1},
        {2, 1788220800, -2, 3, 0}, {4, 1788307200, 0, 0, 0},
        {5, 1788350400, 100, 0, -1}, {7, 1788354000, 0, 0, 0}}) {
        auto row = task(delta.mc_seqno, delta.gen_utime, "100000000000000000101");
        row.parsed_block_->account_deltas_ = delta;
        history.push_back(std::move(row));
    }
    {
        pqxx::work txn(connection);
        gramstat::postgres::insert_account_deltas(txn, history);
        txn.commit();
    }
    history[4].parsed_block_->account_deltas_->delta_uninit = 1;
    {
        pqxx::work txn(connection);
        gramstat::postgres::insert_account_deltas(txn, history);  // corrected replay
        gramstat::postgres::insert_account_deltas(txn, history);  // identical replay
        CHECK(txn.query_value<int>("SELECT delta_uninit FROM gramstat.account_deltas WHERE mc_seqno=5") == 1);
        CHECK(txn.query_value<long long>("SELECT count(*) FROM gramstat.account_deltas") == 10);
        txn.commit();
    }
    std::cout << "PostgreSQL Gramstat exact amounts, atomic writes, retries, backfill and charts: OK\n";
}
