#include <iostream>
#include <sstream>
#include <stdexcept>

#include "TotalSupply.h"
#include "AccountDeltas.h"
#include "Deployments.h"
#include "GramstatStorage.h"
#include "DataParser.h"
#include "BlockInterfacesDetector.h"
#include "TokenStatesStorage.h"
#include "AccountStateHistory.h"
#include "convert-utils.h"
#include "../../tondb-scanner/test/smc-fixture.h"
#include "vm/cp0.h"
#include "vm/db/DynamicBagOfCellsDb.h"
#include "crypto/block/block.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/string.h"
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

void check_block_fees() {
    block::gen::BlockExtra::Record extra;
    schema::Block result;
    extra.account_blocks = vm::AugmentedDictionary(256, block::tlb::aug_ShardAccountBlocks).get_wrapped_dict_root();
    extra.in_msg_descr = vm::AugmentedDictionary(256, block::tlb::aug_InMsgDescrDefault).get_wrapped_dict_root();
    ParseQuery::parse_block_fees(extra, result).ensure();
    CHECK(result.transaction_fees->sgn() == 0 && result.import_fees->sgn() == 0);

    // Root-augmentation fixtures: a single zero-key leaf with an exact amount
    // larger than UInt64. The parser only reads the stored aggregate, not leaves.
    auto wrap_aggregate = [](td::Ref<vm::Cell> aggregate) {
        vm::CellBuilder leaf, root;
        leaf.store_long(6, 3).store_long(256, 9);  // hml_same$11, v=0, n=256
        leaf.append_cellslice(vm::load_cell_slice(aggregate));
        root.store_long(1, 1).store_ref(leaf.finalize());
        root.append_cellslice(vm::load_cell_slice(aggregate));
        return root.finalize();
    };
    auto amount = td::dec_string_to_int256(std::string("100000000000000000001"));
    vm::CellBuilder transactions, imports;
    CHECK(block::CurrencyCollection(amount).store(transactions));
    CHECK(block::tlb::t_Grams.store_integer_ref(imports, td::make_refint(23)));
    CHECK(block::CurrencyCollection(999).store(imports));  // imported value is NOT a fee
    extra.account_blocks = wrap_aggregate(transactions.finalize());
    extra.in_msg_descr = wrap_aggregate(imports.finalize());
    ParseQuery::parse_block_fees(extra, result).ensure();
    CHECK(td::cmp(result.transaction_fees, amount) == 0 && td::cmp(result.import_fees, 23) == 0);
    extra.in_msg_descr = vm::CellBuilder().finalize();
    CHECK(ParseQuery::parse_block_fees(extra, result).is_error());
}

void check_block_value_flow() {
    block::ValueFlow flow{block::ValueFlow::SetZero{}};
    flow.created = block::CurrencyCollection(td::dec_string_to_int256(std::string("100000000000000000001")));
    schema::Block result;
    // store() uses the legacy constructor when burned=0, v2 otherwise.
    for (int burned : {0, 23}) {
        flow.burned = block::CurrencyCollection(burned);
        vm::CellBuilder value_flow;
        CHECK(flow.store(value_flow));
        ParseQuery::parse_block_value_flow(value_flow.finalize(), result).ensure();
        CHECK(td::cmp(result.created, flow.created.grams) == 0);
        CHECK(td::cmp(result.burned, burned) == 0);
    }
    CHECK(ParseQuery::parse_block_value_flow(vm::CellBuilder().finalize(), result).is_error());
}

void check_account_deltas() {
    using S = schema::AccountStatus;
    ParsedBlock parsed;
    for (int wc : {-1, 0, 1}) {
        schema::Block block{};
        block.workchain = wc;
        block.seqno = wc == -1 ? 100 : 20;
        block.gen_utime = wc == -1 ? 1788307200 : 1788307199;
        block.mc_block_workchain = -1;
        block.mc_block_seqno = 100;
        parsed.blocks_.push_back(block);
    }
    auto add = [&](std::size_t index, S before, S after) {
        schema::Transaction tx{};
        tx.mc_seqno = 100;
        tx.account = block::StdAddress(parsed.blocks_[index].workchain, td::Bits256::zero());
        tx.orig_status = before;
        tx.end_status = after;
        parsed.blocks_[index].transactions.push_back(tx);
    };
    auto zero = gramstat::calculate_account_deltas(100, parsed).move_as_ok();
    CHECK(zero.delta_uninit == 0 && zero.delta_active == 0 && zero.delta_frozen == 0);
    add(0, S::nonexist, S::active);
    add(1, S::nonexist, S::uninit);
    add(1, S::uninit, S::active);
    add(1, S::active, S::frozen);
    add(1, S::frozen, S::nonexist);  // same account born and removed within this MC
    add(2, S::uninit, S::active);
    add(2, S::active, S::frozen);
    auto delta = gramstat::calculate_account_deltas(100, parsed).move_as_ok();
    CHECK(delta.mc_seqno == 100 && delta.gen_utime == 1788307200);
    CHECK(delta.delta_uninit == -1 && delta.delta_active == 1 && delta.delta_frozen == 1);
    // Sum all sixteen transitions; self-transitions and opposite pairs cancel.
    for (auto before : {S::nonexist, S::uninit, S::active, S::frozen}) {
        for (auto after : {S::nonexist, S::uninit, S::active, S::frozen}) {
            add(0, before, after);
        }
    }
    auto same = gramstat::calculate_account_deltas(100, parsed).move_as_ok();
    CHECK(same.delta_uninit == -1 && same.delta_active == 1 && same.delta_frozen == 1);
    auto columns = gramstat::account_deltas_block({delta, zero});
    CHECK(columns.GetRowCount() == 2 && columns.GetColumnCount() == 5);
    CHECK(columns[0]->As<clickhouse::ColumnUInt32>()->At(0) == 100);
    CHECK(columns[1]->As<clickhouse::ColumnDateTime>()->RawAt(0) == 1788307200);
    CHECK(columns[2]->As<clickhouse::ColumnInt64>()->At(0) == -1);
    CHECK(columns[3]->As<clickhouse::ColumnInt64>()->At(0) == 1);
    CHECK(columns[4]->As<clickhouse::ColumnInt64>()->At(1) == 0);

    CHECK(gramstat::calculate_account_deltas(101, parsed).is_error());
    CHECK(gramstat::calculate_account_deltas(100, {}).is_error());
    parsed.blocks_.push_back(parsed.blocks_.front());
    CHECK(gramstat::calculate_account_deltas(100, parsed).is_error());
    parsed.blocks_.pop_back();
    parsed.blocks_.back().mc_block_seqno.reset();
    CHECK(gramstat::calculate_account_deltas(100, parsed).is_error());
    parsed.blocks_.back().mc_block_seqno = 100;
    parsed.blocks_.back().transactions.back().mc_seqno = 99;
    CHECK(gramstat::calculate_account_deltas(100, parsed).is_error());
    parsed.blocks_.back().transactions.back().mc_seqno = 100;
    parsed.blocks_.back().transactions.back().end_status = static_cast<S>(99);
    CHECK(gramstat::calculate_account_deltas(100, parsed).is_error());
}

void check_deployments() {
    using S = schema::AccountStatus;
    ParsedBlock parsed;
    for (int wc : {-1, 0}) {
        schema::Block block{};
        block.workchain = wc;
        block.seqno = wc == -1 ? 100 : 20;
        block.gen_utime = wc == -1 ? 1788307200 : 1788307199;
        block.mc_block_workchain = -1;
        block.mc_block_seqno = 100;
        parsed.blocks_.push_back(block);
    }
    CHECK(gramstat::collect_deployments(100, parsed).move_as_ok().empty());
    auto code = vm::CellBuilder().store_long(42, 8).finalize();
    auto other_code = vm::CellBuilder().store_long(43, 8).finalize();
    auto init = vm::CellBuilder().store_long(0, 2).store_long(1, 1).store_ref(code)
        .store_long(0, 2).finalize();  // no prefix/special/data/library

    // Both inline/reference StateInit, external/internal messages go through the
    // production message parser before collecting deployments.
    auto message = [&](bool external, bool by_ref) {
        vm::CellBuilder b;
        auto address = [&]() { b.store_long(4, 3).store_long(0, 8).store_zeroes(256); };
        if (external) {
            b.store_long(2, 2).store_long(0, 2);  // ext_in, addr_none
            address();
            b.store_long(0, 4);  // import_fee
        } else {
            b.store_long(4, 4);  // internal, ihr_disabled, not bounce/bounced
            address();
            address();
            b.store_zeroes(5 + 4 + 4 + 64 + 32);  // zero currency/fees/creation time
        }
        b.store_long(1, 1).store_long(by_ref, 1);
        if (by_ref) {
            b.store_ref(init);
        } else {
            b.append_cellslice(vm::load_cell_slice(init));
        }
        b.store_long(0, 1);  // empty inline body
        return ParseQuery::parse_message(b.finalize(), 0).move_as_ok();
    };
    auto add = [&](std::size_t block, S before, S after, bool aborted, bool external, bool by_ref) {
        schema::Transaction tx{};
        tx.account = block::StdAddress(parsed.blocks_[block].workchain, td::Bits256::zero());
        tx.lt = 9007199254740993ULL + parsed.blocks_[block].transactions.size();
        tx.now = parsed.blocks_[block].gen_utime;
        tx.mc_seqno = 100;
        tx.hash = vm::CellBuilder().store_long(tx.lt, 64).finalize()->get_hash().bits();
        tx.orig_status = before;
        tx.end_status = after;
        schema::TransactionDescr_ord descr{};
        descr.aborted = aborted;
        tx.description = descr;
        tx.in_msg = message(external, by_ref);
        parsed.blocks_[block].transactions.push_back(std::move(tx));
    };
    add(0, S::uninit, S::active, false, true, true);
    add(1, S::nonexist, S::active, false, false, false);
    // Every actual outgoing message counts, including a bounced message.
    parsed.blocks_[1].transactions.back().out_msgs.resize(2);
    parsed.blocks_[1].transactions.back().out_msgs.back().bounced = true;
    add(1, S::active, S::nonexist, false, false, true);
    add(1, S::uninit, S::active, false, true, false);  // same-address redeploy
    add(1, S::uninit, S::active, true, false, true);  // aborted
    add(1, S::frozen, S::active, false, false, true);  // unfreeze
    add(1, S::active, S::active, false, true, true);
    add(1, S::uninit, S::active, false, true, true);
    parsed.blocks_[1].transactions.back().in_msg->init_state = vm::CellBuilder().store_long(0, 5).finalize();

    // A different final code (e.g. SETCODE) must not affect deployment classification.
    schema::AccountState post{};
    post.hash = td::Bits256::zero();
    post.code_hash = other_code->get_hash().bits();
    parsed.account_states_.push_back(post);
    auto events = gramstat::collect_deployments(100, parsed).move_as_ok();
    CHECK(events.size() == 4);
    CHECK(events[0].mc_seqno == 100 && events[0].account.workchain == -1);
    CHECK(events[0].gen_utime == 1788307200 && events[0].out_msgs_count == 0);
    CHECK(events[1].gen_utime == 1788307199 && events[1].out_msgs_count == 2);
    CHECK(events[1].lt == 9007199254740993ULL && events[2].lt == 9007199254740995ULL);
    for (int i = 0; i < 3; ++i) {
        CHECK(events[i].init_code_hash == code->get_hash().bits());
    }
    CHECK(!events[3].init_code_hash);
    auto columns = gramstat::deployments_block(events);
    CHECK(columns.GetRowCount() == 4 && columns.GetColumnCount() == 7);
    CHECK(columns[1]->As<clickhouse::ColumnDateTime>()->RawAt(1) == 1788307199);
    CHECK(columns[2]->As<clickhouse::ColumnString>()->At(0) == "-1:" + std::string(64, '0'));
    CHECK(columns[3]->As<clickhouse::ColumnUInt64>()->At(1) == 9007199254740993ULL);
    CHECK(columns[4]->As<clickhouse::ColumnFixedString>()->At(1) == td::base64_encode(events[1].tx_hash.as_slice()));
    CHECK(columns[5]->As<clickhouse::ColumnNullable>()->IsNull(3));
    CHECK(!columns[5]->As<clickhouse::ColumnNullable>()->IsNull(1));
    CHECK(columns[6]->As<clickhouse::ColumnUInt32>()->At(1) == 2);

    CHECK(gramstat::collect_deployments(100, {}).is_error());
    CHECK(gramstat::collect_deployments(101, parsed).is_error());
    auto& last = parsed.blocks_[1].transactions.back();
    last.in_msg->init_state = vm::CellBuilder().finalize();
    CHECK(gramstat::collect_deployments(100, parsed).is_error());
    last.in_msg->init_state = vm::CellBuilder().append_cellslice(vm::load_cell_slice(init)).store_long(0, 1).finalize();
    CHECK(gramstat::collect_deployments(100, parsed).is_error());
    last.in_msg.reset();
    CHECK(gramstat::collect_deployments(100, parsed).is_error());
    last.in_msg = schema::Message{};
    CHECK(gramstat::collect_deployments(100, parsed).is_error());
    last.in_msg->init_state = init;
    last.description = schema::TransactionDescr_split_install{};
    CHECK(gramstat::collect_deployments(100, parsed).is_error());
    last.description = schema::TransactionDescr_ord{};
    last.mc_seqno = 99;
    CHECK(gramstat::collect_deployments(100, parsed).is_error());
}

void check_final_interface_state() {
    // Only block-end states are available. A later terminal state must prevent
    // running getters on the older active state, regardless of input ordering.
    for (const auto* status : {"nonexist", "frozen", "uninit", "active"}) {
        for (bool reversed : {false, true}) {
            auto parsed = std::make_shared<ParsedBlock>();
            schema::AccountState active{};
            active.account = block::StdAddress(0, td::Bits256::zero());
            active.last_trans_lt = 100;
            active.account_status = "active";
            active.code = active.data = vm::CellBuilder().finalize();
            active.code_hash = active.data_hash = active.code->get_hash().bits();
            schema::AccountState final{};
            final.account = active.account;
            final.account_status = status;
            final.last_trans_lt = 200;
            parsed->account_states_ = reversed ? std::vector{final, active} : std::vector{active, final};
            bool finished = false;
            td::actor::TestScheduler scheduler;
            scheduler.run([&]() -> td::actor::Task<> {
                td::actor::create_actor<BlockInterfaceProcessor>("final_state", parsed,
                    td::PromiseCreator::lambda([&](td::Result<ParsedBlockPtr> result) {
                        auto block = result.move_as_ok();
                        CHECK(block->account_interfaces_.at(active.account).empty());
                        CHECK(block->contract_methods_.empty());
                        finished = true;
                    })).release();
                while (!finished) {
                    co_await scheduler.wait_sync_work();
                }
                co_return td::Unit{};
            });
        }
    }
}

void check_jetton_detection_status() {
    vm::init_op_cp0();
    for (bool declared : {false, true}) {
        auto parsed = std::make_shared<ParsedBlock>();
        parsed->mc_block_.config_ = test_smc_config(1700000000);
        schema::AccountState state{};
        state.account = block::StdAddress(0, td::Bits256::zero());
        state.last_trans_lt = 100;
        state.account_status = "active";
        state.code = test_method_dispatcher(declared ? 97026 : 42,
            vm::CellBuilder().store_long(0xf20b, 16).finalize());
        state.data = vm::CellBuilder().finalize();
        state.code_hash = state.code->get_hash().bits();
        state.data_hash = state.data->get_hash().bits();
        parsed->account_states_.push_back(state);
        bool finished = false;
        td::actor::TestScheduler scheduler;
        scheduler.run([&]() -> td::actor::Task<> {
            td::actor::create_actor<BlockInterfaceProcessor>("jetton_status", parsed,
                td::PromiseCreator::lambda([&](td::Result<ParsedBlockPtr> result) {
                    auto block = result.move_as_ok();
                    CHECK(block->token_detection_.at(state.account).jetton_wallet ==
                        (declared ? std::nullopt : std::optional<bool>(false)));
                    CHECK(block->get_accounts_v2<schema::JettonWalletDataV2>().empty());
                    finished = true;
                })).release();
            while (!finished) {
                co_await scheduler.wait_sync_work();
            }
            co_return td::Unit{};
        });
    }
}

class HistoryCellReader final : public vm::CellDbReader {
public:
    std::unordered_map<std::string, td::Ref<vm::Cell>> cells;
    std::vector<std::string> reads;

    void add(const td::Ref<vm::Cell>& cell) {
        cells[cell->get_hash().as_slice().str()] = cell;
    }
    td::Result<td::Ref<vm::DataCell>> load_cell(td::Slice hash) override {
        reads.push_back(hash.str());
        auto found = cells.find(hash.str());
        if (found == cells.end()) {
            return td::Status::Error("Unexpected history cell read");
        }
        TRY_RESULT(cell, found->second->load_cell());
        return cell.data_cell;
    }
    td::Result<std::vector<td::Ref<vm::DataCell>>> load_bulk(td::Span<td::Slice>) override {
        return td::Status::Error("Unexpected history bulk read");
    }
    td::Ref<vm::Cell> create_unloaded_cell(const td::Ref<vm::Cell>& cell, int) override {
        return cell;
    }
};

void check_jetton_history() {
    ParsedBlock parsed;
    schema::Block mc{};
    mc.workchain = -1;
    mc.mc_block_workchain = -1;
    mc.seqno = 10;
    mc.mc_block_seqno = 10;
    mc.gen_utime = 1788307200;
    parsed.blocks_.push_back(mc);
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    CHECK(account_history::collect_states(11, parsed).is_error());
    CHECK(account_history::collect_states(10, {}).is_error());
    const auto address = block::StdAddress(0, td::Bits256::zero());
    for (auto lt : {200ULL, 100ULL}) {
        schema::Block shard{};
        shard.workchain = 0;
        shard.mc_block_workchain = -1;
        shard.mc_block_seqno = 10;
        shard.gen_utime = 1788307199;
        schema::Transaction tx{};
        tx.account = address;
        tx.mc_seqno = 10;
        tx.lt = lt;
        tx.orig_status = schema::AccountStatus::active;
        tx.end_status = schema::AccountStatus::active;
        tx.account_state_hash_after = td::Bits256::zero();
        shard.transactions.push_back(tx);
        parsed.blocks_.push_back(shard);
    }
    schema::AccountState state{};
    state.account = address;
    state.hash = td::Bits256::zero();
    state.last_trans_lt = 200;
    state.account_status = "active";
    auto throw11 = vm::CellBuilder().store_long(0xf20b, 16).finalize();
    state.code = test_method_dispatcher(97026, throw11);
    state.data = vm::CellBuilder().finalize();
    state.code_hash = state.code->get_hash().bits();
    state.data_hash = state.data->get_hash().bits();
    parsed.account_states_.push_back(state);
    auto reader = std::make_shared<HistoryCellReader>();
    parsed.cell_db_reader_ = reader;
    auto before = test_active_account(address, state.code);
    reader->add(before);
    // The input blocks are deliberately out of order. Only the first tx's
    // before-hash is present in the reader; the last tx's before-state is absent.
    parsed.blocks_[2].transactions[0].account_state_hash_before = before->get_hash().bits();
    schema::JettonWalletDataV2 wallet{};
    wallet.address = address;
    wallet.last_transaction_lt = 200;
    wallet.code_hash = *state.code_hash;
    wallet.data_hash = *state.data_hash;
    const std::string amount = "100000000000000000000000000000000000000000000000001";
    wallet.balance = td::dec_string_to_int256(amount);
    parsed.account_interfaces_[address] = {wallet};
    parsed.token_detection_[address].jetton_wallet = true;
    auto rows = account_history::collect_states(10, parsed).move_as_ok().jetton_wallets;
    CHECK(rows.size() == 1 && rows[0].existing == true && rows[0].last_tx_lt == 200);
    CHECK(rows[0].gen_utime == 1788307200 && rows[0].balance->to_dec_string() == amount);
    auto columns = jetton_history::states_block(rows);
    CHECK(columns.GetColumnCount() == 6 && columns.GetRowCount() == 1);
    CHECK(columns[5]->As<clickhouse::ColumnNullable>()->Nested()->As<clickhouse::ColumnString>()->At(0) == amount);

    // A failed final-state load must not silently carry forward the older state.
    parsed.account_states_.back().last_trans_lt = 100;
    CHECK(account_history::collect_states(10, parsed).is_error());
    parsed.account_states_.back().last_trans_lt = 200;
    parsed.token_detection_[address].jetton_wallet.reset();
    auto unknown = account_history::collect_states(10, parsed).move_as_ok().jetton_wallets[0];
    CHECK(!unknown.existing && unknown.balance.is_null());
    parsed.token_detection_[address].jetton_wallet = false;
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets[0].existing == false);
    parsed.token_detection_[address].jetton_wallet = true;
    CHECK(reader->reads.empty());
    for (auto status : {schema::AccountStatus::nonexist, schema::AccountStatus::frozen, schema::AccountStatus::uninit}) {
        reader->reads.clear();
        parsed.blocks_[1].transactions[0].end_status = status;
        auto terminal = account_history::collect_states(10, parsed).move_as_ok().jetton_wallets[0];
        CHECK(terminal.existing == false && terminal.balance.is_null());
        CHECK(reader->reads == std::vector<std::string>{before->get_hash().as_slice().str()});
        auto serialized = jetton_history::states_block({terminal, unknown});
        CHECK(!serialized[4]->As<clickhouse::ColumnNullable>()->IsNull(0));
        CHECK(serialized[4]->As<clickhouse::ColumnNullable>()->IsNull(1));
        CHECK(serialized[5]->As<clickhouse::ColumnNullable>()->IsNull(0));
        CHECK(serialized[5]->As<clickhouse::ColumnNullable>()->IsNull(1));
    }
    // Redeployment becomes active again; stale interface data still cannot match.
    parsed.blocks_[1].transactions[0].end_status = schema::AccountStatus::active;
    reader->reads.clear();
    std::get<schema::JettonWalletDataV2>(parsed.account_interfaces_[address][0]).last_transaction_lt = 100;
    CHECK(!account_history::collect_states(10, parsed).move_as_ok().jetton_wallets[0].existing.has_value());
    std::get<schema::JettonWalletDataV2>(parsed.account_interfaces_[address][0]).last_transaction_lt = 200;
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets[0].existing == true);
    CHECK(reader->reads.empty());

    // An ordinary active account emits neither false nor unknown observations.
    auto ordinary_code = test_method_dispatcher(42, throw11);
    parsed.account_states_.back().code = ordinary_code;
    parsed.account_states_.back().code_hash = ordinary_code->get_hash().bits();
    parsed.token_detection_[address].jetton_wallet = false;
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    parsed.token_detection_[address].jetton_wallet.reset();
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    // Even a potential SETCODE is not a reason to read before-state while active.
    schema::TransactionDescr_ord setcode{};
    setcode.action.emplace();
    setcode.action->spec_actions = 1;
    parsed.blocks_[1].transactions[0].description = setcode;
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    CHECK(reader->reads.empty());

    parsed.account_states_.back().code = throw11;  // unsupported method dispatcher
    parsed.account_states_.back().code_hash = throw11->get_hash().bits();
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    CHECK(reader->reads.empty());

    auto ordinary_before = test_active_account(address, ordinary_code);
    reader->add(ordinary_before);
    parsed.blocks_[2].transactions[0].account_state_hash_before = ordinary_before->get_hash().bits();
    for (auto status : {schema::AccountStatus::nonexist, schema::AccountStatus::frozen, schema::AccountStatus::uninit}) {
        reader->reads.clear();
        parsed.blocks_[1].transactions[0].end_status = status;
        CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
        CHECK(reader->reads == std::vector<std::string>{ordinary_before->get_hash().as_slice().str()});
    }
    // Already nonactive accounts, and deploy+destroy inside this MC, need no read.
    reader->reads.clear();
    for (auto status : {schema::AccountStatus::nonexist, schema::AccountStatus::frozen, schema::AccountStatus::uninit}) {
        parsed.blocks_[2].transactions[0].orig_status = status;
        CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    }
    CHECK(reader->reads.empty());

    // A missing/corrupt boundary state is a load error, not a generic tombstone.
    parsed.blocks_[2].transactions[0].orig_status = schema::AccountStatus::active;
    reader->cells.clear();
    CHECK(account_history::collect_states(10, parsed).is_error());
    auto malformed = vm::CellBuilder().finalize();
    reader->add(malformed);
    parsed.blocks_[2].transactions[0].account_state_hash_before = malformed->get_hash().bits();
    CHECK(account_history::collect_states(10, parsed).is_error());

    // Library-backed wallet code is checked from the same MC configuration.
    auto library_code = test_library_reference(state.code);
    auto library_before = test_active_account(address, library_code);
    reader->add(library_before);
    parsed.blocks_[2].transactions[0].account_state_hash_before = library_before->get_hash().bits();
    parsed.mc_block_.config_ = test_smc_config(1788307200, {state.code});
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets[0].existing == false);
    parsed.blocks_[1].transactions[0].end_status = schema::AccountStatus::active;
    parsed.account_states_.back().code = library_code;
    parsed.account_states_.back().code_hash = library_code->get_hash().bits();
    reader->reads.clear();
    auto library_unknown = account_history::collect_states(10, parsed).move_as_ok().jetton_wallets;
    CHECK(library_unknown.size() == 1 && !library_unknown[0].existing.has_value());
    CHECK(reader->reads.empty());
    // Unresolved code cannot prove eligibility. It is not a negative observation.
    parsed.mc_block_.config_ = test_smc_config(1788307200);
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    CHECK(reader->reads.empty());
    parsed.blocks_[1].transactions[0].end_status = schema::AccountStatus::frozen;
    CHECK(account_history::collect_states(10, parsed).move_as_ok().jetton_wallets.empty());
    CHECK(reader->reads == std::vector<std::string>{library_before->get_hash().as_slice().str()});
}

void check_nft_history() {
    auto parsed = std::make_shared<ParsedBlock>();
    parsed->mc_block_.config_ = test_smc_config(1788307200);
    schema::Block mc{};
    mc.workchain = -1;
    mc.mc_block_workchain = -1;
    mc.seqno = 10;
    mc.mc_block_seqno = 10;
    mc.gen_utime = 1788307200;
    parsed->blocks_.push_back(mc);
    schema::Block shard{};
    shard.workchain = 0;
    shard.mc_block_workchain = -1;
    shard.mc_block_seqno = 10;
    const auto address = block::StdAddress(0, td::Bits256::zero());
    const auto owner = block::StdAddress(-1, td::Bits256::zero());
    schema::Transaction tx{};
    tx.account = address;
    tx.mc_seqno = 10;
    tx.orig_status = tx.end_status = schema::AccountStatus::active;
    tx.account_state_hash_after = td::Bits256::zero();
    tx.lt = 200;
    shard.transactions.push_back(tx);
    tx.lt = 100;
    auto throw11 = vm::CellBuilder().store_long(0xf20b, 16).finalize();
    auto boundary_code = test_methods_dispatcher({{97026, throw11}, {102351, throw11}});
    auto before = test_active_account(address, boundary_code);
    tx.account_state_hash_before = before->get_hash().bits();
    shard.transactions.push_back(tx);
    parsed->blocks_.push_back(shard);
    auto reader = std::make_shared<HistoryCellReader>();
    reader->add(before);
    parsed->cell_db_reader_ = reader;
    schema::AccountState state{};
    state.account = address;
    state.hash = td::Bits256::zero();
    state.last_trans_lt = 200;
    state.account_status = "active";
    state.code = test_nft_code(true, test_address_cell(owner), {});  // invalid metadata
    state.data = vm::CellBuilder().finalize();
    state.code_hash = state.code->get_hash().bits();
    state.data_hash = state.data->get_hash().bits();
    parsed->account_states_.push_back(state);
    auto earlier = state;
    earlier.last_trans_lt = 100;
    earlier.code = test_nft_code(true, test_address_cell(address), {});
    earlier.code_hash = earlier.code->get_hash().bits();
    parsed->account_states_.push_back(earlier);
    bool finished = false;
    td::actor::TestScheduler scheduler;
    scheduler.run([&]() -> td::actor::Task<> {
        td::actor::create_actor<BlockInterfaceProcessor>("nft_history", parsed,
            td::PromiseCreator::lambda([&](td::Result<ParsedBlockPtr> result) {
                result.ensure();
                finished = true;
            })).release();
        while (!finished) {
            co_await scheduler.wait_sync_work();
        }
        co_return td::Unit{};
    });
    auto collect = [&]() { return account_history::collect_states(10, *parsed).move_as_ok(); };
    auto rows = collect();
    auto latest = parsed->get_accounts_v2<schema::NFTItemDataV2>();
    CHECK(latest.size() == 1 && !latest[0].content && latest[0].owner_address == owner);
    CHECK(rows.jetton_wallets.empty() && rows.nft_items.size() == 1);
    CHECK(rows.nft_items[0].existing == true && rows.nft_items[0].owner == latest[0].owner_address);
    CHECK(rows.nft_items[0].last_tx_lt == 200 && rows.nft_items[0].gen_utime == 1788307200);
    auto columns = nft_history::states_block(rows.nft_items);
    CHECK(columns.GetColumnCount() == 6 && columns.GetRowCount() == 1);
    CHECK(columns[5]->As<clickhouse::ColumnNullable>()->Nested()->As<clickhouse::ColumnString>()->At(0) ==
          convert::to_raw_address(owner));
    auto& item = std::get<schema::NFTItemDataV2>(parsed->account_interfaces_[address][0]);
    item.owner_address.reset();
    CHECK(collect().nft_items[0].existing == true && !collect().nft_items[0].owner);
    item.init = false;
    CHECK(collect().nft_items[0].existing == false);
    item.init = true;
    item.owner_address = owner;
    item.last_transaction_lt = 100;
    CHECK(!collect().nft_items[0].existing && !collect().nft_items[0].owner);
    item.last_transaction_lt = 200;
    parsed->token_detection_[address].nft_item.reset();
    CHECK(!collect().nft_items[0].existing && !collect().nft_items[0].owner);
    parsed->token_detection_[address].nft_item = false;
    CHECK(collect().nft_items[0].existing == false && !collect().nft_items[0].owner);
    CHECK(reader->reads.empty());

    // Both tombstones share exactly one read of the FIRST transaction's before
    // state. The last transaction's before-state is deliberately unavailable.
    for (auto status : {schema::AccountStatus::nonexist, schema::AccountStatus::frozen, schema::AccountStatus::uninit}) {
        reader->reads.clear();
        parsed->blocks_[1].transactions[0].end_status = status;
        auto terminal = collect();
        CHECK(terminal.jetton_wallets.size() == 1 && terminal.nft_items.size() == 1);
        CHECK(terminal.jetton_wallets[0].existing == false && terminal.nft_items[0].existing == false);
        CHECK(!terminal.nft_items[0].owner && reader->reads.size() == 1);
    }
    reader->reads.clear();
    parsed->blocks_[1].transactions[1].orig_status = schema::AccountStatus::uninit;
    CHECK(collect().nft_items.empty() && reader->reads.empty());
    parsed->blocks_[1].transactions[1].orig_status = schema::AccountStatus::active;
    auto library_before = test_active_account(address, test_library_reference(boundary_code));
    reader->add(library_before);
    parsed->blocks_[1].transactions[1].account_state_hash_before = library_before->get_hash().bits();
    parsed->mc_block_.config_ = test_smc_config(1788307200, {boundary_code});
    CHECK(collect().nft_items[0].existing == false && reader->reads.size() == 1);
    parsed->mc_block_.config_ = test_smc_config(1788307200);
    CHECK(collect().nft_items.empty());
}

}  // namespace

int main(int argc, char** argv) {
    td::Time::allow_freezes();
    if (argc == 2 && std::string(argv[1]) == "--jetton-schema") {
        std::cout << jetton_history::CREATE_STATES << ";\n";
        return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--nft-schema") {
        std::cout << nft_history::CREATE_STATES << ";\n";
        return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--schema") {
        std::cout << gramstat::CREATE_DATABASE << ";\n" << gramstat::CREATE_TOTAL_SUPPLY << ";\n"
                  << gramstat::CREATE_ACCOUNT_DELTAS << ";\n" << gramstat::CREATE_DEPLOYMENTS << ";\n";
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
    check_block_fees();
    check_block_value_flow();
    check_account_deltas();
    check_deployments();
    check_final_interface_state();
    check_jetton_detection_status();
    check_jetton_history();
    check_nft_history();
    std::cout << "Gramstat supply, account deltas, deployments and exact ClickHouse columns: OK\n";
}
