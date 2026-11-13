#include "ElectorRoundResults.h"
#include "DataParser.h"
#include "parse_contract_methods.h"
#include "smc-fixture.h"
#include "common/checksum.h"
#include "emulator/transaction-emulator.h"
#include "td/utils/base64.h"
#include "vm/db/DynamicBagOfCellsDb.h"
#include "vm/cp0.h"
#include "vm/vm.h"
#include "vm/boc.h"
#include <iostream>
#include <map>

namespace {

constexpr uint32_t NOW = 1700000000;
const block::StdAddress ADDRESS(-1, td::Bits256::zero());

class Cells final : public vm::CellDbReader {
 public:
  std::map<std::string, td::Ref<vm::DataCell>> roots;
  void add(td::Ref<vm::Cell> cell) { roots[cell->get_hash().to_hex()] = cell->load_cell().move_as_ok().data_cell; }
  td::Result<td::Ref<vm::DataCell>> load_cell(td::Slice hash) override {
    auto it = roots.find(vm::CellHash::from_slice(hash).to_hex());
    if (it == roots.end()) return td::Status::Error("cell not found");
    return it->second;
  }
  td::Result<std::vector<td::Ref<vm::DataCell>>> load_bulk(td::Span<td::Slice> hashes) override {
    std::vector<td::Ref<vm::DataCell>> result;
    for (auto hash : hashes) { TRY_RESULT(cell, load_cell(hash)); result.push_back(cell); }
    return result;
  }
  td::Ref<vm::Cell> create_unloaded_cell(const td::Ref<vm::Cell>& cell, int) override { return cell; }
};

class BlockData final : public ton::validator::BlockData {
  td::Ref<vm::Cell> root_;
 public:
  explicit BlockData(td::Ref<vm::Cell> root) : root_(std::move(root)) {}
  td::BufferSlice data() const override { return vm::std_boc_serialize(root_).move_as_ok(); }
  ton::FileHash file_hash() const override { return td::Bits256::zero(); }
  ton::BlockIdExt block_id() const override {
    return {ton::masterchainId, ton::shardIdAll, 1, root_->get_hash().bits(), td::Bits256::zero()};
  }
  td::Ref<vm::Cell> root_cell() const override { return root_; }
};

// A minimal deterministic contract: each transaction advances c4 to its first
// ref. Getters execute the supplied result builders in refs 1 and 2. This lets
// real TVM transactions model rewards, penalties, cancellations and unfreezing.
td::Ref<vm::Cell> code(int behavior = 0) {
  auto transition = vm::CellBuilder().store_long(0xf800, 16).store_long(0xed44, 16)
      .store_long(0xd0, 8).store_long(0xd74c, 16).store_long(0xed54, 16).finalize();
  if (behavior == 1) transition = vm::CellBuilder().store_long(0xf800, 16).finalize();
  if (behavior == 2) transition = vm::CellBuilder().store_long(0xf20b, 16).finalize();
  auto getter = [](unsigned ref) {
    return vm::CellBuilder().store_long(0xed44, 16).store_long(0xd0, 8)
        .store_long(0xd74c + ref, 16).store_long(0xd0, 8).store_long(0xed1e, 16).store_long(0xd9, 8).finalize();
  };
  std::string ids = "past_election_ids", amounts = "past_elections";
  std::vector<std::pair<unsigned, td::Ref<vm::Cell>>> methods{{0, transition}, {0x7fffe, transition}};
  if (behavior != 3 && behavior != 4) methods.emplace_back(unsigned((td::crc16(ids) & 0xffff) | 0x10000), getter(1));
  if (behavior != 3) methods.emplace_back(unsigned((td::crc16(amounts) & 0xffff) | 0x10000), getter(2));
  auto result = test_methods_dispatcher(methods);
  if (behavior == 3 || behavior == 4) {
    // The deployed genesis Elector uses DICTIGETJMP, not DICTIGETJMPZ.
    // A missing zero-argument getter reaches THROWARG with an empty stack (exit 2).
    return vm::CellBuilder().store_long(0xff00, 16).store_long(0xf4a413, 24)
        .store_ref(vm::load_cell_slice(result).prefetch_ref())
        .store_long(0xf4a0, 16).store_long(0xf2c80b, 24).finalize();
  }
  return result;
}

void push_small(vm::CellBuilder& b, int n) { b.store_long(0x80, 8).store_long(n, 8); }
void push_coins(vm::CellBuilder& b, const std::string& n) {
  auto cell = vm::CellBuilder().store_int256(*td::dec_string_to_int256(n), 120, false).finalize();
  b.store_long(0x89, 8).store_ref(cell).store_long(0xd3, 8).store_long(119, 8).store_long(0x30, 8);
}

struct Amount { int id; std::string stake, bonuses; };

td::Ref<vm::Cell> data(const std::vector<Amount>& amounts, td::Ref<vm::Cell> next = {}, bool flat = true) {
  vm::CellBuilder ids, values;
  for (const auto& a : amounts) {
    push_small(ids, a.id);
    push_small(values, a.id);
    push_small(values, 50);  // unfreeze_at (already due)
    push_small(values, 10);  // stake_held
    push_small(values, 123); // vset hash
    values.store_long(0x6d, 8); // frozen dictionary intentionally absent: totals require no traversal
    push_coins(values, a.stake);
    push_coins(values, a.bonuses);
    values.store_long(0x6d, 8); // complaints
    if (flat) values.store_long(0x6f08, 16);
    else values.store_long(0x6f07, 16).store_long(0x6f02, 16);
  }
  ids.store_long(0x6d, 8);
  values.store_long(0x6d, 8);
  for (size_t i = 0; i < amounts.size(); ++i) {
    ids.store_long(0x6f02, 16);
    values.store_long(0x6f02, 16);
  }
  return vm::CellBuilder().store_ref(next.not_null() ? next : vm::CellBuilder().finalize())
      .store_ref(ids.finalize()).store_ref(values.finalize()).finalize();
}

std::shared_ptr<block::ConfigInfo> config(td::Ref<vm::Cell>* root = nullptr, bool zero_gas = false) {
  vm::Dictionary storage(32);
  auto prices = vm::CellBuilder().store_long(0xcc, 8).store_zeroes(32 + 4 * 64).finalize();
  CHECK(storage.set(td::BitArray<32>::zero(), vm::load_cell_slice_ref(prices)));
  auto gas = vm::CellBuilder().store_long(0xdd, 8).store_long(65536, 64)
      .store_long(zero_gas ? 0 : 10000000, 64).store_long(10000, 64).store_long(100000000, 64)
      .store_long(1000000000, 64).store_long(10000000000LL, 64).finalize();
  auto msg = vm::CellBuilder().store_long(0xea, 8).store_zeroes(3 * 64 + 32 + 16 + 16).finalize();
  return test_smc_config(NOW, {}, {{1, vm::CellBuilder().store_zeroes(256).finalize()},
      {18, storage.get_root_cell()}, {20, gas}, {24, msg}, {25, msg}}, root);
}

td::Ref<vm::Cell> initial_account(td::Ref<vm::Cell> body, int behavior = 0) {
  vm::CellBuilder b;
  b.store_long(1, 1).append_cellslice(vm::load_cell_slice(test_address_cell(ADDRESS)));
  b.store_zeroes(3 + 3 + 3 + 32 + 1 + 64);
  CHECK(block::tlb::t_Grams.store_integer_ref(b, td::make_refint(1000000000000LL)));
  b.store_long(0, 1).store_long(1, 1); // extra currencies; active
  b.store_long(0, 1).store_long(1, 1).store_long(3, 2); // split_depth; special tick+tock
  CHECK(b.store_maybe_ref(code(behavior)));
  CHECK(b.store_maybe_ref(body));
  return b.store_long(0, 1).finalize();
}

td::Ref<vm::Cell> block_root(td::Ref<vm::Cell> old_state) {
  auto empty = vm::CellBuilder().finalize();
  auto prev = vm::CellBuilder().store_zeroes(64 + 32 + 256 + 256).finalize();
  auto info = vm::CellBuilder().store_long(0x9bc7a987, 32).store_zeroes(32 + 8 + 8)
      .store_long(1, 32).store_zeroes(32).store_long(0, 8).store_long(-1, 32).store_zeroes(64)
      .store_long(NOW, 32).store_long(1000000, 64).store_long(2000000, 64)
      .store_zeroes(4 * 32).store_ref(prev).finalize();
  // Merkle update with identical roots suffices to test loading the pre-config.
  auto update = vm::CellBuilder().store_long(4, 8)
      .store_bits(old_state->get_hash().bits(), 256).store_bits(old_state->get_hash().bits(), 256)
      .store_long(old_state->get_depth(), 16).store_long(old_state->get_depth(), 16)
      .store_ref(old_state).store_ref(old_state).finalize(true);
  return vm::CellBuilder().store_long(0x11ef55aa, 32).store_long(-3, 32)
      .store_ref(info).store_ref(empty).store_ref(update).store_ref(empty).finalize();
}

struct Fixture {
  ParsedBlock parsed;
  std::shared_ptr<Cells> cells = std::make_shared<Cells>();
  std::vector<td::Ref<vm::Cell>> states;

  Fixture(td::Ref<vm::Cell> start, const std::vector<int>& types, int behavior = 0) {
    td::Ref<vm::Cell> config_root;
    auto cfg = config(&config_root);
    parsed.mc_block_.config_ = cfg;
    cells->add(config_root);
    parsed.cell_db_reader_ = cells;
    auto raw_block = td::make_ref<BlockData>(block_root(config_root));
    parsed.mc_block_.shard_blocks_diff_.push_back({raw_block, config_root, {}});
    schema::Block block{};
    block.workchain = -1; block.seqno = 1; block.gen_utime = NOW;
    auto seed = td::Bits256::ones();
    block.rand_seed = td::base64_encode(seed.as_slice());
    auto root = initial_account(start, behavior);
    states.push_back(root);
    auto shard = vm::CellBuilder().store_ref(root).store_zeroes(256 + 64).finalize();
    block::Account account(-1, ADDRESS.addr.bits());
    CHECK(account.unpack(vm::load_cell_slice_ref(shard), NOW, true));
    emulator::TransactionEmulator engine(cfg);
    engine.set_rand_seed(seed);
    uint64_t lt = 1000000;
    for (auto type : types) {
      td::Ref<vm::Cell> msg;
      if (type == block::transaction::Transaction::tr_ord) {
        vm::CellBuilder message;
        message.store_long(6, 4)
            .append_cellslice(vm::load_cell_slice(test_address_cell(ADDRESS)))
            .append_cellslice(vm::load_cell_slice(test_address_cell(ADDRESS)));
        CHECK(block::tlb::t_Grams.store_integer_ref(message, td::make_refint(1000000)));
        msg = message.store_zeroes(1 + 4 + 4 + 64 + 32 + 1 + 1).finalize();
      }
      auto result = engine.emulate_transaction(std::move(account), msg, NOW, lt, type).move_as_ok();
      auto success = dynamic_cast<emulator::TransactionEmulator::EmulationSuccess*>(result.get());
      CHECK(success);
      auto tx = ParseQuery::parse_transaction(success->transaction, -1, cfg->get_global_version()).move_as_ok();
      tx.mc_seqno = 1;
      block.transactions.push_back(tx);
      account = std::move(success->account);
      states.push_back(account.total_state);
      lt += 100;
    }
    parsed.blocks_.push_back(std::move(block));
    cells->add(states.front());
    cells->add(states.back());
  }
};

void check_collector() {
  using T = block::transaction::Transaction;
  // A first tick changes the state, an ordinary transaction updates rewards and
  // applies a penalty, then a tock releases the round. Only block boundaries are in CellDB.
  auto end = data({});
  auto pre_unfreeze = data({{1, "95", "20"}}, end);
  auto middle = data({{1, "100", "10"}}, pre_unfreeze);
  auto start = data({{1, "100", "0"}}, middle);
  Fixture f(start, {T::tr_tick, T::tr_ord, T::tr_tock});
  elector::CollectionStats stats;
  auto rows = elector::collect_round_results(f.parsed, &stats).move_as_ok();
  CHECK(rows.size() == 1 && rows[0].election_id == 1);
  CHECK(rows[0].total_stake->to_dec_string() == "95" && rows[0].bonuses->to_dec_string() == "20");
  CHECK(rows[0].unfreeze_tx_lt == f.parsed.blocks_[0].transactions.back().lt);
  CHECK(stats.replayed_transactions == 2 && stats.amount_getters == 1);

  // Post-block gas prices would prevent replay. The collector must use the
  // preceding MC config carried by the block's Merkle update instead.
  f.parsed.mc_block_.config_ = config(nullptr, true);
  stats = {};
  CHECK(elector::collect_round_results(f.parsed, &stats).move_as_ok().size() == 1);
  CHECK(stats.replayed_transactions == 2);

  Fixture quiet(data({{1, "100", "0"}}), {T::tr_tick, T::tr_tock}, 1);
  stats = {};
  CHECK(elector::collect_round_results(quiet.parsed, &stats).move_as_ok().empty());
  CHECK(stats.id_getters == 0 && stats.amount_getters == 0 && stats.replayed_transactions == 0);

  Fixture aborted(data({{1, "100", "0"}}, end), {T::tr_tick}, 2);
  stats = {};
  CHECK(elector::collect_round_results(aborted.parsed, &stats).move_as_ok().empty());
  CHECK(stats.id_getters == 0 && stats.replayed_transactions == 0);

  Fixture zero(data({{1, "100", "0"}}, end), {T::tr_tick});
  CHECK(elector::collect_round_results(zero.parsed).move_as_ok().at(0).bonuses->sgn() == 0);

  // Direct intermediate cells avoid replay and yield byte-identical results.
  for (const auto& state : f.states) f.cells->add(state);
  stats = {};
  auto direct = elector::collect_round_results(f.parsed, &stats).move_as_ok();
  CHECK(direct[0].unfreeze_tx_hash == rows[0].unfreeze_tx_hash && stats.replayed_transactions == 0);

  // A changing reward balance without removal never replays missing intermediates.
  Fixture reward(data({{1, "100", "1"}}, data({{1, "100", "2"}}, data({{1, "100", "3"}}))),
                 {T::tr_tick, T::tr_tock});
  stats = {};
  CHECK(elector::collect_round_results(reward.parsed, &stats).move_as_ok().empty());
  CHECK(stats.replayed_transactions == 0 && stats.amount_getters == 0 && stats.id_getters == 2);

  // Cancellation during an ordinary transaction is not scheduled unfreezing.
  Fixture cancelled(data({{1, "100", "0"}}, data({{1, "100", "0"}}, end)), {T::tr_tick, T::tr_ord});
  CHECK(elector::collect_round_results(cancelled.parsed).move_as_ok().empty());

  Fixture two(data({{1, "10", "1"}, {2, "20", "2"}}, data({{2, "20", "2"}}, end)),
              {T::tr_tick, T::tr_tock});
  CHECK(elector::collect_round_results(two.parsed).move_as_ok().size() == 2);

  // Invalid original input cannot silently manufacture a result.
  Fixture broken(start, {T::tr_tick, T::tr_ord, T::tr_tock});
  broken.parsed.blocks_[0].transactions[0].raw = vm::CellBuilder().finalize();
  CHECK(elector::collect_round_results(broken.parsed).is_error());
  broken.cells->roots.clear();
  CHECK(elector::collect_round_results(broken.parsed).is_error());

  Fixture mismatch(start, {T::tr_tick, T::tr_ord, T::tr_tock});
  mismatch.parsed.blocks_[0].transactions[0].raw = mismatch.parsed.blocks_[0].transactions.back().raw;
  CHECK(elector::collect_round_results(mismatch.parsed).is_error());

  auto bad_getter = vm::CellBuilder().store_long(0xf20b, 16).finalize();
  auto no_ids = vm::CellBuilder().store_ref(end).store_ref(bad_getter).store_ref(bad_getter).finalize();
  Fixture failed_getter(no_ids, {T::tr_tick});
  CHECK(elector::collect_round_results(failed_getter.parsed).is_error());

  Fixture unsupported(data({{1, "100", "20"}}, end), {T::tr_tick}, 3);
  auto legacy_state = ParseQuery::parse_account(unsupported.states.front(), NOW, td::Bits256::zero(), 0).move_as_ok();
  auto metadata = get_contract_method_info(legacy_state.code, unsupported.parsed.mc_block_.config_.get()).move_as_ok();
  CHECK(metadata->exhaustive);
  CHECK(elector::run_get_method(legacy_state, unsupported.parsed.mc_block_.config_, "past_election_ids", {}, NOW)
      .move_as_error().to_string().find("exit code 2") != std::string::npos);
  stats = {};
  CHECK(elector::collect_round_results(unsupported.parsed, &stats).move_as_ok().empty());
  CHECK(stats.unsupported_blocks == 1 && stats.id_getters == 0 && stats.amount_getters == 0);

  Fixture legacy_ids(data({{1, "100", "20"}}, end), {T::tr_tick}, 4);
  stats = {};
  auto legacy_rows = elector::collect_round_results(legacy_ids.parsed, &stats).move_as_ok();
  CHECK(legacy_rows.size() == 1 && legacy_rows[0].total_stake->to_dec_string() == "100" &&
        legacy_rows[0].bonuses->to_dec_string() == "20");
  CHECK(stats.legacy_id_getters == 2 && stats.unsupported_blocks == 0 && stats.id_getters == 0);
}

void check_amounts() {
  auto cfg = config();
  for (bool flat : {true, false}) {
    auto body = data({{1, "900719925474099312345", "12345678901234567890"}}, {}, flat);
    auto state = ParseQuery::parse_account(initial_account(body), NOW, td::Bits256::zero(), 0).move_as_ok();
    auto stack = elector::run_get_method(state, cfg, "past_elections", {}, NOW).move_as_ok();
    auto amounts = elector::parse_round_amounts(stack).move_as_ok();
    CHECK(amounts.size() == 1 && amounts[0].total_stake->to_dec_string() == "900719925474099312345");
    CHECK(amounts[0].bonuses->to_dec_string() == "12345678901234567890");
  }
  CHECK(elector::parse_round_amounts({}).is_error());
  CHECK(elector::parse_round_amounts({vm::StackEntry{}}).move_as_ok().empty());
  auto integer = [](int n) { return vm::StackEntry(td::make_refint(n)); };
  auto entry = vm::StackEntry(vm::make_tuple_ref(integer(1), integer(50), integer(10), integer(123),
      vm::StackEntry{}, integer(100), integer(-1), vm::StackEntry{}));
  CHECK(elector::parse_round_amounts({vm::StackEntry(vm::make_tuple_ref(entry, vm::StackEntry{}))}).is_error());
}

}  // namespace

int main() {
  vm::init_vm().ensure();
  check_amounts();
  check_collector();
  std::cout << "Elector round amounts, direct states and verified intermediate replay: OK\n";
}
