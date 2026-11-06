#include "MemoCache.h"
#include "ValidatorElection.h"
#include "StakingEventProcessor.h"
#include "parse_contract_methods.h"
#include "smc-fixture.h"
#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cp0.h"

#include <atomic>
#include <fstream>
#include <future>
#include <iostream>
#include <thread>

namespace {

void check_lru_and_budget() {
  memo::Cache<int, std::string> cache(4096, 2);
  int calls = 0;
  auto get = [&](int key) {
    return cache.get(key, [&]() -> td::Result<std::string> { ++calls; return std::string(300, char(key)); },
                     [](const auto& value) { return value.capacity() + 1; }).move_as_ok();
  };
  auto held = get(1);
  get(2); get(1); get(3); // 2 is the least recently used entry
  CHECK(calls == 3 && cache.stats().evictions == 1);
  get(2);
  CHECK(calls == 4 && cache.stats().entries == 2 && cache.stats().bytes <= 4096);
  CHECK(*held == std::string(300, char(1))); // eviction cannot invalidate a caller
  auto report = cache.report("test");
  CHECK(report.find("test.hit COUNT : 1\n") != std::string::npos);
  CHECK(cache.report("test").find("test.hit COUNT : 0\n") != std::string::npos);

  memo::Cache<int, std::vector<int>> budget(4096);
  auto weight = [](const auto& value) { return value.capacity() * sizeof(int); };
  for (int i = 0; i < 100; ++i) {
    CHECK(budget.get(i, []() -> td::Result<std::vector<int>> { return std::vector<int>(128); }, weight).is_ok());
    CHECK(budget.stats().bytes <= 4096);
  }
  CHECK(budget.stats().evictions > 0);
  auto entries = budget.stats().entries;
  for (int i = 0; i < 2; ++i) {
    CHECK(budget.get(1000, []() -> td::Result<std::vector<int>> { return std::vector<int>(10000); }, weight).is_ok());
  }
  CHECK(budget.stats().oversized == 2 && budget.stats().entries == entries);
  // Empty successful results consume an entry's overhead and are reusable.
  CHECK(budget.get(1001, []() -> td::Result<std::vector<int>> { return std::vector<int>{}; }, weight).is_ok());
  CHECK(budget.get(1001, []() -> td::Result<std::vector<int>> { CHECK(false); return td::Status::Error("unused"); }, weight).is_ok());
  for (int i = 1002; i < 1102; ++i) {
    CHECK(budget.get(i, []() -> td::Result<std::vector<int>> { return std::vector<int>{}; }, weight).is_ok());
    CHECK(budget.stats().bytes <= 4096);
  }
  CHECK(budget.stats().entries < 100);
}

void check_coalescing(bool fail) {
  memo::Cache<int, int> cache(4096);
  std::promise<void> started, release;
  auto gate = release.get_future().share();
  std::atomic<int> computations{0};
  auto loader = [&]() -> td::Result<int> {
    ++computations;
    started.set_value();
    gate.wait();
    if (fail) throw std::runtime_error("transient read failure");
    return 42;
  };
  auto weight = [](int) { return size_t(0); };
  auto first = std::async(std::launch::async, [&] { return cache.get(1, loader, weight); });
  started.get_future().wait();
  std::vector<std::future<td::Result<std::shared_ptr<const int>>>> followers;
  for (int i = 0; i < 7; ++i) {
    followers.push_back(std::async(std::launch::async, [&] { return cache.get(1, loader, weight); }));
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (cache.stats().coalesced != followers.size()) {
    CHECK(std::chrono::steady_clock::now() < deadline);
    std::this_thread::yield();
  }
  // A different key completes while the slow computation and its followers wait.
  CHECK(*cache.get(2, []() -> td::Result<int> { return 7; }, weight).move_as_ok() == 7);
  release.set_value();
  CHECK(first.get().is_error() == fail);
  for (auto& follower : followers) CHECK(follower.get().is_error() == fail);
  CHECK(computations == 1 && cache.stats().inflight == 0);
  if (fail) {
    CHECK(cache.stats().entries == 1 && cache.stats().errors == 1);
    CHECK(*cache.get(1, []() -> td::Result<int> { return 43; }, weight).move_as_ok() == 43);
  }
}

// Same content identity before and after a transient CellDB-style load failure.
class ReadFaultCell final : public vm::Cell {
  td::Ref<vm::Cell> inner_;
  std::shared_ptr<std::atomic<bool>> fail_;
 public:
  ReadFaultCell(td::Ref<vm::Cell> inner, std::shared_ptr<std::atomic<bool>> fail)
      : inner_(std::move(inner)), fail_(std::move(fail)) {}
  td::Status set_data_cell(td::Ref<vm::DataCell>&& cell) const override {
    return inner_->set_data_cell(std::move(cell));
  }
  td::Result<LoadedCell> load_cell() const override {
    if (*fail_) return td::Status::Error("injected CellDB read failure");
    return inner_->load_cell();
  }
  bool is_virtualized() const override { return inner_->is_virtualized(); }
  vm::CellUsageTree::NodePtr get_tree_node() const override { return inner_->get_tree_node(); }
  bool is_loaded() const override { return !*fail_ && inner_->is_loaded(); }
  LevelMask get_level_mask() const override { return inner_->get_level_mask(); }
 private:
  td::uint16 do_get_depth(td::uint32 level) const override { return inner_->get_depth(level); }
  const Hash do_get_hash(td::uint32 level) const override { return inner_->get_hash(level); }
};

void check_methods() {
  auto body = vm::CellBuilder().store_long(0xf20b, 16).finalize();
  auto code = test_methods_dispatcher({{99001, body}, {99002, body}});
  auto missing = test_smc_config(10);
  auto available = test_smc_config(20, {code});
  auto reference = test_library_reference(code);
  auto before = contract_methods_cache_stats();
  CHECK(parse_contract_methods(reference, missing.get()).is_error());
  auto methods = parse_contract_methods(reference, available.get()).move_as_ok();
  CHECK(methods == parse_contract_methods_uncached(reference, available.get()).move_as_ok());
  CHECK(methods == (std::vector<unsigned long long>{99001, 99002}));
  // Metadata survives both a historical missing library and no config at all.
  CHECK(parse_contract_methods(reference, missing.get()).move_as_ok() == methods);
  CHECK(parse_contract_methods(reference).move_as_ok() == methods);
  CHECK(parse_contract_methods_uncached(reference, missing.get()).is_error());
  CHECK(contract_methods_cache_stats().computations == before.computations + 2);
  CHECK(parse_contract_methods(code).move_as_ok() == methods);

  auto other = test_method_dispatcher(99003, body);
  auto other_ref = test_library_reference(other);
  CHECK(parse_contract_methods(other_ref, available.get()).is_error());
  CHECK(parse_contract_methods(other_ref, test_smc_config(30, {other}).get()).move_as_ok() ==
        (std::vector<unsigned long long>{99003}));
  auto nested = test_library_reference(other_ref);
  CHECK(parse_contract_methods(nested, test_smc_config(40, {other_ref}).get()).is_error());
  CHECK(parse_contract_methods(nested, test_smc_config(40, {other_ref, other}).get()).move_as_ok() ==
        (std::vector<unsigned long long>{99003}));

  auto fail = std::make_shared<std::atomic<bool>>(true);
  auto flaky = td::make_ref<ReadFaultCell>(test_method_dispatcher(99004, body), fail);
  before = contract_methods_cache_stats();
  CHECK(parse_contract_methods(flaky).is_error());
  *fail = false;
  CHECK(parse_contract_methods(flaky).move_as_ok() == (std::vector<unsigned long long>{99004}));
  *fail = true;
  CHECK(parse_contract_methods(flaky).is_ok()); // successful values need no cell reload
  CHECK(contract_methods_cache_stats().computations == before.computations + 2);

  before = contract_methods_cache_stats();
  for (int i = 0; i < 256; ++i) {
    // SETCP0 followed by DUP, not DICTPUSHCONST: code-as-state loggers.
    auto unsupported = vm::CellBuilder().store_long(0xff0020, 24).store_long(i, 32).finalize();
    CHECK(parse_contract_methods(unsupported).is_error());
    CHECK(parse_contract_methods(unsupported).is_error());
  }
  auto after = contract_methods_cache_stats();
  CHECK(after.entries == before.entries && after.inflight == 0);
  CHECK(after.errors == before.errors + 512 && after.computations == before.computations + 512);
}

td::Ref<vm::Cell> elector_code() {
  std::ifstream in(ELECTOR_CACHE_FIXTURE);
  CHECK(in.good());
  std::string encoded, part;
  while (in >> part) encoded += part;
  auto code = vm::std_boc_deserialize(td::base64_decode(encoded).move_as_ok()).move_as_ok();
  CHECK(td::base64_encode(code->get_hash().as_slice()) == "Yhf4csmfr8uHDywRo2L1kzm+lQlfcNALnP8vbc1p090=");
  return code;
}

void coins(vm::CellBuilder& b, int64_t amount) {
  CHECK(block::tlb::t_Grams.store_integer_ref(b, td::make_refint(amount)));
}

td::Ref<vm::Cell> members(int count, int extra = 0, int adnl = 7) {
  vm::Dictionary dict(256);
  for (int i = 1; i <= count; ++i) {
    vm::CellBuilder b;
    coins(b, 1000 + i + (i == 1 ? extra : 0));
    b.store_long(100, 32).store_long(65536 + i, 32).store_int256(*td::make_refint(100 + i), 256)
        .store_int256(*td::make_refint(adnl + i), 256);
    auto key = td::Bits256::zero();
    (key.bits() + 224).store_uint(i, 32);
    CHECK(dict.set(key, vm::load_cell_slice_ref(b.finalize())));
  }
  return dict.get_root_cell();
}

td::Ref<vm::Cell> election(uint32_t id, td::Ref<vm::Cell> members, int total = 3006,
                           bool finished = false, bool failed = false) {
  vm::CellBuilder b;
  b.store_long(id, 32).store_long(id + 100, 32);
  coins(b, 100); coins(b, total);
  CHECK(b.store_maybe_ref(std::move(members)));
  return b.store_long(failed, 1).store_long(finished, 1).finalize();
}

schema::AccountState state(td::Ref<vm::Cell> code, td::Ref<vm::Cell> election, int unrelated = 0) {
  schema::AccountState state{};
  state.account = block::StdAddress(-1, td::Bits256::zero());
  state.code = std::move(code);
  vm::CellBuilder b;
  CHECK(b.store_maybe_ref(std::move(election)));
  // Arbitrary changes to credits/past_elections and accumulated grams.
  CHECK(b.store_maybe_ref(vm::CellBuilder().store_long(unrelated, 32).finalize()));
  CHECK(b.store_maybe_ref(vm::CellBuilder().store_long(unrelated + 1, 32).finalize()));
  coins(b, unrelated + 100);
  state.data = b.store_long(unrelated, 32).store_zeroes(256).finalize();
  return state;
}

void equal(const std::optional<schema::ValidatorElection>& a,
           const std::optional<schema::ValidatorElection>& b) {
  CHECK(a.has_value() == b.has_value());
  if (!a) return;
  CHECK(a->election_id == b->election_id && a->elect_close == b->elect_close);
  CHECK(td::cmp(a->min_stake, b->min_stake) == 0 && td::cmp(a->total_stake, b->total_stake) == 0);
  CHECK(a->failed == b->failed && a->finished == b->finished && a->source_mc_seqno == b->source_mc_seqno);
  CHECK(a->participants.size() == b->participants.size());
  for (size_t i = 0; i < a->participants.size(); ++i) {
    const auto& x = a->participants[i]; const auto& y = b->participants[i];
    CHECK(x.election_id == y.election_id && x.validator_pubkey == y.validator_pubkey && td::cmp(x.stake, y.stake) == 0);
    CHECK(x.max_factor == y.max_factor && x.stake_holder_address == y.stake_holder_address && x.adnl_addr == y.adnl_addr);
    CHECK(x.source_mc_seqno == y.source_mc_seqno);
  }
}

void check_elections() {
  auto code = elector_code();
  auto config = test_smc_config(100);
  auto a = election(1000, members(3));
  auto before = elector::election_cache_stats();
  for (auto mc : {30U, 10U, 20U, 30U}) {
    auto s = state(code, a, mc);
    auto cached = elector::parse_election(s, config, mc * 100, mc).move_as_ok();
    auto direct = elector::parse_election_uncached(s, config, mc * 100, mc).move_as_ok();
    equal(cached, direct);
    CHECK(cached->participants.size() == 3 && cached->participants.front().stake->to_long() == 1001);
    CHECK(cached->source_mc_seqno == mc && cached->participants.front().source_mc_seqno == mc);
    cached->participants.front().validator_pubkey = "caller-owned mutation";
  }
  CHECK(elector::election_cache_stats().computations == before.computations + 1);
  // Same election ID: more participants, stake top-up, ADNL change, then flags.
  for (auto changed : {election(1000, members(4), 4010), election(1000, members(3, 50), 3056),
                       election(1000, members(3, 0, 20)), election(1000, members(3), 3006, true),
                       election(1000, members(3), 3006, false, true), a}) {
    auto s = state(code, changed);
    equal(elector::parse_election(s, config, 2000, 40).move_as_ok(),
          elector::parse_election_uncached(s, config, 2000, 40).move_as_ok());
  }
  CHECK(elector::election_cache_stats().computations == before.computations + 6);
  CHECK(!elector::parse_election(state(code, {}), config, 3000, 50).move_as_ok());
  CHECK(!elector::parse_election(state(code, {}, 99), config, 4000, 60).move_as_ok());

  // Changing the VM version partitions otherwise identical inputs.
  for (auto version : {1U, 2U, 1U}) {
    td::Ref<vm::Cell> root;
    test_smc_config(100, {}, {{8, vm::CellBuilder().store_long(0xc4, 8)
        .store_long(version, 32).store_zeroes(64).finalize()}}, &root);
    ton::BlockIdExt id{ton::masterchainId, ton::shardIdAll, 0, td::Bits256::zero(), td::Bits256::zero()};
    std::shared_ptr<block::ConfigInfo> cfg =
        block::ConfigInfo::extract_config(root, id, block::ConfigInfo::needCapabilities).move_as_ok();
    CHECK(cfg->get_global_version() == int(version));
    equal(elector::parse_election(state(code, a), cfg, 100, 70).move_as_ok(),
          elector::parse_election_uncached(state(code, a), cfg, 100, 70).move_as_ok());
  }
  CHECK(elector::election_cache_stats().computations == before.computations + 9);

  auto fail = std::make_shared<std::atomic<bool>>(true);
  auto flaky_members = td::make_ref<ReadFaultCell>(members(3), fail);
  auto flaky = state(code, election(2000, flaky_members));
  CHECK(elector::parse_election(flaky, config, 100, 80).is_error());
  *fail = false;
  equal(elector::parse_election(flaky, config, 100, 80).move_as_ok(),
        elector::parse_election_uncached(flaky, config, 100, 80).move_as_ok());

  auto unreadable_key = state(code, election(2001, members(3)));
  unreadable_key.data = td::make_ref<ReadFaultCell>(unreadable_key.data, fail);
  *fail = true;
  CHECK(elector::parse_election(unreadable_key, config, 100, 81).is_error());
  *fail = false;
  CHECK(elector::parse_election(unreadable_key, config, 100, 81).move_as_ok()->election_id == 2001);

  // Unknown code may depend on NOW even with an identical c4. Never memoize it
  // by the audited Elector's subtree assumptions. DROP; NOW; 0 0 0 NULL 0 0.
  auto timed_code = vm::CellBuilder().store_long(0x30f823, 24).store_long(0x7070706d7070LL, 48).finalize();
  auto unknown = state(timed_code, a);
  before = elector::election_cache_stats();
  for (auto now : {100U, 200U, 100U}) {
    auto result = elector::parse_election(unknown, config, now, 90).move_as_ok();
    CHECK(result->election_id == now);
  }
  CHECK(elector::election_cache_stats().bypasses == before.bypasses + 3);

  for (int i = 3000; i < 3040; ++i) {
    CHECK(elector::parse_election(state(code, election(i, members(3))), config, 100, 100).is_ok());
  }
  CHECK(elector::election_cache_stats().entries == 16);
  CHECK(elector::election_cache_stats().bytes <= 8 * 1024 * 1024);
  equal(elector::parse_election(state(code, a), config, 100, 10).move_as_ok(),
        elector::parse_election_uncached(state(code, a), config, 100, 10).move_as_ok());

  // Real-sized participant list, parallel MCs with different unrelated data.
  // Every result must match the uncached VM and carry its caller's source MC.
  auto large = election(5000, members(380), 452390);
  auto expected = elector::parse_election_uncached(state(code, large), config, 100, 0).move_as_ok();
  CHECK(expected->participants.size() == 380);
  before = elector::election_cache_stats();
  std::promise<void> start;
  auto go = start.get_future().share();
  std::vector<std::future<td::Result<std::optional<schema::ValidatorElection>>>> tasks;
  for (uint32_t mc = 200; mc < 208; ++mc) {
    tasks.push_back(std::async(std::launch::async, [&, mc] {
      go.wait();
      return elector::parse_election(state(code, large, mc), config, 100 + mc, mc);
    }));
  }
  start.set_value();
  for (size_t i = 0; i < tasks.size(); ++i) {
    expected->source_mc_seqno = 200 + i;
    for (auto& p : expected->participants) p.source_mc_seqno = 200 + i;
    equal(tasks[i].get().move_as_ok(), expected);
  }
  CHECK(elector::election_cache_stats().computations == before.computations + 1);
}

void check_trigger_counters() {
  g_statistics.generate_report_and_reset();
  auto parsed = std::make_shared<ParsedBlock>();
  parsed->mc_block_.config_ = test_smc_config(100, {}, {{1, vm::CellBuilder().store_zeroes(256).finalize()}});
  schema::Block block{};
  block.workchain = -1; block.seqno = 123; block.gen_utime = 100;
  schema::Transaction tx{};
  tx.account = block::StdAddress(-1, td::Bits256::zero());
  tx.account_state_hash_before = td::Bits256::zero();
  tx.account_state_hash_after = td::Bits256::ones();
  tx.description = schema::TransactionDescr_tick_tock{};
  block.transactions.push_back(tx);
  parsed->blocks_.push_back(block);
  parsed->account_states_.push_back(state(elector_code(), election(9999, members(3))));
  CHECK(StakingEventProcessor(parsed).process_snapshots().is_ok());
  CHECK(parsed->validator_elections_.size() == 1);
  auto report = g_statistics.generate_report_and_reset();
  for (auto counter : {"indexer.elector.compare.error", "indexer.elector.election.trigger", "indexer.elector.participant_getter"}) {
    CHECK(report.find(std::string(counter) + " COUNT : 1\n") != std::string::npos);
  }
}

}  // namespace

int main() {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  vm::init_op_cp0();
  check_lru_and_budget();
  check_coalescing(false);
  check_coalescing(true);
  check_methods();
  check_elections();
  check_trigger_counters();
  std::cout << "Memoized parsers: bounded LRU, concurrent coalescing, retries, metadata and historical Elector parity: OK\n";
}
