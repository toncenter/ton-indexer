#include "smc-interfaces/ParentAccountCache.h"
#include "smc-interfaces/ParentCellCache.h"
#include "smc-interfaces/FetchAccountFromShard.h"
#include "smc-interfaces/execute-smc.h"
#include "smc-fixture.h"
#include "cell-reader.h"
#include "validator/block-handle.hpp"

#include <atomic>
#include <future>

namespace {

td::Bits256 identity(uint32_t n) {
  auto hash = td::Bits256::zero();
  (hash.bits() + 224).store_uint(n, 32);
  return hash;
}

using Reader = TestCellDbReader;

schema::AccountState parent(const block::StdAddress& address, int value) {
  schema::AccountState state{};
  state.account = address;
  state.code = test_method_dispatcher(97026, vm::CellBuilder().store_long(0xf823, 16).finalize()); // NOW
  state.data = vm::CellBuilder().store_long(value, 32).finalize();
  state.code_hash = state.code->get_hash().bits();
  state.data_hash = state.data->get_hash().bits();
  state.hash = state.data_hash.value();
  state.balance.grams = td::make_refint(123);
  state.account_status = "active";
  return state;
}

void check_segments_and_invalidation() {
  memo::Cache<int, int> cache(4096, 3, memo::Policy::segmented);
  auto weight = [](int) { return size_t(0); };
  auto get = [&](int key) { return cache.get(key, [=]() -> td::Result<int> { return key; }, weight).move_as_ok(); };
  get(1); get(2); get(1); // protect 1
  for (int i = 3; i < 30; ++i) get(i);
  auto calls = cache.stats().computations;
  CHECK(*get(1) == 1 && cache.stats().computations == calls);
  cache.update(1, [](int) { return std::make_shared<const int>(99); }, weight);
  for (int i = 30; i < 60; ++i) get(i);
  CHECK(*get(1) == 99); // update kept its protected status
  cache.erase(1);
  CHECK(*get(1) == 1);
  cache.clear();
  CHECK(cache.stats().entries == 0 && cache.stats().bytes <= 4096);

  std::promise<void> started, release;
  auto gate = release.get_future().share();
  auto old = std::async(std::launch::async, [&] {
    return cache.get(100, [&]() -> td::Result<int> { started.set_value(); gate.wait(); return 10; }, weight);
  });
  started.get_future().wait();
  cache.erase(100);
  cache.put(100, std::make_shared<const int>(20), weight);
  release.set_value();
  CHECK(*old.get().move_as_ok() == 10);
  CHECK(*get(100) == 20); // late loader cannot overwrite the replacement
}

void check_versions() {
  constexpr size_t budget = 512 * 1024;
  ParentAccountCache cache(budget);
  const block::StdAddress addr(0, identity(1));
  auto a = parent(addr, 1), b = parent(addr, 2);
  auto reader = std::make_shared<Reader>();
  reader->add(a.code); reader->add(a.data); reader->add(b.data);
  unsigned lookups = 0;
  auto load_a = [&]() -> td::Result<schema::AccountState> { ++lookups; return a; };
  auto load_b = [&]() -> td::Result<schema::AccountState> { ++lookups; return b; };
  auto v100 = cache.advance(100, identity(100), identity(99), {});
  auto held = cache.lookup(v100, addr, reader, load_a).move_as_ok();
  auto v101 = cache.advance(101, identity(101), identity(100), {});
  CHECK(cache.lookup(v101, addr, reader, load_a).move_as_ok().hash == a.hash && lookups == 1);
  auto v102 = cache.advance(102, identity(102), identity(101), {{addr, &b}});
  CHECK(cache.lookup(v102, addr, reader, load_b).move_as_ok().hash == b.hash && lookups == 1);
  CHECK(cache.lookup(v100, addr, reader, load_a).move_as_ok().hash == a.hash && lookups == 2);
  CHECK(cache.lookup(v102, addr, reader, load_b).move_as_ok().hash == b.hash && lookups == 2);
  CHECK(vm::load_cell_slice(held.data).prefetch_ulong(32) == 1);
  CHECK(cache.advance(101, identity(101), identity(100), {}).epoch == v101.epoch); // retry

  auto v103 = cache.advance(103, identity(103), identity(102), {{addr, nullptr}});
  CHECK(cache.state_stats().entries == 0);
  CHECK(cache.lookup(v103, addr, reader, load_b).is_ok() && lookups == 3);
  // Neither a gap nor a different block identity can extend an old value.
  auto v105 = cache.advance(105, identity(105), identity(104), {});
  CHECK(v105.epoch != v103.epoch && cache.state_stats().entries == 0);
  CHECK(cache.lookup(v103, addr, reader, load_b).is_ok() && lookups == 4);
  CHECK(cache.state_stats().entries == 0);
  CHECK(cache.lookup(v105, addr, reader, load_a).is_ok() && lookups == 5);
  auto fork = cache.advance(105, identity(900), identity(104), {});
  CHECK(fork.epoch != v105.epoch && cache.state_stats().entries == 0);
  CHECK(cache.lookup(fork, addr, reader, load_b).is_ok());
  CHECK(cache.retained_bytes() <= budget);

  // Snapshot/config-dependent getters always execute against current arguments.
  auto state = cache.lookup(fork, addr, reader, load_b).move_as_ok();
  for (unsigned now : {1000U, 2000U}) {
    auto result = execute_smc_method<1>(addr, state.code, state.data, test_smc_config(now), "get_wallet_data", {},
                                     {vm::StackEntry::Type::t_int});
    CHECK(result.is_ok() && result.ok()[0].as_int()->to_long() == now);
  }
  // A warmed library reference still resolves against the caller's config.
  auto library = a.code;
  b.code = test_library_reference(library);
  b.code_hash = b.code->get_hash().bits();
  reader->add(b.code);
  auto v106 = cache.advance(106, identity(901), identity(900), {{addr, &b}});
  state = cache.lookup(v106, addr, reader, load_b).move_as_ok();
  CHECK(execute_smc_method<1>(addr, state.code, state.data, test_smc_config(3000, {library}),
      "get_wallet_data", {}, {vm::StackEntry::Type::t_int}).move_as_ok()[0].as_int()->to_long() == 3000);
  CHECK(execute_smc_method<1>(addr, state.code, state.data, test_smc_config(4000),
      "get_wallet_data", {}, {vm::StackEntry::Type::t_int}).is_error());
}

void check_late_publication() {
  ParentAccountCache cache;
  const block::StdAddress addr(0, identity(2));
  auto a = parent(addr, 1), b = parent(addr, 2);
  auto reader = std::make_shared<Reader>();
  reader->add(a.code); reader->add(a.data); reader->add(b.data);
  auto v100 = cache.advance(100, identity(100), identity(99), {});
  std::promise<void> started, release;
  auto gate = release.get_future().share();
  auto old = std::async(std::launch::async, [&] {
    return cache.lookup(v100, addr, reader, [&]() -> td::Result<schema::AccountState> {
      started.set_value(); gate.wait(); return a;
    });
  });
  started.get_future().wait();
  auto v101 = cache.advance(101, identity(101), identity(100), {{addr, &b}});
  CHECK(cache.lookup(v101, addr, reader, [&]() -> td::Result<schema::AccountState> { return b; }).is_ok());
  release.set_value();
  CHECK(old.get().move_as_ok().hash == a.hash);
  CHECK(cache.lookup(v101, addr, reader, []() -> td::Result<schema::AccountState> {
    return td::Status::Error("New state must remain cached");
  }).move_as_ok().hash == b.hash);

  // Loads behind the head MAY populate the cache if the complete intervening
  // change journal proves the parent untouched (normal parallel actor timing).
  const block::StdAddress other(0, identity(3));
  a.account = other;
  CHECK(cache.lookup(v100, other, reader, [&]() -> td::Result<schema::AccountState> { return a; }).is_ok());
  CHECK(cache.lookup(v101, other, reader, []() -> td::Result<schema::AccountState> {
    return td::Status::Error("Untouched historical load should be reusable");
  }).is_ok());
}

void check_lazy_cells() {
  ParentCellCache cache(32 * 1024);
  auto reader = std::make_shared<Reader>();
  auto leaf = vm::CellBuilder().store_long(42, 32).finalize();
  auto cold = vm::CellBuilder().store_long(43, 32).finalize();
  auto root = vm::CellBuilder().store_ref(leaf).store_ref(cold).finalize();
  reader->add(root);
  auto descriptor = ParentCellCache::describe(root);
  auto proxy = cache.wrap(descriptor, reader);
  CHECK(reader->reads == 0 && cache.stats().entries == 0);
  auto slice = vm::load_cell_slice(proxy);
  CHECK(reader->reads == 1 && cache.stats().entries == 1);
  CHECK(vm::load_cell_slice(slice.prefetch_ref(0)).prefetch_ulong(32) == 42);
  CHECK(reader->reads == 2 && cache.stats().entries == 2); // cold ref was never read
  // A new snapshot has new lazy refs, but the warmed images need no DB reads.
  auto next = cache.wrap(descriptor, reader);
  CHECK(vm::load_cell_slice(vm::load_cell_slice(next).prefetch_ref(0)).prefetch_ulong(32) == 42);
  CHECK(reader->reads == 2);

  reader->fail = true;
  auto cold_proxy = vm::load_cell_slice(next).prefetch_ref(1);
  CHECK(cold_proxy->load_cell().is_error());
  reader->fail = false;
  CHECK(cold_proxy->load_cell().is_ok());
  CHECK(cache.stats().errors == 1);

  // Eviction accounts every loaded image, including children reached later.
  for (unsigned i = 0; i < 200; ++i) {
    auto cell = vm::CellBuilder().store_long(i + 100, 32).finalize();
    reader->add(cell);
    CHECK(cache.wrap(ParentCellCache::describe(cell), reader)->load_cell().is_ok());
    CHECK(cache.stats().bytes <= 32 * 1024);
  }
  CHECK(cache.stats().evictions > 0);
  CHECK(vm::load_cell_slice(slice.prefetch_ref(0)).prefetch_ulong(32) == 42); // held graph survives
  // Persistent images never retain the source reader or its snapshot.
  std::weak_ptr<Reader> weak = reader;
  proxy.clear(); next.clear(); cold_proxy.clear(); slice = vm::CellSlice{};
  reader.reset();
  CHECK(weak.expired());
}

class BlockData final : public ton::validator::BlockData {
 public:
  explicit BlockData(unsigned seqno) : seqno_(seqno) {}
  td::BufferSlice data() const override { return {}; }
  ton::FileHash file_hash() const override { return identity(seqno_); }
  ton::BlockIdExt block_id() const override { return {-1, ton::shardIdAll, seqno_, identity(seqno_), identity(seqno_)}; }
  td::Ref<vm::Cell> root_cell() const override { return {}; }
 private:
  unsigned seqno_;
};

ParsedBlock parsed(unsigned seqno) {
  ParsedBlock block;
  auto data = td::make_ref<BlockData>(seqno);
  auto handle = std::make_shared<ton::validator::BlockHandleImpl>(data->block_id());
  handle->set_merge(false);
  handle->set_prev(td::make_ref<BlockData>(seqno - 1)->block_id());
  handle->flushed_upto(handle->version());
  block.mc_block_.shard_blocks_diff_.push_back({data, {}, handle});
  schema::Block schema{};
  schema.mc_block_seqno = seqno;
  block.blocks_.push_back(schema);
  return block;
}

void check_missing_final_state() {
  ParentAccountCache cache;
  auto a = parent(block::StdAddress(0, identity(7)), 1);
  auto reader = std::make_shared<Reader>(); reader->add(a.code); reader->add(a.data);
  auto before = parsed(100);
  auto v100 = cache.observe(100, before);
  CHECK(v100.owner == &cache);
  CHECK(cache.lookup(v100, a.account, reader, [&]() -> td::Result<schema::AccountState> { return a; }).is_ok());
  auto after = parsed(101);
  a.last_trans_lt = 10;
  after.account_states_.push_back(a); // earlier shard block's state survived
  schema::Transaction tx{};
  tx.account = a.account; tx.lt = 20; tx.mc_seqno = 101; tx.account_state_hash_after = identity(999);
  after.blocks_[0].transactions.push_back(tx);
  CHECK(cache.observe(101, after).owner == &cache);
  CHECK(cache.state_stats().entries == 0); // latest read failure invalidated
}

void check_bounded_journal_and_state_memory() {
  constexpr size_t budget = 512 * 1024;
  ParentAccountCache cache(budget);
  auto reader = std::make_shared<Reader>();
  const auto old = cache.advance(100, identity(100), identity(99), {});
  auto current = old;
  for (unsigned n = 101; n <= 250; ++n) current = cache.advance(n, identity(n), identity(n - 1), {});
  auto state = parent(block::StdAddress(0, identity(10000)), 1);
  reader->add(state.code); reader->add(state.data);
  CHECK(cache.lookup(old, state.account, reader, [&]() -> td::Result<schema::AccountState> { return state; }).is_ok());
  CHECK(cache.state_stats().entries == 0); // cannot prove changes outside journal
  CHECK(cache.advance(100, identity(100), identity(99), {}).owner == nullptr);
  for (unsigned i = 1; i <= 300; ++i) {
    state.account = block::StdAddress(0, identity(10000 + i));
    auto result = cache.lookup(current, state.account, reader, [&]() -> td::Result<schema::AccountState> { return state; });
    CHECK(result.is_ok());
    CHECK(cache.retained_bytes() <= budget);
  }
  CHECK(cache.state_stats().evictions > 0);
  auto report = cache.report();
  CHECK(report.find("indexer.cache.parents.stale_publish COUNT : 1\n") != std::string::npos);
  CHECK(cache.report().find("indexer.cache.parents.stale_publish COUNT : 0\n") != std::string::npos);
}

}  // namespace

void check_parent_cache() {
  check_segments_and_invalidation();
  check_versions();
  check_late_publication();
  check_lazy_cells();
  check_missing_final_state();
  check_bounded_journal_and_state_memory();
}
