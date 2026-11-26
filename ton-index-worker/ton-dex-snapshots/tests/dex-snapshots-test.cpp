#include <cmath>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>

#include <clickhouse/client.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include "block/block-parse.h"
#include "vm/cells/CellBuilder.h"
#include "td/utils/tests.h"
#include "td/utils/ScopeGuard.h"
#include "smc-interfaces/DexConstants.h"
#include "smc-interfaces/DexPools.h"
#include "PoolMath.h"
#include "PoolCache.h"
#include "SnapshotWriter.h"

using dex_snapshots::PoolInputs;

namespace {

td::RefInt256 num(const std::string& s) {
  return td::dec_string_to_int256(s);
}

bool close_rel(std::optional<double> value, double want, double rel) {
  double got = value.value_or(NAN);
  bool ok = std::fabs(got - want) <= rel * std::fabs(want);
  LOG_IF(ERROR, !ok) << "got " << got << ", want " << want << " within relative tolerance " << rel;
  return ok;
}

// Pools at mc seqno 102169248; expected values from an independent Python solver.
TEST(pool_math, curves) {
  auto check = [](const PoolInputs& pool, double spot, double depth0, double depth1) {
    auto p = dex_snapshots::spot_depth(pool);
    EXPECT_EQ(p.math_version, 1);
    EXPECT(close_rel(p.spot, spot, 1e-9));
    EXPECT(close_rel(p.depth0, depth0, 1e-6));
    EXPECT(close_rel(p.depth1, depth1, 1e-6));
  };
  // STON.fi v2 stableswap USDe / USD₮, A = 100.
  check({"stonfi", 2, "stableswap", num("102349862342"), num("102663816577"), td::make_refint(100)},
        1.000060056, 3.937585618e+10, 3.920482523e+10);
  // STON.fi v2 weighted stableswap TON / tsTON: A = 50, rate = 1.165, w0 = 0.25 (all × 1e18).
  check({"stonfi", 2, "weighted_stableswap", num("71453959666898"), num("93874412019361"),
         num("50000000000000000000"), num("1165000000000000000"), num("250000000000000000")},
        0.8527744842, 9.675465683e+13, 4.334201129e+13);
  // STON.fi v2 weighted constant product TON / STON, w0 = 0.25.
  check({"stonfi", 2, "weighted_const_product", num("40172652425750"), num("299064355295260"),
         {}, {}, num("250000000000000000")},
        2.48149207, 6.133321337e+11, 1.514298306e+12);
  // DeDust v1 stable DONE (9 decimals) / USD₮ (6): rate ratio 1000 (without rates, spot is off by 1.2%).
  check({"dedust", 1, "stable", num("6381707572587"), num("428732713"), {}, num("1000000000000000000000")},
        0.0001991512061, 3.337622432e+10, 6688585.334);
}

TEST(pool_math, spot_depth) {
  auto pow2 = [](int n) { return td::make_refint(1) << n; };
  auto spot_depth = [](td::RefInt256 r0, td::RefInt256 r1) {
    return dex_snapshots::spot_depth({"stonfi", 1, "constant_product", r0, r1});
  };
  // Reserves at the Coins limit and beyond (UInt128): no overflow, ratio exact to a few ulps.
  EXPECT(close_rel(spot_depth(pow2(120) - 1, td::make_refint(3)).spot, 3 * std::ldexp(1.0, -120), 1e-15));
  auto big = spot_depth(pow2(128) - 1, pow2(127) + 12345);
  EXPECT(big.spot == 0.5);
  EXPECT(close_rel(big.depth0, std::ldexp(1.0, 128) * (1.0 / std::sqrt(0.98) - 1.0), 1e-15));
}

td::Ref<vm::Cell> dedust_v2_storage(const block::StdAddress& admin) {
  vm::CellBuilder admin_cell;
  block::tlb::t_MsgAddressInt.store_std_address(admin_cell, admin.workchain, admin.addr);
  return vm::CellBuilder().store_zeroes(17)
      .store_ref(vm::CellBuilder().store_long(0x1234, 16).finalize())
      .store_ref(vm::CellBuilder().store_zeroes(26).finalize())
      .store_ref(admin_cell.store_zeroes(3).finalize()).finalize();
}

td::Bits256 state_init_hash(const td::Ref<vm::Cell>& data) {
  auto init = vm::CellBuilder().store_long(0b00110, 5)
      .store_ref(dex_constants::dedust_v2_pool_library_cells.at(0)).store_ref(data).finalize();
  return td::Bits256(init->get_hash().bits());
}

TEST(dex_pools, dedust_v2_identity) {
  auto verify = [](const block::StdAddress& address, const td::Ref<vm::Cell>& storage) {
    DedustPoolDetector::Result pool;
    pool.version = 2;
    return DedustPoolDetector::verify_identity(address, storage, {}, nullptr, pool);
  };
  auto canonical = dedust_v2_storage(dex_constants::dedust_v2_admin);
  auto hash = state_init_hash(canonical);
  ASSERT_TRUE(verify({0, hash}, canonical).is_ok());
  ASSERT_TRUE(verify({-1, hash}, canonical).is_error());
  hash.bits()[0] = !hash.bits()[0];
  ASSERT_TRUE(verify({0, hash}, canonical).is_error());

  auto other_admin = dex_constants::dedust_v2_admin;
  other_admin.addr.bits()[5] = !other_admin.addr.bits()[5];
  auto foreign = dedust_v2_storage(other_admin);
  ASSERT_TRUE(verify({0, state_init_hash(foreign)}, foreign).is_error());
}

// DeDust v1 pool storage: 4 refs, version, trade fee, LP supply, reserves, protocol fees, then the given tail bits.
td::Ref<vm::Cell> dedust_v1_storage(int version, td::RefInt256 r0, td::RefInt256 r1, td::Ref<vm::Cell> settings,
                                    td::Ref<vm::Cell> tail) {
  vm::CellBuilder root;
  for (int i = 0; i < 3; i++) {
    root.store_ref(vm::CellBuilder().finalize());
  }
  root.store_ref(settings).store_long(version, 16).store_long(10, 16);
  for (auto& c : {td::make_refint(1000), r0, r1, td::make_refint(0), td::make_refint(0)}) {
    block::tlb::t_Grams.store_integer_value(root, *c);
  }
  root.append_cellslice(vm::load_cell_slice(tail));
  return root.finalize();
}

// Storage version 4 reads decimals from ref 3 (here 6 and 18 decimals).
TEST(dex_pools, dedust_v1_stable_rates) {
  auto r0 = num("49075125000000"), r1 = num("48925275000133931721556653");
  auto stable_rate = [&](const td::Ref<vm::Cell>& storage, td::RefInt256 reserve1) {
    DedustPoolDetector::Result pool;
    pool.is_stable = true;
    pool.reserve_1 = r0;
    pool.reserve_2 = reserve1;
    DedustPoolDetector::read_stable_rates(storage, pool);
    return make_dex_pool_snapshot(pool, 1, td::Bits256::zero()).rate;
  };
  auto empty = vm::CellBuilder().finalize();
  auto decimals = vm::CellBuilder().store_long(6, 8).store_long(18, 8).finalize();
  auto old = dedust_v1_storage(4, r0, r1, decimals, empty);
  auto rate = stable_rate(old, r1);
  ASSERT_TRUE(rate.not_null() && td::cmp(rate, num("1000000")) == 0);
  EXPECT(close_rel(dex_snapshots::spot_depth({"dedust", 1, "stable", r0, r1, {}, rate}).spot, 999999992849.8247, 1e-9));
  ASSERT_TRUE(stable_rate(old, r1 + 1).is_null());

  // Version 5 and later: rate0 and rate1 follow the protocol fees.
  vm::CellBuilder rates;
  rates.store_int256(*num("1000000000000000000000000000000"), 128, false);
  rates.store_int256(*num("1000000000000000000000000000"), 128, false);
  rate = stable_rate(dedust_v1_storage(6, r0, r1, empty, rates.finalize()), r1);
  ASSERT_TRUE(rate.not_null() && td::cmp(rate, num("1000000000000000")) == 0);
}

block::StdAddress address(int n) {
  block::StdAddress a(0, td::Bits256::zero());
  a.addr.bits().store_uint(n, 32);
  return a;
}

TEST(dex_pools, out_of_order_removals) {
  DedustPoolDetector::Result pool;
  pool.address = address(2010);
  pool.version = 1;
  pool.asset_2 = address(2011);
  const auto code_hash = td::Bits256::zero();
  auto& cache = dex_pool_cache();
  cache.put(pool, code_hash);

  // An older block finishes first and must not consume the identity needed by a later removal.
  auto older = make_dex_pool_removal(pool.address, code_hash, 100);
  ASSERT_TRUE(older && older->removed);
  auto newer = make_dex_pool_removal(pool.address, code_hash, 200);
  ASSERT_TRUE(newer && newer->removed && newer->last_transaction_lt == 200);
  ASSERT_EQ(newer->asset1, older->asset1);
  cache.erase(pool.address);
}

TEST(dex_pools, restart_removal) {
  const char* dsn = std::getenv("DEX_SNAPSHOTS_TEST_DSN");
  if (dsn == nullptr) {
    LOG(INFO) << "Set DEX_SNAPSHOTS_TEST_DSN to run the pool cache restart test";
    return;
  }
  auto options = dex_snapshots::parse_writer_dsn(dsn).move_as_ok();
  const auto database = "dex_snapshots_test_restart_" + std::to_string(
      std::chrono::system_clock::now().time_since_epoch().count());
  clickhouse::ClientOptions client_options;
  client_options.SetHost(options.host).SetPort(options.port).SetUser(options.user).SetPassword(options.password);
  clickhouse::Client client(client_options);
  dex_snapshots::create_schema(client, database);
  SCOPE_EXIT { client.Execute("DROP DATABASE " + database); };

  StonfiPoolDetector::Result pool;
  pool.address = address(2020);
  pool.version = 2;
  pool.pool_type = "constant_product";
  pool.router = address(2021);
  pool.asset_0 = address(2022);
  pool.asset_1 = address(2023);
  pool.reserve_0 = td::make_refint(100);
  pool.reserve_1 = td::make_refint(200);
  auto code_hash = td::Bits256::zero();
  code_hash.bits()[0] = true;
  auto snapshot = make_dex_pool_snapshot(pool, 100, code_hash);
  dex_snapshots::insert_snapshots(client, database, {{100, 1767225600, {snapshot}}}, "live");

  client.Execute("INSERT INTO " + database + ".pool_snapshots (pool, mc_seqno) VALUES ('not an address', 100)");

  auto& cache = dex_pool_cache();
  ASSERT_TRUE(!cache.get(pool.address));
  dex_snapshots::restore_pool_cache(client, database);
  auto restored = cache.get(pool.address);
  ASSERT_TRUE(restored && restored->identity.code_hash.is_zero());
  auto removed = make_dex_pool_removal(pool.address, td::Bits256::zero(), 200);
  ASSERT_TRUE(removed && removed->removed && removed->last_transaction_lt == 200);
  ASSERT_EQ(removed->dex, snapshot.dex);
  ASSERT_EQ(removed->dex_version, snapshot.dex_version);
  ASSERT_EQ(removed->pool_type, snapshot.pool_type);
  ASSERT_TRUE(removed->router == snapshot.router);
  ASSERT_EQ(removed->asset0, snapshot.asset0);
  ASSERT_EQ(removed->asset1, snapshot.asset1);
  ASSERT_TRUE(removed->reserve0.is_null() && removed->reserve1.is_null());
  dex_snapshots::insert_snapshots(client, database, {{200, 1767225720, {*removed}}}, "live");
  // A delayed older block cannot resurrect the pool in pool_latest.
  dex_snapshots::insert_snapshots(client, database, {{150, 1767225660, {snapshot}}}, "live");
  bool found = false;
  auto latest = "SELECT removed, asset0, asset1 FROM " + database + ".pool_latest FINAL WHERE pool != 'not an address'";
  client.Select(latest, [&](const clickhouse::Block& rows) {
    for (std::size_t i = 0; i < rows.GetRowCount(); ++i) {
      found = true;
      ASSERT_EQ(rows[0]->As<clickhouse::ColumnUInt8>()->At(i), 1);
      ASSERT_EQ(std::string(rows[1]->As<clickhouse::ColumnString>()->At(i)), snapshot.asset0);
      ASSERT_EQ(std::string(rows[2]->As<clickhouse::ColumnString>()->At(i)), snapshot.asset1);
    }
  });
  ASSERT_TRUE(found);

  // Restart again with a removal as the latest persisted row.
  cache.erase(pool.address);
  dex_snapshots::restore_pool_cache(client, database);
  ASSERT_TRUE(make_dex_pool_removal(pool.address, td::Bits256::zero(), 300).has_value());
  cache.erase(pool.address);
}

}  // namespace

int main() {
  auto& runner = td::TestsRunner::get_default();
  runner.run_all();
  return runner.any_test_failed() ? 1 : 0;
}
