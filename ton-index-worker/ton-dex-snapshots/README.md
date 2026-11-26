# DEX pool snapshots

`ton-index-clickhouse` writes the state of trusted STON.fi v1/v2 and DeDust v1/v2 pools to ClickHouse: one pool snapshot per masterchain block where a pool changes, with spot and depth for supported curves.

## Components and data flow

### Indexer

- `ton-index-clickhouse --dex-snapshots-database dex_snapshots` detects and verifies pools. It writes their pool snapshots into that database on the same server.
- At startup, `ton-index-clickhouse` creates the pool tables.
- A failed pool snapshot insert logs an ERROR line with the dropped masterchain range and never stops indexing.
- Without `--dex-snapshots-database`, `ton-index-clickhouse` skips STON.fi pool detection and builds no pool snapshots. It still detects DeDust pools.
- `ton-index-postgres` does not write pool snapshots.

### ClickHouse tables

[sql/schema.sql](sql/schema.sql) defines the ClickHouse tables.

| Table | Contents |
|---|---|
| `pool_snapshots` | Pool snapshots |
| `pool_latest` | Each pool's newest state (filled by materialized view) |
| `pool_daily` | Each pool's newest state per day (filled by materialized view) |
| `pool_fees` | Each pool's newest swap fee (filled by materialized view) |

- Materialized views fill `pool_latest`, `pool_daily`, and `pool_fees` on insert. Refill them after adding data with ATTACH or RESTORE.

## Rules

### Pool trust

- Store a pool only when both its getters and trust checks pass.
- Write a removal row when a cached trusted pool stops passing.
- At startup, `ton-index-clickhouse` loads the identity of every pool in `pool_latest` into its cache.
- A pool removed after a restart therefore still gets a removal row.
- Detection checks a loaded pool again before trusting it.
- **DeDust v1**: must match the factory's derived pool address.
- **DeDust v2**: must have the DeDust admin and match its original StateInit address in workchain 0.
- **STON.fi**: must have an allowlisted router that derives the pool address. Both wallets must belong to that router and match their masters' derived addresses.

### Curves and spot math

- Compute spot and depth only for unlocked pools that use a supported curve and have positive reserves.
- A DeDust v2 pool counts as locked until its status is ready (2), and while its swaps are off.
- Invariant curves use `amp`, `rate`, and `w0` as the getters return them.
- For DeDust stable, `rate` is asset1's storage rate over asset0's × 1e18, which carries the decimals.

| Curve family | Curves | Method |
|---|---|---|
| Constant product | STON.fi v1/v2 `constant_product`, DeDust v1/v2 `volatile` | Closed form |
| Invariant | STON.fi v2 `stableswap`, `weighted_stableswap`, `weighted_const_product`; DeDust v1 `stable` | From invariants; depths found by bisection |

### Constants

- `DexConstants.{h,cpp}` holds the compiled allowlists and code hashes.

## Operations

### Launch order

B and S are masterchain block seqnos.

1. Restart `ton-index-clickhouse` with `--dex-snapshots-database dex_snapshots`. It keeps running and writes live pool snapshots from block S onward with writer `live`.
2. Backfill beside it, into the same ClickHouse database:

   ```sh
   ton-dex-snapshots-backfill --db <node db> --secondary <new dir> --from B --to <S plus about 1000> \
     --accounts-file <pool list> --dex-snapshots-clickhouse clickhouse://<user>:<password>@<host>:9000/dex_snapshots
   ```

   - `--secondary` takes a new directory for every write (RocksDB state and stats files). Backfill only reads the node's database.
   - At block B, backfill snapshots every listed pool (see [Pool list](#pool-list)). History therefore starts with all pools that exist at B.
   - Later blocks add pool changes, written as `backfill`.
   - `-t` and `--parallel` set the reading threads.
   - Overlap with the live indexer is safe. Rows of the same pool and block collapse.
   - Every `--progress-sec` seconds (default 10), it logs listed accounts read, then blocks done, chain time reached, speed, and an ETA.
   - It writes no Postgres or kvrocks data. It exits non-zero if any blocks were dropped.

### Pool list

`--accounts-file` lists raw addresses (`0:HEX`), one per line. Backfill skips listed accounts that are not trusted pools at block B. It finds pools created after block B when they first change.

Collect today's pools from these sources:

- **DeDust v1**: take the `address` fields of `https://api.dedust.io/v2/pools` (user-friendly form) and convert them to raw.
- **kvrocks code-hash index**: read `ton-index:v1:idx:latest_account_states:code_hash:<base64 code hash>:by_account`, a sorted set of raw addresses. The hashes below and in `DexConstants.cpp` are hex; convert them to base64 for the key.
  - **STON.fi v1/v2**: use `stonfi_v1_pool_code_hashes` and `stonfi_v2_pool_code_hashes` from `DexConstants.cpp`.
  - **DeDust v2**: pool code is a library reference cell. Index keys use the hashes of those cells, not the library hashes:
    - `3997A5C1EE8923E93CF3A6A98EA3EF9482C64DD8AEA2BA8365689E14D99C760D` (CPMM v2.0)
    - `5851780D2386E989B9AB956DA7CEC5C69A667B9E525F628DCBA5EB02C1EE0367` (CPMM v2.1)
  - **DeDust v1**: the index also covers DeDust v1 (`dedust_v1_pool_code_hashes`).

### Outage refill

`ton-index-clickhouse` logs one ERROR line per dropped masterchain range (`DEX snapshots writer: dropped mc seqno`) with its reason. To refill a dropped range, run `ton-dex-snapshots-backfill` over that range.

### Tests

From the repository root, with `build/` configured as in CI:

```sh
cmake --build build --target ton-dex-snapshots-test ton-dex-snapshots-backfill ton-index-clickhouse
DEX_SNAPSHOTS_TEST_DSN=clickhouse://127.0.0.1:9000 ctest --test-dir build/ton-index-worker/ton-dex-snapshots --output-on-failure
```

Omitting `DEX_SNAPSHOTS_TEST_DSN` skips the pool cache restart test.
