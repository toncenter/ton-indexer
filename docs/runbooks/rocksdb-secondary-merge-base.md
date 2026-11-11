# Secondary CellDB merge-base failure (2026-11-10)

## Incident and evidence

The Gramstat live reader aborted at `celldb.cpp:94`,
`CHECK(merge_in.existing_value)`, at **09:18:41.817597 UTC** and again at
**18:17:21.765238038 UTC**. The second time, the ordinary indexer independently
reported the same assertion **7.274386 ms earlier**. The node stayed running;
neither incident showed a new kernel OOM. Both indexers recovered automatically.

The failing stack is a read:

`MergeOperatorAddCellRefcnt::FullMergeV2` → `DBImplSecondary::GetImpl` →
`td::RocksDbSecondary::get` → `vm::CellLoader::load` → shard-hash dictionary
iteration → `IndexQuery::fetch_shard_blocks`.

Recovery checks found all **695 MC blocks/deltas in 104526260..104526954** after
the first incident and **709 in 104603705..104604413** after the second, with
known balances and matching timestamps. These are bounded recovery checks,
not a claim that every historical secondary read was correct.

Server evidence is under
`/opt/gramstat-full/incidents/20261110T181848Z-celldb-merge-assert-repeat/`:

- `crash-and-resume.log`, `ordinary-indexer-journal.txt`, `snapshot.json`,
  `kernel.log`, `recovery-verification.json`;
- `secondary-race-probe-result.json` (v2), with the earlier attempt as `.v1`;
- `secondary-fix-result-v1.json` and `secondary-fix-v1/build.log` (A/B check).

The exact failing production key was not captured. The mechanism below is
reproduced with the deployed library, but its involvement in those particular
production reads remains an inference.

## Reproduced mechanism

1. Primary puts a base value and flushes it, then appends a merge in its WAL.
2. A secondary reads that merge into its **active** memtable.
3. Primary adds another merge (or deletes the key), flushes, and optionally
   compacts. The next WAL is empty.
4. Secondary catch-up installs the new MANIFEST/SST view, but retains its old
   active memtable: WAL recovery normally rotates it only upon another record,
   and the existing cleanup only removes immutable memtables.
5. The old operand is applied again against the new SST contents. A base `0`
   followed by two `+1` merges yields primary **2**, secondary **3**. Following
   deletion/compaction, the stale operand has **no base** at all.

Reopening the secondary returns the correct value/NotFound. The test operator
returns `false` on a missing base, producing `Corruption: Merge operator failed`
where TON's operator aborts. This happens sequentially; no concurrent threads
are needed. A separate probe of `LastSequence()` versus `SuperVersion` ordering
did **not** reproduce an invalid interleaved read. The other catch-up thread in
the captured crash belonged to StateDB, not CellDB.

## Fix and regression coverage

`ton-index-worker/patches/fix-secondary-flushed-memtable.patch` retires a
nonempty active memtable when its tracked WAL number is **strictly below that
column family's MANIFEST log boundary**. Retirement happens before WAL replay
can update the tracking map. The old table goes through the normal immutable
list/refcount cleanup, preserving readers that still pin an old SuperVersion.
The new table uses `kMaxSequenceNumber` until its first WAL record establishes
the earliest sequence: another column family's flush may have advanced the
MANIFEST's global sequence beyond still-unreplayed WAL records.

The CMake patch target runs after `fix-secondary-open.patch`. In particular,
the existing protection against dropping a lower current WAL just because a
higher empty WAL exists is retained. The nested RocksDB Git repository is an
ignored submodule; these tracked patch files are the reproducible source of
the changes, rather than an unpublished submodule commit.

Build and run the public-API suite:

```sh
cmake --build build --target test-rocksdb-secondary -j2
ctest --test-dir build -R '^rocksdb_secondary$' --output-on-failure
```

The executable optionally accepts a **new** synthetic-test directory. By
default it creates a unique directory under RocksDB's test directory; failed
fixtures are retained. Checks remain active in Release builds.

Eight cases cover flush without compaction, flush with compaction, deletion,
repeated catch-up, a pinned old iterator reseeking after retirement, unflushed
records in the next WAL, a precreated higher empty WAL, per-column-family flush
boundaries, and catch-up from an initially empty secondary.

The Linux A/B run completed **2026-11-10 18:51:29.810354 UTC** against the existing
production-build static library, replacing only the secondary implementation
object in the test executables:

| Check | Original | Fixed |
| --- | --- | --- |
| Regression suite | 6 failures / 8 cases | 8 passed |
| Sequential merge | 3 (expected 2) | 2 |
| Sequential delete | missing base / Corruption | NotFound |
| New WAL and higher empty WAL | passed | passed |
| Original four-scenario probe | stale merges / missing base | no mismatch or missing base |

The test service had a **1 GiB / 1 CPU / 600 s** budget. NodeDB was inaccessible;
source/build trees and the production binary were read-only. SHA-256 and mtime
checks confirmed the original library, source and binary were unchanged.

Local macOS CMake generation, ordered patch application (including repeated
application), and compilation of the new test target's object passed. The full
local build stopped in existing RocksDB `cache/charged_cache.cc` and `cache.cc`:
`port/port_posix.h` does not declare `pid_t` with the installed Clang 23 toolchain.
Consequently no macOS CTest pass is claimed; the successful runtime checks above
are the Linux A/B executables linked against the deployment's library.

At this verification point, production still runs reader revision
`437bef63faf1ba6d16bb73981046bb768e4445ca`, executable SHA-256
`c3fdeebd1af90b30785d03b1ee19d057ddca3c37a8cb8eecd99f32729bfb951d`.
**This is a tested candidate, not a production rollout.** A rollout must retain
the Gramstat-only scope and verify resume coverage, live lag and rearmed PID-bound
observers. The completed early-supply backfill does not need to be rerun.

## Production release, 2026-11-11

The operator authorized a Gramstat-only rollout after the fourth assertion
(**12:37:49.549852561 UTC**). The ordinary indexer independently asserted
**24.725192 ms earlier**; both automatically restarted, while the node remained
running. All **330 blocks/deltas in 104761290..104761619** passed the recovery
check. This still does not identify the exact failing CellDB key.

The release build first relinked the existing production objects/libraries and
reproduced the running executable **byte-for-byte**. It then compiled only the
patched secondary implementation using the original Release/O3 flags, replaced
that member in a private copy of `librocksdb.a`, and relinked the candidate.
Original build/source trees and the installed executable remained read-only.
This keeps the release's native inputs pinned while preserving other working
trees and concurrent Go/dashboard development.

Final candidate (earliest-sequence refinement included):

- patch SHA-256: `fb7acbf59953e176982f92f614345bb366c90b0f3e10a0fb9c859f68cd62fe2a`;
- executable SHA-256: `c27982a884deac20f48f3e341f30305fba6ee91dae09779b8082659475649651`;
- build result: `/opt/gramstat-full/secondary-release-build-result-v2.json`;
- completed **2026-11-11 13:10:36 UTC**: four native suites and all eight secondary
  regressions passed; the multi-CF case also checks several unreplayed records
  below the MANIFEST's global sequence;
- CLI help matches the original (both intentionally exit **2**). The first
  release-build attempt incorrectly treated that exit code as a failure; its
  result is retained in `secondary-release-build-result.json`.

A separate read-only NodeDB canary compares block/account/interface/delta
digests around the fourth crash and on recent blocks. Its result is recorded in
`/opt/gramstat-full/secondary-release-canary-result-v2.json`; installation and
resume verification are separate from the build gate. The first canary failed
on the unchanged baseline at DB open because its file-descriptor limit was
131072; the retry uses 1048576. This was not a missing-merge-base failure.
The second attempt was stopped by the host-PSI guard before producing a digest.
The rollout therefore performs one 32-MC read-only A/B comparison during a bounded
pause of our live reader, releasing its memory first. The old executable is
restored if the comparison or subsequent resume validation fails.

### Installed and verified

Release **`4e82a1686fb4335bdbef1bebb1a37ca34f6add0b`** was installed on
**2026-11-11 at 13:24:27 UTC**, only in `gramstat-mainnet-full.service`.
The pause took **116.024 seconds**, including the read-only A/B canary:

- **MC 104767782..104767813**, 32 masterchain blocks per arm;
- identical digests over **64 total blocks, 678 transactions, 500 account
  states, 220 interfaces**, block metrics and account deltas/deployments;
- digest `8f0a589841df33ba4920960de9cf33c6ef3d0646e905afcfd792383c65a64f85`;
- result: `/opt/gramstat-full/secondary-release-canary-result-maintenance.json`.

Live PID changed **2924146 → 1292050**, with the candidate executable SHA above.
Node PID **2062301** and ordinary-indexer PID **2763123** were unchanged across
the rollout. Runtime arguments, allocator settings, resource limits, 1-GiB LRU,
and database targets were preserved. Rollback backup:
`/opt/gramstat-full/upgrades/20261111T132231Z-secondary-memtable/`.
The independent eight-minute rescue timer was cancelled after verification.

At **13:25:40 UTC**, **401 MC blocks/deltas in 104767682..104768082** passed the
resume check, wall lag **3.52s**, zero cgroup memory-limit/OOM events.
`secondary-release-install-result.json` records the installation and checks.

The first startup watcher also recorded retryable `cell not found` fetches for
**MC 104768215 and 104768220** during catch-up. These were **not** the fatal
merge-base assertion. The follow-up check at **13:27:11 UTC** verified all
**818 blocks/deltas in 104767682..104768499**, including both affected MCs:
no gaps, missing balances or timestamp mismatches. Latest persisted MC
**104768549**, wall lag **3.68s**, PID **1292050**, **zero restarts/fatal lines**.
Evidence: `/opt/gramstat-full/secondary-release-poststartup-verification.json`.

The initial memory observer rejected a stale pre-pause log report before the
new process's first periodic report; this was an observer-startup issue. Both
observers were rearmed after the recovery check:

- live: `gramstat-full-live-health-1292050-secondary-steady.service`;
- host memory: `gramstat-full-memory-secondary-1292050-steady.service`
  (observe-only, 12-hour cap).

These checks prove successful rollout and recovery, not indefinite absence of
the original intermittent assertion. Continuous monitoring remains active.
