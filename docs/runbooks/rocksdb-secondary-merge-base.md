# Secondary CellDB merge-base failure (2026-11-10)

**Current source:** upstream RocksDB **v11.13.2**, via TON commit
`348c85c096f96831d89c00870b8097f5cec4ed21`, with the previous **fast-open patch
restored** for synchronous secondary opening. The CMake application helper is
restored too. WAL-reader retention and flushed-memtable retirement remain supplied
by upstream; their duplicate local hunks are not reapplied.

**Deployment:** the restored-patch ELF from indexer **`c1ef71d`** was installed
in Gramstat on **2026-11-12 at 11:25:55 UTC** and verified live. See the rollout
record below; the two earlier unsuccessful attempts are preserved separately.

**2026-11-13 outcome:** the same `merge_in.existing_value` assertion recurred on
the restored-patch ELF at **14:58:45 UTC**. The upgrade plus old fast-open patch
has **not eliminated the production failure**. Automatic recovery was verified;
the release remains installed and observation continues.

The follow-up [per-invariant code audit](rocksdb-secondary-upstream-audit.md)
maps every part of the removed patches to the pinned upstream implementation,
adds atomic-group and pre-existing-SST lifetime checks, and records a shared
same-name SST restoration limitation plus an old patched-reader failure.

## Restoring the old fast-open patch, 2026-11-12

The clean-upstream deployment repeated `CHECK(merge_in.existing_value)` at
**07:18:58.292485989 UTC** and **10:00:44.388226832 UTC**. Both stacks follow
the same CellDB point-read path. Systemd restarted Gramstat automatically;
the running ELF still had SHA-256
`4c3a923560d9e2c312ccea20ef37b18c84979bc3f4d7219ff33ca07095ad7478`.
The previously recorded successful tests did not establish that this production
failure was eliminated.

Per the operator's instruction, work now restores the previous patch rather
than investigating a new failure mechanism or introducing a new crash fix:

- `fix-secondary-open.patch` restores final-state MANIFEST recovery, missing-SST
  retries/PIT fallback, atomic-group/CF handoff, and the empty-baseline/rebase
  machinery from `cc2a67d`.
- The old WAL retention code and `CatchUpTailsCurrentWalWhenFutureWalExists`
  remain unchanged in upstream #14781; only duplicate patch-file hunks are absent.
- The later local `fix-secondary-flushed-memtable.patch` stays replaced by
  upstream #15066. Its implementation is not overwritten with the older raw-CF
  log-number rule.
- Rebase adaptations cover constructor/open signatures, new MANIFEST read-offset
  and installed-Version watermark bookkeeping, and the checked handoff status.
  Async opening and trust-MANIFEST remote compaction keep their upstream paths.

Local compatibility checks passed: **74 secondary tests** (including the three
restored fast-open tests), **10 memtable-list**, **29 version-builder**, **24
atomic/PIT** tests, and all **8 public regressions**. The Debug build enables
`ASSERT_STATUS_CHECKED`; the initial run exposed an unchecked successful handoff
status, corrected by returning that status to the caller. That failed run is
preserved. Patch application and a second, idempotent application were verified
against a fresh export of the pinned upstream files. These checks do not claim
that fast-open restoration fixes the production abort, nor remove the previously
recorded same-name SST restoration limitation.

Runtime evidence: local `gramstat-full/old-patch-rebase/`, including
`test-result.json`, `public-result.json`, `apply-result.json`, and the first
run's `test-result-v1-unchecked-handoff-status.json`.
Production incident/recovery evidence is in
`/opt/gramstat-full/incidents/20261112T101748Z-upstream-merge-repeat/`.
At **10:19:49 UTC**, bounded recovery checks covered **2483 MC** across
104923400..104924400, 104946800..104947800, and 104949217..104949697, with no gaps,
NULL `global_balance`, or timestamp mismatch. The 12-hour observe-only memory
watch ended at **04:47:43 UTC** without meeting admission criteria; it launched
no child job.

### Linux release and first rollout attempt

The Linux build of `c1ef71db864fc1d44f47b51b9e72efd561129155` passed the four
native suites and all eight public secondary regressions. All **2592 non-RocksDB
object files** remained byte-identical to the clean-upstream build. Original
server source/build/installed files were preserved by private build mounts.

- Candidate ELF SHA-256:
  `d7135c6a862663c35049887600779252044cc126b9d08eafea8a7060374a67e8`.
- Restored patch SHA-256:
  `a96b37bb08a10bfe0e1323f46a49852f02e9763ca29f361f581f38a29e679d18`.
- Evidence: `/opt/gramstat-full/rocksdb-fast-open-c1ef71d/build-result.json`.

The first rollout paused Gramstat at **11:02:24 UTC**, but its read-only A/B was
stopped at **11:04:45 UTC** while opening the **old clean-upstream** arm: host
available memory was **5.983 GiB** with full PSI avg10 **18.86%**, crossing the
existing combined `<6 GiB and >=10%` guard. Node and ordinary-indexer full PSI
were both zero. No candidate arm completed and the candidate ELF was not
installed. The controller resumed the old ELF as PID **2489216**; both protected
service PIDs remained unchanged. Original failure evidence is retained in
`install-result.json` and `canary-result.json` in the release directory.

Recovery verification at **11:11:05 UTC** checked **1331 MC,
104955753..104957083**, with no gaps, NULL balances or timestamp mismatch; lag
was **3.81s** and there were no restarts. A startup `cell not found` fetch for
MC **104956560** triggered the observer, then was confirmed persisted inside
that range. The health watcher was rearmed.

The second attempt (`-v2`) timed out in preflight at **11:23:01 UTC** without
pausing Gramstat: its 120 samples all had **zero host PSI**, but only
**3.78..4.37 GiB** available, so the requested 60s with >=6 GiB never began.
This was an overly restrictive pre-stop admission rule: the running Gramstat
itself occupied about **7.2 GiB**. The next attempt (`-v3`) checks headroom again
after stopping only Gramstat and runs one bounded **candidate-only smoke**.
The existing canary resource guards and automatic rollback remain in force.
It does not claim completed same-range old/new A/B equivalence; the full A/B
attempt remains recorded as failed.

### Restored fast-open deployed and observed

The candidate-only smoke passed **64 MC, 104958895..104958958**, reading
**131 blocks, 1501 transactions, 1148 accounts, 402 interfaces**. Its digest was
`b4c41cd086bf3fbff88af8d5bd375d846c0dd27b149d2219178cdb16cd857cdc`;
whole-process time **58.83s**, peak RSS **3012512 KiB**. This is a successful
bounded read, not an A/B equality result or a cold-cache performance comparison.

Only **`gramstat-mainnet-full.service`** was upgraded. PID changed
**2489216 -> 3365444**; stop through smoke and restart took **60.93s**. The
candidate started at **11:25:55 UTC**, and the live-data gate passed at
**11:27:26 UTC**. ELF SHA-256 is
`d7135c6a862663c35049887600779252044cc126b9d08eafea8a7060374a67e8`.

The install gate verified **444 MC**; follow-up at **11:27:39 UTC** verified
**479 MC, 104958995..104959473**, spanning the pause and restart. There were no
gaps, NULL `global_balance`, timestamp mismatches, retryable fetches or fatal
lines. Latest persisted MC was **104959493**, lag **4.79s**, restart count **0**.
The cgroup's high/max/OOM/oom_kill counters were zero. Node PID **2062301** and
ordinary-indexer PID **2850699**, their restart counts, Gramstat's arguments,
launcher, unit and allocator settings remained unchanged; `MALLOC_ARENA_MAX`
remains absent.

Release evidence in `/opt/gramstat-full/rocksdb-fast-open-c1ef71d/`:

- `install-result-v3.json`, `rollout-state-v3.json`, `canary-result-v3.json`;
- `poststartup-verification-v3.json`;
- rollback backup:
  `/opt/gramstat-full/upgrades/20261112T112453Z-fast-open-c1ef71d/`.

The independent ten-minute rollback timer was cancelled after verification.
Three server-side observers were started with reconnecting notification waiters:

- `gramstat-full-live-health-3365444-fast-open-c1ef71d.service`: log errors,
  fatal lines and memory-pressure events;
- `gramstat-full-status-3365444-fast-open.service`: PID/restart/lag/progress
  every 30s and bounded persisted-data coverage every 10 minutes;
- `gramstat-full-resources-3365444-fast-open-6h.service`: six-hour resource
  sample series at 30-second intervals, with a completion result.

Their unit/result/waiter IDs are recorded in
`/opt/gramstat-full/health-watch-current.json`; release summary is
`/opt/gramstat-full/fast-open-release.json`. This establishes rollout and bounded
data verification, not that the intermittent merge-base abort has been cured.

### Same assertion recurred with the restored patch, 2026-11-13

The first 12 hours had no fatal events; bounded checks covered 104708 consecutive
MC. At **06:03:47 UTC**, both indexers experienced ClickHouse connection errors,
followed by a separate `TraceAssembler.cpp:133` fatal on an already passed seqno.
Systemd restarted Gramstat as PID **2617286** at **06:04:20 UTC**. Its 601-MC
recovery window included all affected seqnos, with no gaps, NULL balances or
timestamp mismatch. Evidence:
`/opt/gramstat-full/incidents/20261113T060347Z-clickhouse-insert-trace-restart/`.
The unchanged ordinary indexer subsequently repeated the CellDB assertion at
**08:50:24** and **10:25:05 UTC**.

At **14:58:45.104803822 UTC**, Gramstat itself repeated
`celldb.cpp:94: Check merge_in.existing_value failed` in `!indexquery`. Its
installed and recovered running ELF hashes still match
`d7135c6a862663c35049887600779252044cc126b9d08eafea8a7060374a67e8`:
this occurred on **11.13.2 plus the restored fast-open patch**, not a reverted
binary. Cgroup high/max/OOM/oom_kill counters were zero; both service and host
full PSI avg10 were zero at the event.

Systemd restarted it at **14:59:18 UTC**, PID **2617286 -> 643229**, restart
count **2** including the earlier ClickHouse/TraceAssembler event. Ordinary
indexer PID **178930** and node PID **1824966** stayed unchanged in this incident.
At **15:01:20 UTC**, the recovered reader had persisted MC **105199926**, lag
**5.01s**. The **607-MC** window **105199300..105199906** had no gaps, NULL balances
or timestamp mismatch; startup fetch retries for **105199738, 105199742,
105199743** were inside the verified window. No new fatal occurred after restart.

Evidence:
`/opt/gramstat-full/incidents/20261113T145845Z-fast-open-merge-repeat/`
contains the complete captured crash/restart log segment, service journals,
observer events, binary manifests, `capture.json` and `recovery-verification.json`.
The three Gramstat observers were rearmed for PID **643229**; the read-only
ordinary-indexer journal observer remains active for PID **178930**. Current
unit/result/notification IDs are in `health-watch-current.json`.

This recurrence disproves an operational claim that the retained patch bundle
fully fixes the crash. It does not identify the exact merge key or underlying
failure mechanism. No new production patch or rollback was made for this event.

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

## Upstream provenance review (after rollout)

The shipped patch is maintained in **ton-indexer**, alongside the existing
`ton-index-worker/patches/fix-secondary-open.patch`; CMake applies both inside
the nested RocksDB source tree. Neither the TON gitlink (`31f1bdf1`) nor the
RocksDB gitlink (`4595a5e9`, version 10.10.1) was advanced. TON's secondary wrapper
lives in the TON fork, but the existing RocksDB patch delivery mechanism is in
the indexer repository.

A subsequent upstream check found that this defect was already reported in
[RocksDB #15051](https://github.com/meta/rocksdb/issues/15051) and addressed by
[commit b616b203 / PR #15066](https://github.com/meta/rocksdb/commit/b616b203b8aab0c6f122e157f954e984d2bbc90c)
on **2026-08-17**. The pinned revision predates it. It affects generic secondary
reads, including ordinary Put/Get without TON or merge operators; TON's
reference-count merge turns the stale-memtable case into a missing-base assertion.
The primary writer is not the defective component in this reproducer.

**The deployed minimal patch is not equivalent to the complete upstream fix.**
Upstream gates retirement on the log boundary of the **installed readable
Version**, not simply `cfd->GetLogNumber()`: the latter can advance while SST
opening fails or an atomic group remains incomplete. It also reconciles column
families after failed catch-up rounds and tracks partially replayed WAL batches
conservatively. Those cases were not covered by our eight original regressions.
The upstream implementation should be reviewed for a compatible backport rather
than treating the local minimal change as the final general-purpose solution.

RocksDB's [secondary-instance documentation](https://github.com/meta/rocksdb/wiki/Read-only-and-Secondary-instances)
explicitly supports this read-only catch-up pattern. Applications using only a
primary instance do not exercise the affected path; secondary readers on an
affected version may need the upstream fix even outside the TON ecosystem.

### Isolation from TON and the previous patches

The pinned **unmodified** RocksDB `4595a5e9` was built separately on macOS arm64
and tested using only its public API, without TON/indexer libraries. Result:
`upstream-secondary-result.json`, completed **2026-11-11 14:39:24 UTC** in the
runtime evidence directory. The baseline reproduced all six stale-memtable
regressions, including **3 instead of 2** and **Corruption instead of NotFound**.
Adding only our new patch corrected those six. The separate future-empty-WAL
regression failed in both arms because neither contained the older #14781
backport. Thus the suite counts are **7 failures → 1 failure**, not a claim that
the new patch alone addresses every secondary defect.

The clean build needed compiler flags to force-include `sys/types.h` and
`cstdlib` with Clang 23; the test executable was built without RTTI to match
the standalone library. These are build-compatibility adjustments, not source
changes to the baseline. The initial missing-declaration/link failures remain
in the build log.

### Status of `fix-secondary-open.patch` in upstream

Checked against upstream `main` **`4ff3dd282a7e54be7b7183cc48c9793398ea748e`**:

| Part of our existing bundle | Upstream status |
| --- | --- |
| Fast initial recovery through a regular `VersionEditHandler`, then handoff to `ManifestTailer` | [PR #14815](https://github.com/meta/rocksdb/pull/14815), authored by `dungeon-master-666`, is still open; this exact path is not in checked main. |
| Underlying slow/quadratic point-in-time MANIFEST recovery | Fixed by a different approach: [commit c9536aae / #14929](https://github.com/meta/rocksdb/commit/c9536aaef7815467f406ddf94cb2feeb772068f9), **2026-07-15**. It replaces whole-VersionBuilder copies per edit with single-edit undo, retaining PIT recovery semantics. |
| Preserving an older growing WAL when a future empty WAL appears | [Commit ae30c71c / #14781](https://github.com/meta/rocksdb/commit/ae30c71c6b4490fe74cc102cd8e868bd07f2ae0c), **2026-05-26**; already backported into our bundle. |
| Atomic-group transfer, rebasing builders to release old SSTs, retry/fallback around our accelerated open | Accompany the local fast-open/handoff design. Checked main does not contain these helpers; without that local design they should not be blindly carried over as independent upstream bug fixes. |

Upstream also has asynchronous secondary file opening (#15294), but it changes
when SSTs are pinned and requires additional deletion coordination. It is not a
drop-in replacement for our synchronous max-open-files=-1 setup.

An upstream upgrade containing **#14929, #14781 and #15066** could allow removing
the local secondary patch bundle. That requires validating startup time on the
real MANIFEST, SST lifetime and catch-up correctness; absence of our helper names
alone is not a performance equivalence proof. No additional production upgrade
was made as part of this provenance investigation.

## Clean upstream v11.13.2 evaluation

The tested upstream revision is **`17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`**
([v11.13.2](https://github.com/meta/rocksdb/releases/tag/v11.13.2), published
2026-11-04). Its ancestry contains #14781, #14929 and #15066.

The TON branch is
[`1IxI1/ton:gramstat-rocksdb-11.13.2`](https://github.com/1IxI1/ton/tree/gramstat-rocksdb-11.13.2),
HEAD **`348c85c096f96831d89c00870b8097f5cec4ed21`**. It starts directly from
**`dungeon-master-666/ton:ton-indexer` at `31f1bdf1f5d3eb027cc423559203b496036027ad`**,
not from a different fork's master. The two commits pin upstream RocksDB and
adapt its removed `DB**` open overloads to `std::unique_ptr<DB>*` in the three
database wrappers and optional `bench-state-gen`. Ownership transfers to the
existing shared reader after successful open/catch-up. Transaction-DB's separate
raw-pointer API is unchanged. No RocksDB source modifications are applied.

Completed checks on **2026-11-11**:

| Check | Result |
| --- | --- |
| Upstream Debug/assertions secondary suites, macOS arm64 | **71 passed**, two upstream-disabled tests |
| Upstream Debug/assertions memtable-list suite | **10 passed** |
| Our eight public-API regressions against clean Debug upstream | **8 passed** |
| Linux Release build | `ton-index-clickhouse` and `ton-index-postgres` built |
| Linux native suites | **4 passed**: Gramstat, elector rounds, caches, state-update reader |
| Our eight public-API regressions against clean Linux upstream | **8 passed** |
| Optional `bench-state-gen` with upstream headers | Compile-only compatibility passed |

The initial builds correctly exposed the removed raw-pointer overloads; those
failed attempt logs are retained. The subsequent checks use the API adaptations.
The Linux build runs in a private namespace over a copied build tree, with
read-only source overlays and NodeDB inaccessible. All **2588 unaffected
non-RocksDB objects** remain byte-identical; the three tddb wrappers and the
RocksDB-dependent `validator/db/celldb.cpp` were rebuilt. Original source/build
files and the deployed executable retain their original SHA-256 and mtime.

Candidate ClickHouse ELF SHA-256:
`4c3a923560d9e2c312ccea20ef37b18c84979bc3f4d7219ff33ca07095ad7478`.
This build result alone does not authorize a claim of real-NodeDB parity or
production deployment. The read-only A/B and its bounded Gramstat-only pause
have separate result records.

Evidence under `/opt/gramstat-full/`:

- `rocksdb-upstream-build-result-v2.json`;
- `rocksdb-upstream-local-tests-result-v2.json`;
- `rocksdb-upstream-11.13.2/benchmark-compatibility-result.json`;
- `rocksdb-upstream-canary-result.json` and
  `rocksdb-upstream-canary-maintenance-result.json` (A/B and resume separately).

### Real NodeDB evaluation: incomplete because of host pressure

The deployed-reader baseline successfully read **MC 104786915..104786978**:
**133 blocks, 1963 transactions, 1360 account states, 536 interfaces**; digest
`ca58eb3707a810f84bd1d46954177a5a7bc1bd708ebb1a492b0a71ac764d3c3a`.
Its whole-process time was **59.784s**, including **0.614s** parsing, with peak
RSS **2873564 KiB**. The remaining time includes DB open and teardown; it is not
an independently instrumented open-only duration.

Three upstream attempts were interrupted by host-memory-pressure guards before
producing an `UPDATE_RESULT`. Their partial work does **not** establish either
data parity or an upstream startup-time regression:

| Attempt | Probe cgroup cap | Reason for interruption |
| --- | --- | --- |
| Initial A/B | 4 GiB | Host full PSI avg10 **5.89%**, available **9.29 GiB** |
| v2, upstream-only against the saved baseline | 6 GiB | Host full PSI **5.53%**, available **8.97 GiB**; cgroup `memory.events.max=2233`, no OOM; RSS still below **1.5 GiB**, most charged memory was file cache |
| v3, larger cache budget and separate 4-GiB RSS / 4-GiB host-headroom guards | 12 GiB | Host full PSI **11.78%**, available **8.64 GiB**; **zero** cgroup max/OOM events; sampled peak RSS **1.877 GiB**, cgroup **9.560 GiB**, file cache **8.236 GiB** |

The larger cgroup removed the probe's own limit events but did not prevent host
pressure. Thus the small probe cap was not a complete explanation, and these
results do not demonstrate excessive upstream heap/RSS either. A completed
real-NodeDB comparison under adequate host conditions remains the release gate.
The three runs' logs, resource samples and failed result JSONs are retained;
`rocksdb-upstream-resource-analysis.json` summarizes v3. The retry filenames carry
`-v2` / `-v3`; they reuse the successful baseline's exact range and digest, not a
claim of a controlled cold-cache timing benchmark.

Each run paused only our Gramstat service and independently guaranteed resumption
of the **same** deployed ELF. Pauses were **122.541s**, **32.304s** and **56.691s**.
Node PID **2062301** and ordinary-indexer PID **2763123** remained unchanged.
Runtime arguments, launcher, unit and binary manifest retained their hashes.

Post-resume persistence checks passed:

- initial: **993 MC**, 104787015..104788007, including the retryable startup fetch
  at 104787468;
- v2: **1301 MC**, 104787967..104789267, including retryable fetches at 104788197
  and 104788202;
- v3: **492 MC**, 104789368..104789859, with no startup fetch failures, missing
  balances or timestamp mismatches. At **15:56:31 UTC**, PID **3551820**, zero
  restarts/fatal lines, latest persisted MC **104789879**, wall lag **5.49s**.

Current observers are
`gramstat-full-live-health-3551820-after-upstream-canary-v3.service` and
`gramstat-full-memory-after-upstream-3551820.service` (read-only host recovery
watch). They survive SSH loss and have completion/event notification waiters.

**Decision at 15:56 UTC (superseded by the completed v4 check below):** retain both patches and the old TON gitlink in the main indexer
checkout and deployed reader for now. The published TON candidate branch and
compiled upstream binaries are ready for the remaining canary; the upstream
upgrade has **not** been deployed. The regression source now uses the owning
open API accepted by both RocksDB 10.10.1 and 11.13.2. No writer/NFT or historical
metrics changes are included in this evaluation.

### Completed real-NodeDB A/B, 2026-11-11 16:32 UTC

Further read-only host inspection identified **ZFS 2.1.5** on the NodeDB dataset,
with an approximately **61–63 GiB ARC**. This accounts for much of the memory
outside process RSS and Linux's ordinary page cache. Earlier global-PSI stops
were not sufficient evidence that the protected services were memory-stalled.
No ZFS, node, ordinary-indexer or global kernel settings were changed.

The v4 canary retained a **12-GiB cgroup cap, 4-GiB RSS limit, 4-GiB minimum host
headroom and 180-second per-arm timeout**. It additionally stopped for protected
service full PSI >=5%, or the combined condition of host full PSI >=10% with
available memory <6 GiB. It recorded ARC, reclaim counters and each service's
PSI throughout. Both arms used their original binaries/options, on the same new
range, without flushing caches.

**MC 104794490..104794553 passed completely:** identical digest
`f3045f41b6507c797ce35aabe8b1e45df7335eb3b54234d390c35ebbc9286cce`
over **133 blocks, 1971 transactions, 1458 account states, 543 interfaces**,
block metrics and account deltas/deployments.

| Measurement | Deployed 10.10.1 + patches | Clean 11.13.2 |
| --- | ---: | ---: |
| Whole-process time | 55.260s | 75.066s |
| Parsing measured range | 0.655s | 0.698s |
| Open + teardown (difference, not open-only instrumentation) | 54.605s | 74.368s |
| Peak RSS | 2891968 KiB | 2944372 KiB |
| Sampled peak cgroup memory, including file cache | 4.019 GiB | 9.491 GiB |
| Minimum available host memory | 7.292 GiB | 7.560 GiB |
| Maximum host full PSI avg10 | 4.27% | 13.46% |
| Maximum probe / node / ordinary-indexer full PSI avg10 | 0% / 0% / 0% | 0% / 0% / 0% |

Upstream's whole-process time was **1.358×** baseline in this single sequential
measurement. It is slower by about 20 seconds, with similar RSS; no multi-minute
startup regression was observed. This is a live-node comparison, not a
controlled cold-cache benchmark. Resource evidence is in
`rocksdb-upstream-resource-analysis-v4.json` and
`rocksdb-upstream-11.13.2/canary-resources-v4.jsonl`.

The probe succeeded. The surrounding controller then received systemctl exit
**5** when stopping the already-completed, garbage-collected transient probe
unit. Its exception path resumed the old reader immediately. The initial
controller failure remains recorded, and the cleanup now accepts an absent unit
only after checking that it is inactive with PID 0. Follow-up verification is
separate, in `rocksdb-upstream-canary-maintenance-result-v4-recovered.json` and
`rocksdb-upstream-canary-resume-verification-v4.json`: **1211 MC in
104794590..104795800**, including retryable fetches 104795068/104795069, no gaps,
NULL balances, timestamp mismatches or fatal lines; PID **955158**, zero
restarts, lag **4.37s** at **16:37:50 UTC**. Node and ordinary-indexer PIDs stayed
2062301 and 2763123.

**Source migration:** the indexer now pins the tested TON commit from
`1IxI1/ton:gramstat-rocksdb-11.13.2`, removes both secondary patch targets/files
and the unused `ApplyPatch.cmake`, and keeps the public regressions. The committed
worker CMake file is byte-identical to the tested no-patch overlay; the TON
wrappers match the compiled overlays, and the nested RocksDB tree is clean.
The six previously patched nested files were first verified byte-for-byte against
the two known patches, then preserved in a named Git stash and a runtime diff
backup before switching versions. This source change is distinct from installing
the candidate production executable.

### Upstream installed and verified

Indexer source **`fed5c1e3e9ba11056ca35a289ed740d1f3871542`** was published and its
verified ELF installed in **`gramstat-mainnet-full.service` only** on
**2026-11-11 at 16:43:56 UTC**. PID changed **955158 → 1412059**. The stop/replace/
start interval was **1.603s**; startup and catch-up continued afterwards, and the
live-data gate passed at **16:45:44 UTC**. This is not a claim that indexing was
uninterrupted during DB open.

- ELF SHA-256: `4c3a923560d9e2c312ccea20ef37b18c84979bc3f4d7219ff33ca07095ad7478`;
- TON: `348c85c096f96831d89c00870b8097f5cec4ed21`, based on the requested
  `dungeon-master-666/ton:ton-indexer` revision;
- RocksDB: upstream `17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`, **no local patches**;
- rollback backup: `/opt/gramstat-full/upgrades/20261111T164354Z-rocksdb-upstream-11132/`;
- result: `/opt/gramstat-full/rocksdb-upstream-install-result.json`;
- independent eight-minute rollback timer cancelled after successful verification.

The install gate verified **343 consecutive MC blocks/deltas**. Follow-up at
**16:47:04 UTC** verified **542 MC in 104796594..104797135**, including the
retryable startup fetch for **104796955**: no missing rows, NULL balances,
timestamp mismatches or fatal lines. Latest persisted MC **104797155**, lag
**4.64s**, zero restarts. Evidence:
`rocksdb-upstream-poststartup-verification.json`. The earlier startup watcher
event was resolved by that persistence check, then the steady watcher was armed.

Runtime arguments, database targets, launcher, service resource limits and
allocator settings were preserved (`MALLOC_ARENA_MAX` remains absent).
Cgroup high/max/OOM counters were all zero. Node PID **2062301** and
ordinary-indexer PID **2763123** stayed unchanged. Monitoring:

- `gramstat-full-live-health-1412059-rocksdb-upstream-steady.service`;
- `gramstat-full-memory-rocksdb-upstream-1412059.service` (observe-only).

Both have durable result files and reconnecting event/completion notification
waiters. Successful rollout and bounded recovery checks do not establish
indefinite absence of intermittent failures; live observation continues.
