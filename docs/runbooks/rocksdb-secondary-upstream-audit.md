# Audit of the removed secondary patches against RocksDB 11.13.2

**Audited revision:** `17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`, not moving main.
**Date:** 2026-11-11. This supplements the [rollout record](rocksdb-secondary-merge-base.md).

**2026-11-12 update:** the clean-upstream deployment repeated the CellDB
merge-base abort twice. The old fast-open patch is now restored and deployed,
rebased for 11.13.2, at the operator's request. WAL retention and memtable
retirement remain upstream implementations. The conclusions below describe the
earlier removal audit, not the current patch policy or proof that the production
failure was eliminated. See the [restoration record](rocksdb-secondary-merge-base.md#restoring-the-old-fast-open-patch-2026-11-12)
for scope and compatibility checks.

The pre-rollout review established upstream fix ancestry, exercised the secondary
and memtable suites, ran eight public regressions, and compared real NodeDB reads.
It did **not** yet trace every fast-open supporting change to its replacement
invariant. This follow-up closes that review gap. Uptime is not its evidence.

The removed patches are available at indexer revision `cc2a67d`:
`ton-index-worker/patches/fix-secondary-open.patch` and
`ton-index-worker/patches/fix-secondary-flushed-memtable.patch`.

## Scope and conclusion

The patch changes reduce to the seven obligations below. For the deployed
**synchronous secondary** configuration, the audited code and focused checks
support removing both patches. This is **not** a proof that RocksDB has no other
bugs, nor that all fault-path return values/timing are identical.

The TON wrapper leaves `max_open_files=-1` and `open_files_async=false` at their
upstream defaults (`include/rocksdb/options.h:792,829`). It invokes
`TryCatchUpWithPrimary()` immediately after `OpenAsSecondary()` and propagates
its status before returning (`tddb/td/db/RocksDbSecondary.cpp:113-120` in TON
`348c85c0`). These details matter for file pinning and the open-race result.

An additional restored-file experiment found a **shared limitation**, described
below, plus a failure in the old patched reader that the new reader avoids.
Those findings are retained, rather than turning all audit results green.

## 1. Avoid the old quadratic MANIFEST recovery mechanism

**Local mechanism:** `FastSecondaryVersionEditHandler` bypassed point-in-time
(PIT) recovery for the initial open and constructed the final version directly.

**Upstream:** #14929 / `c9536aae` removes the per-edit whole-builder copy while
keeping PIT recovery. In the audited tag:

- [`VersionBuilder::Rep::EditUndo`, lines 350-461](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/version_builder.cc#L350-L461)
  records only slots touched by one edit; copying `Rep` is explicitly deleted.
- `CaptureUndo`, lines 598-657, visits the edit's additions/deletions/cursors.
- `RollbackLastApply` / `RedoLastApply` / `CommitLastApply`, lines 725-752,
  preserve the pre-edit state without a whole-tree copy.
- [`MaybeCreateVersionBeforeApplyEdit`, lines 871-980](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/version_edit_handler.cc#L871-L980)
  rolls back only on a valid-to-invalid transition, creates the readable prior
  version, redoes the edit, and commits its undo record.

**Evidence:** 29 version-builder tests and 24 selected atomic/PIT/version-set
tests passed in this audit. The earlier real NodeDB comparison completed in
75.066s versus 55.260s for the patched reader (whole process, including teardown).
The identified deep-copy mechanism is gone; this does not claim every possible
MANIFEST workload is linear or that startup is equally fast.

## 2. Preserve an incomplete atomic group from open into catch-up

**Local mechanism:** `CopyReadBufferFrom` inside `PrepareForCatchUpAfterRecover`
copied a partially read group from the fast initial handler into a new tailer.

**Upstream:** [`ReactiveVersionSet::Recover` / `ReadAndApply`, lines 8785-8845](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/version_set.cc#L8785-L8845)
create **one** `ManifestTailer` and keep using it. There is no handler handoff.
[`VersionEditHandlerBase::Iterate`, lines 24-100](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/version_edit_handler.cc#L24-L100)
does not clear `read_buffer_` at EOF: it replays only a complete group and clears
it after successful group replay. The tailer changes mode in
`CheckIterationResult:1174-1191` without replacing that buffer.

Resetting it in `PrepareToReadNewManifest` is a different boundary: CURRENT has
changed to a new MANIFEST, so the old file's partial group must not be spliced
into the new file. `MaybeSwitchManifest:8899-8905` controls that transition.

**Evidence:** the newly executed
`HandleIncompleteTrailingAtomicGroupWithReactiveVersionSetRecover` writes three
of four edits, opens the secondary version set, appends the fourth edit, then
asserts the whole group was replayed and the buffer cleared. Related valid,
partial, malformed and manifest-switch cases also passed (24-test group).
This exact recovery-to-catch-up case was outside the earlier 71-test secondary
suite, so the earlier suite count alone did not cover this obligation.

## 3. Preserve the column-family selection state

**Local mechanism:** `CopyDoNotOpenColumnFamiliesFrom` preserved CF IDs omitted
by the caller while replacing the initial handler; fast recovery also needed
`MustOpenAllColumnFamilies=false`.

**Upstream:** the same handler owns `do_not_open_column_families_` throughout.
`OnColumnFamilyAdd:243-278` records omitted CFs, `OnNonCfOperation:313-348`
skips their edits, and `OnColumnFamilyDrop:281-299` removes their IDs.
`ManifestTailer::MustOpenAllColumnFamilies` returns false
(`version_edit_handler.h:452`). No copy is needed because ownership never moves.

**Evidence:** the previously passed `OpenWithSubsetOfColumnFamilies` opens only
default while another CF already exists, flushes both, and catches up correctly.
`PrimaryDropColumnFamily`, `SwitchManifest` and `SwitchManifestTwice` also passed.
This is not a claim of support for automatically opening newly created CFs;
the upstream tailer explicitly ignores CFs absent from its open configuration.

## 4. Release obsolete SSTs, including files present at initial open

**Local mechanism:** the fast handler installed a populated current Version.
Bootstrapping the tailer from that Version pinned its SSTs forever, so the patch
added `RebaseBuildersToCurrentVersions`, a separately retained empty MANIFEST
baseline, and matching destructor/CF-drop `Unref` operations.

**Upstream ownership chain:**

1. `VersionSet::CreateColumnFamily:8637-8667` creates an **empty** initial Version.
2. `VersionEditHandler::CreateCfAndInit:499-511` constructs its builder before
   replaying SST additions. `BaseReferencedVersionBuilder:2086-2098` references
   that empty Version; `SaveTo` builds separate Versions, not a mutated base.
3. `ApplyFileDeletion:1198-1207` removes the added file's ownership. Its ref is
   temporarily held by undo, then released by `CommitLastApply:740-752`.
   `UnrefFile:480-505` releases the pinned table reader and evicts its cache entry
   when the last metadata reference goes away.
4. MANIFEST rotation reconstructs from `dummy_versions()->Next()` while the old
   builder still pins the empty initial Version (`ManifestTailer::Initialize:
   1099-1125`, `OnColumnFamilyAdd:1140-1171`). It does not choose a populated
   current Version. Builder destruction unrefs its base (`version_builder.cc:
   2113-2115`).
5. Installed Versions/readers retain their own references. Secondary read-view
   reclamation and `job_context.Clean()` run before obsolete-file processing
   (`db_impl_secondary.cc:1131-1145`), so an active reader keeps its valid files.

**New deterministic tests, all passed:**

- `ReleasesFilesPresentAtOpen`: two SSTs exist before secondary open; compaction
  plus catch-up closes exactly those two files.
- `ReleasesInitialFilesAfterManifestSwitch`: the same after primary reopen and
  MANIFEST rotation.
- `RetainsInitialFilesOnlyWhileIteratorPinsThem`: an old iterator can still read
  all four keys after compaction; files close after its release and catch-up.

These strengthen the earlier `SecondaryCloseFiles` test, which creates its SSTs
**after** opening the secondary. Helper-name absence was not sufficient evidence.

## 5. A referenced SST disappears while secondary open is loading tables

**Local mechanism:** convert missing SST to `TryAgain`, retain the fast builder
while replaying more MANIFEST edits, then recreate the DB in PIT mode if retries
cannot produce a complete end version. `ResetStatusForRetry` and
`MaybeHandleLoadTablesStatus` belong to that fast-path policy.

**Upstream:** PIT is used from the start. `VersionBuilder::ApplyFileAddition:
1310-1334` classifies missing/corrupt files; `ValidVersionAvailable:1829-1839`
requires completeness for secondary mode. The candidate load in
`MaybeCreateVersionBeforeApplyEdit:927-949` rejects missing/corrupt candidates
without latching that transient file error as the tailer's permanent status.
`CheckIterationResult:818-849` installs only a built readable Version. The
ordinary synchronous API passes `trust_manifest_recovery=false`
(`db_impl_secondary.cc:1169-1191`); no unverified async-open mode was enabled.

**Important behavioral difference:** upstream may return an older complete PIT
instead of internally retrying to the newest end version. The TON wrapper's
immediate catch-up remains necessary; equal retry counts/initial freshness were
not established and are not claimed.

**New passed test:** `CompactionDuringFinalTableOpenThenCatchUp` injects primary
compaction exactly at secondary's first physical SST open **after MANIFEST EOF**.
It proves the old SST really became missing, observes the older initial view,
then checks all four keys after the wrapper-equivalent catch-up. Unique-ID
preverification is disabled in this synthetic test to force this narrow window.
The previously passed missing/corrupt-file secondary tests cover the ordinary
configuration too.

## 6. Keep tailing the current WAL when a higher empty WAL exists

This is the actual upstream backport #14781 / `ae30c71c`, not a fast-open helper.
[`FindNewLogNumbers:330-360`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl_secondary.cc#L330-L360)
drops readers only below MANIFEST's `min_log_number_to_keep`, retains the lowest
still-needed reader and sorts the discovered logs. `RecoverLogFiles` no longer
keeps only the highest-number reader at the end of replay.

**Evidence:** `CatchUpTailsCurrentWalWhenFutureWalExists` and our
`future-empty-wal` case passed before rollout. The two upstream-disabled
`SwitchWAL*` tests were also explicitly run once in this audit and passed; their
disabled status was not changed, and a single pass does not resolve their
historical reason for being disabled.

## 7. Retire flushed active memtables without losing a readable merge base

Our minimal patch retired a nonempty active table using raw CF MANIFEST log
number, only for changed CFs, before WAL replay. Upstream #15066 / `b616b203`
and the audited tag's follow-up code have stronger conditions:

- `GetInstalledVersionLogNumber` records the log boundary **with the Version
  actually appended**, not merely a parsed flush edit
  (`version_edit_handler.cc:818-849,918-922,1034-1040`). Atomic-group Versions
  remain buffered until the group is complete (`1079-1097`).
- `TryCatchUpWithPrimary:1064-1128` reconciles every live initialized CF on a
  successful round, so a prior failed WAL round cannot strand an old memtable
  just because no new MANIFEST edits arrive.
- `RecordCurrentLog:589-602` tracks the maximum WAL by stable CF ID. Replay
  records affected CFs before insertion and selected CFs after insertion, also
  on partial/2PC paths (`470-539`), preventing an understated retention boundary.
- `MaybeSealFullyFlushedActiveMemtable:604-653` requires recorded WAL **strictly
  below** installed boundary. `SealActiveMemtable:573-587` uses normal immutable
  retirement and fragmented tombstones. `RemoveOldMemTables:1133-1158` uses that
  same installed boundary; old SuperVersions keep pinned readers safe.
- Upstream initializes the replacement's sequence with `LastSequence()` rather
  than our `kMaxSequenceNumber`, but `MemTable::Add:1252-1263` lowers the earliest
  sequence on insertion. Thus a different CF advancing MANIFEST sequence does
  not hide an older unreplayed batch.

**Evidence already passed:** all eight public regressions (merge/delete,
compaction, old iterator, next-WAL data, per-CF and sequence boundaries, empty
secondary), plus upstream `StaleActiveMemtableDoesNotShadowFlushedData`,
`ReconcilesMemtableAfterFailedRound`, `KeepsMemtableWhenFlushedFileIsMissing`,
`KeepsMemtableWhenAtomicFlushSiblingFileIsMissing`,
`KeepsMemtableWhenFlushedFileIsCorrupt`,
`KeepsImmutableMemtableWhenFlushedFileIsMissing`,
`KeepsMemtableAfterPartialReplayFailure`, and the 2PC-specific cases.

## Additional finding: putting the same missing SST back is insufficient

The fifth new local test tried more than the original missing-SST fallback test:
hide a live SST during open, perform catch-up while it is absent, restore it at
the same filename **without any MANIFEST edit**, and require another catch-up to
read all keys. That assertion **failed**; its failure/XML have been preserved.

A public-API reproducer was then compiled separately against the exact Linux
libraries for old **10.10.1 + both patches** and new **11.13.2**, using each
version's own headers:

| Recovery after same-name restoration | Old patched reader | Clean upstream |
| --- | --- | --- |
| Three catch-ups, no MANIFEST change | Still the older PIT | Still the older PIT |
| Close/reopen secondary | All four keys readable | All four keys readable |
| Primary compaction writes replacement SST/MANIFEST, then catch-up | `Corruption: Cannot delete table file #11 ... not in the LSM tree` | All four keys readable |

The missing-file bookkeeping is not revalidated merely because a pathname
reappeared. This is **not a regression introduced by the upgrade**, and it was
not solved by our old patch. The two versions may select different older PITs;
this experiment does not establish identical initial freshness.

The old reader's additional compaction failure is consistent with its unconditional
`RebaseBuildersToCurrentVersions` after catch-up: rebasing onto the installed
older PIT discards metadata for the still-missing file, so its later MANIFEST
deletion cannot find it. Upstream retains the MANIFEST builder's state and
successfully processes the replacement edit. This concrete A/B strengthens the
case for removing the local handoff/rebase machinery; it is not a reason to put
it back. Reopening after administrative same-name SST restoration remains an
explicit operational boundary.

## Evidence and limits

Runtime evidence under `/opt/gramstat-full/` and the local Gramstat runtime folder:

- `secondary-patch-audit-result.json`: **24 atomic/PIT + 29 version-builder +
  4 targeted lifetime/open-race checks passed**; the additional same-name
  restoration assumption failed. Two disabled WAL tests passed when forced.
- `secondary-patch-audit-test.cc`, `run-secondary-patch-audit.py`, and
  `secondary-patch-audit/{build.log,*.log,*.xml}`: the local fixture overlay and
  original outputs, including the failed assertion. The library SHA stayed
  `f392338a685debb5453771e8a68b70833856b453b10a23b2e2bd790e18384477` and upstream
  source remained clean.
- `secondary-restore-probe.cpp`, `compare-secondary-restore-v2.py`,
  `secondary-restore-comparison-result-v2.json`: the four Linux comparison arms.
  `success=true` means the diagnostic completed; its separate
  `all_recovery_paths_succeeded=false` preserves the old-reader compaction error.
- The initial comparison stopped at that old-reader error; its original result
  remains `secondary-restore-comparison-result.json`.

The Linux comparison had NodeDB inaccessible and original source/build/production
binary read-only. No production code, configuration or process was changed by
this audit. These are code-path arguments and deterministic fault/lifetime tests,
not an all-RocksDB test-matrix run or a formal correctness proof. The running
service monitor is an additional operational check, not a substitute for them.
