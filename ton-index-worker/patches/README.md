# RocksDB secondary patches

`fix-secondary-open.patch` restores the existing fast-open patch from indexer
`cc2a67d`, rebased onto RocksDB **11.13.2** (`17b624b5`). It retains final-state
MANIFEST recovery, missing-SST retries and PIT fallback, the atomic/CF handoff,
and builder rebasing with a retained empty baseline.

The rebase accommodates the current constructor/open API, transfers the new
installed-Version log watermark and MANIFEST read offset during the handoff,
and leaves asynchronous and remote-compaction opening on their upstream paths.
The patch is applied idempotently by `cmake/ApplyPatch.cmake` before RocksDB builds.

The old WAL-reader retention code and its test
`CatchUpTailsCurrentWalWhenFutureWalExists` already exist in upstream #14781 and
remain in the RocksDB source unchanged. Their duplicate diff hunks are omitted
from the rebased patch file. The later `fix-secondary-flushed-memtable.patch`
remains replaced by upstream #15066's installed-Version-aware retirement.
This is a port of the old opening mechanism, not a new fix for the production
`merge_in.existing_value` abort.

Compatibility checks passed: 137 existing/restored tests with checked Status
objects, plus eight public regressions; clean and idempotent patch application
also passed. This source change is not a production deployment. See
[the runbook](../../docs/runbooks/rocksdb-secondary-merge-base.md).
