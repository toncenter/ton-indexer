# Latest account code-hash distribution

`gramstat-aggregate --handlers account_code_hashes` reads the existing Kvrocks
secondary indexes and publishes all `(code_hash, address_count)` values to
ClickHouse. The Go daemon schedules it every **10 minutes** with a **2-minute**
deadline. It does not modify the blockchain indexer, run TVM, read account
payloads or replay blocks.

## Definition and consistency

The measured population is membership in
`ton-index:v1:idx:latest_account_states:code_hash:<base64>:by_account`.
The writer builds this index when a latest state has a code hash; inactive states
normally have none. The source is explicitly **latest-code-hash-indexes**, not an
independently verified count of all active accounts. Active accounts without code
are not represented. There is no interface/protocol classification yet.

Counts use `ZCARD` (stored sorted-set metadata), not member enumeration. A code
change removes the address from its old hash's index; historical versions are not
accumulated. The delivery-monitor logger was independently checked across four
previous code hashes: old memberships and empty index keys were gone, while the
current hash contained its one address.

This is a **live observation over `[started_at, finished_at]`**, not a snapshot at
one masterchain block. The endpoint may balance connections between replicas.
Each prefix records the replica's progress before/after its scan. Snapshot
`source_mc_min/max` summarize these observations; neither is an exact state
boundary, and their spread includes replica skew as well as elapsed collection
time. Missing/regressing progress, connection failures, malformed results,
repeated cursors/keys, or incomplete coverage fail the entire run.

## Bounded read strategy

- Scan all 64 first-character Base64 prefixes, sequentially.
- Keep one pinned connection for the entire prefix; never retry/reconnect while
  retaining a replica-local numeric cursor.
- Use `EVAL_RO`: `SCAN MATCH <prefix>* COUNT 250`, then `ZCARD` for that page.
- Return compact `hash:count` lines rather than repeated full index key names.
- Preserve every nonzero count, not just the top 1000. Vanished/zero keys are
  counted separately in `zero_hashes`.
- Collection deadline: 90 seconds; connection: 5 seconds; individual I/O:
  10 seconds. A five-million-hash bound prevents runaway collection.
- No parallel readers: measured parallelism did not materially improve total
  time and increased concurrent-request latency.

Read-only production probes on 2026-11-15 reduced a full ~457k-hash pass from
62.55 seconds to **18.35–19.66 seconds** with page size 250 and one reader.
The earlier long pipelines encountered timeouts; the bounded scripts avoid that
path. Changing connections while retaining a SCAN cursor also restarted scans
on the other replica; prefix-local connections eliminate that error.

The first integrated Go run on the same day collected **456,976 hashes /
191,219,825 indexed addresses in 7.67 seconds**, with **8.26 seconds** including
ClickHouse insertion and read-back verification. The API's bounded top-1000 query
used the snapshot primary key and took **50 ms** from the application host.

## Configuration and invocation

Protected `/etc/gramstat-aggregate.env`:

```sh
GRAMSTAT_KVROCKS_ADDR=kvrocks.example.internal:6669
GRAMSTAT_KVROCKS_USERNAME=
# GRAMSTAT_KVROCKS_PASSWORD is supplied separately in the protected environment.
```

The source needs `EVAL_RO`, `SCAN`, `ZCARD`, `GET` and connection authentication.
The supplied source password must never be placed in CLI arguments or logs.

```sh
gramstat-aggregate --print-schema
gramstat-aggregate --handlers account_code_hashes --timeout 2m
# Add account_code_hashes to the daemon's existing handler list:
# --code-hashes-interval 10m --code-hashes-timeout 2m
```

Explicit historical dates are rejected. Recent/reconciliation runs both collect
latest indexes; there is no historical source backfill. Restarts coalesce missed
intervals through the existing scheduler and never overlap this handler.

## Storage and publication

Writer-owned DDL:
[`schema.sql`](../ton-index-go/internal/gramstat/handlers/codehashes/schema.sql).

1. Finish collecting all prefixes before writing.
2. Generate a fresh snapshot UUID.
3. Insert **64 rows** into `gramstat.account_code_hash_counts`, one per prefix,
   each with its full `Map(String, UInt64)`. Counts are ZSTD-compressed.
4. Read back that UUID/day and verify 64 distinct prefixes, key-prefix ownership,
   number of hashes and sum of counts.
5. Insert one completion row into `gramstat.account_code_hash_snapshots` **last**.

The completion row includes start/end/publication times, full hash/address
totals, observed MC range, page/zero counts and `consistency='live-index-scan'`.
Interrupted staged generations are invisible to readers. A retry uses a fresh
UUID, not additive counters. There is no automatic history deletion or TTL;
completed observations retain the full distributions for future reclassification.

Grant the aggregate user `SELECT, INSERT` on both tables and the API user
`SELECT` only. Schema creation is performed separately by an administrator.

Readers first capture the newest completion row ordered by `finished_at DESC,
snapshot_id DESC`, then query only its UUID and `observed_day`. Never find the
latest generation in the counts table: that may be an interrupted run.

## API

Gramstat exposes `GET /api/v1/accounts/code-hashes?limit=1000` (limit 1–1000,
default 1000). It returns the captured snapshot metadata, counts sorted descending
with a deterministic hash tie-break, exact `topAccounts`, `otherAccounts`, and
`otherCodeHashes`. All 64-bit counts are decimal strings. A missing publication
returns `snapshot: null` and unknown totals, not zero. The current UI is unchanged.

Top selection does not alter stored coverage or the denominator. This endpoint
describes latest observations and accepts no historical time range.
