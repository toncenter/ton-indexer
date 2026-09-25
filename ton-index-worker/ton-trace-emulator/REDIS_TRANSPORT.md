# Redis trace materialization

TraceUpdates still enter `TraceProcessor` in their existing order. The processor
assembles and classifies a trace and submits a prepared `RedisWriteBatch` to
`RedisMaterializer`. The processor allows up to 64 outstanding writes.

Finalized updates and confirmed-to-finalized promotions take priority over
unrelated nonfinalized work and cleanup, both before classification and when
waiting for Redis. Within each trace, waiting finalized updates and promotions
form a FIFO prefix ahead of nonfinalized requests. Every finalized update is
retained: separate updates can supply different fragments of the same trace.
An update already being prepared, classified, or written finishes before the
next one; it inherits priority if a finalized request is waiting behind it.

Accepting a finalized update or promotion immediately discards queued pending
updates of that trace. The scheduler also sends one batched notification when
it closes the shard blocks of a finalized masterchain block. The processor
discards queued confirmed updates from those logical `BlockId`s, including
losing versions with different root/file hashes. It uses the block ids on the
fragment roots, never the predicted confirmed `mc_block_seqno`. Updates spanning
several blocks are discarded only when all their source blocks are in the batch.
The scheduler filters later confirmed arrivals, so the processor needs no
second closed-block registry. Each discarded update releases its queue count
and completes successfully (an empty snapshot for confirmed), with telemetry
attribute `ton.trace_state.superseded=true`. Already prepared/running work,
all finalized requests, and confirmed updates from other blocks are retained.

Each classifier receives at most one outstanding request, so its mailbox does
not hide a nonfinalized backlog from this policy.

At 10,000 outstanding trace updates, the processor rejects new pending and
confirmed updates. Finalized updates remain admissible: the scheduler submits
only one finalized block for commit at a time, with at most two blocks computing
or committing. A large committing block can exceed the threshold; nonfinalized
admission resumes after the total backlog falls below it. This threshold is not
a hard memory limit, and does not change Redis failure/retry semantics.

`RedisMaterializer` is a CPU actor, owned by `TraceProcessor` through `ActorOwn`.
Callers submit batches with `send_closure`. It encodes commands with hiredis and
assigns each batch to an idle `RedisConnectionActor`. Connections are created
lazily, reused, and limited by the materializer capacity. There is no additional
unbounded pending queue and no worker thread per connection.

`RedisConnectionActor` runs with `with_poll()` on TON's existing I/O worker.
It uses a nonblocking `td::SocketFd`, subscribes to TD's poller, and feeds received
bytes to the hiredis RESP reader. It does not call `redisGetReply`,
`redisBufferRead`, `Pipeline::exec`, or any other blocking hiredis network API.
Each actor turn handles at most 64 KiB in each direction and 256 replies, then
yields if more work is ready. Empty readiness waits for a poll notification.

## Ordering and failures

- One batch is active per connection. Batches on different connections can
  progress concurrently; `TraceProcessor` still serializes each trace's updates.
- All commands in the data phase are pipelined in their original order, including
  chronological plans carried from a failed write.
- Trace `PUBLISH` commands are sent only after all data replies are successful.
  Account-state notifications remain inside the existing atomic Lua script.
- Completion is delivered in actor scheduler context after publication replies.
  The pool returns the original batch on failure so the existing carry-forward
  and cleanup retry policies continue to work.
- A timeout, malformed response, or command error closes the connection. The
  next submitted batch reconnects and repeats AUTH/SELECT. The transport never
  automatically replays a batch: some of its commands may have executed before
  the connection failed.
- Stopping the pool/connection reports errors for active work. The finalized
  commit barrier, block-data lifetime, and error handling in `TraceScheduler`
  are unchanged. In particular, this change does not turn its completion marker
  into a durability or all-writes-succeeded guarantee.

## Finalized-only producer

`ton-finalized-streamer` uses the same transaction decoder, MCH classifier,
serializer and Redis transport as the ordinary emulator. Its assembly path is
separate:

1. `FinalizedTraceScheduler` fetches a masterchain block and uses the stateless
   `BlockParser` to decode the associated blocks into flat `TransactionInfo`
   records. It retains block owners/root cells with the parsed block.
2. `FinalizedTraceProcessor` owns both active trace graphs and the index mapping
   outstanding internal messages to their trace identities. Transactions are
   applied in LT/hash order directly to the flat graph; no fragment trees or
   emulated tails are constructed. All changes to one trace in a block are
   collected before classification. Unknown-root continuations are skipped.
3. The shared account/interface detector works from the changed account addresses.
   MCH classifies each complete known graph using the shared `TraceAssembler`
   view. `TraceMaterialization` prepares a full snapshot directly, rather than
   first building an emulator delta. Plans are emitted as individual traces
   finish; the block promise completes after all plans have been emitted.
4. The scheduler writes plans with up to 64 requests in flight, retries failed
   writes, updates shared health after all local jobs succeed, then advances.

The ordinary `TraceProcessor` retains pending/confirmed updates, promotion,
invalidation, dirty batches and its Redis writer. It has no finalized-only mode.
The finalized processor owns no Redis connections and has no promotion or
per-trace input queues. Each executable creates its own classifier workers.

Only completed traces expire from finalized processor memory, using block time.
Open traces retain their full graph across blocks even after Redis replay expiry.
Traces above 1000 nodes emit cleanup instead of a snapshot. Their IDs and message
routing remain available for later continuations and committed account updates.
An update and retention cleanup for the same trace never run in the same block.

`write_finalized_trace` executes one Lua operation per plan. The
`finalized:version:<trace>` key fences incoming versions at or below the last
successful masterchain seqno. Data and publications precede the version marker;
the first successful result for a trace/version wins. Snapshots and cleanup use
the same check. The marker TTL is the larger of 600 seconds and the snapshot TTL
(cleanup uses 600 seconds). Deduplication is bounded by that TTL and does not
provide exactly-once delivery across expiry or Redis data loss.

Plans carry committed account states. Both executables use the same encoder and
Lua function for `account_finalized:<address>` (`lt`, `state`, `interfaces`, with
a 60-second TTL). A greater LT or an incoming zero LT replaces the state and
publishes its hint. Other LTs refresh the TTL. Decimal uint64 strings avoid Lua
number rounding. Zero represents a deleted account; the existing LT comparison
does not distinguish delayed older states from later recreation after deletion.
Account states do not wait for `trace_complete`.

`finish_finalized` only updates `health:ton-trace-emulator` to the greatest local
completed seqno while that key exists. It does not commit a global block or fence
other producers' writes. Each producer starts at its node's current head and
maintains its own graphs/message index; state is not restored on restart. A
running producer can therefore still publish a known trace after a newly started
producer has finished a newer block. Redis stores no bootstrap, block manifest,
configuration fingerprint or durable processing cursor. Neither startup nor
recovery calls FLUSHDB.

Lua does not roll back earlier commands after a command error. The scheduler
retries errors after 0.5 seconds; a partially failed operation can republish data
because its success marker was not written. Persistent errors require operational
repair. Command size and Lua argument-count limits are checked before sending.

## Configuration

The transport uses the existing Redis URI parser and supports standalone TCP
and POSIX UNIX sockets, RESP2, password/ACL authentication, and database selection.
TCP hostnames are resolved once before the scheduler starts. Reconnects reuse
that address; DNS changes require a process restart. Redis Cluster, Sentinel,
TLS, and RESP3 are outside this transport's scope.

`connect_timeout` defaults to 2 seconds. `socket_timeout` is interpreted as an
absolute batch deadline (setup, data, and publications), defaulting to 5 seconds;
slow partial responses do not extend it. An encoded batch is limited to 64 MiB.
Unexpected/oversized replies fail the batch. The materializer's concurrency
limit controls the number of connections; increasing URI `pool_size` does not
increase that limit. URI pool wait/lifetime/idle settings no longer govern this
actor pool.

In the ordinary emulator, only trace materialization uses this transport. Its
pre-scheduler startup FLUSHDB, input subscriber, and health publisher retain
their existing clients. The finalized-only producer also uses the transport
for its health control operation.

## Validation

Build `ton-trace-emulator`, `test-ton-trace-processor`, and `test-ton-trace-state`.
The `RedisTransport` cases in `test-ton-trace-processor` use an isolated loopback
peer with controlled partial replies, delayed replies, and disconnects. They
also exercise AUTH/SELECT, publication ordering, connection reuse, capacity,
shutdown, and a large request with a responsive poll-worker timer. They require
permission to open local sockets and do not access or clear an external Redis.

For the optional real Redis integration case, set
`TON_REDIS_TRANSPORT_TEST_URI` to an isolated test instance (for example a
temporary password-protected UNIX socket with TCP and persistence disabled).
It verifies binary hash fields, index scores, Lua account updates, ordered
notifications, and cleanup using keys prefixed with `redis-transport-test:<pid>:`.
No FLUSHDB is issued by the tests.

`TON_FINALIZED_TEST_REDIS_URI` enables the finalized protocol integration cases.
Use a dedicated test DB with no health key; the tests check trace-version
idempotence, overlapping producers, account updates, and stale cleanup protection. The controlled loopback peer also verifies that
several trace requests are outstanding before any Redis reply is delivered,
and that the scheduler waits for a failed job's successful retry before finish.

`FinalizedTraceProcessor` tests feed flat transactions and check within-block and
interblock assembly, processor-local message routing, completion, retention,
oversized continuations, real classification, and compatibility with snapshots
prepared by the ordinary fragment assembler. The tests use a synthetic shard
state to exercise the shared account detector without a TON node database.
