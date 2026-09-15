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

Only trace materialization uses this transport. The pre-scheduler startup
FLUSHDB, input subscriber, and health publisher retain their existing clients.

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
