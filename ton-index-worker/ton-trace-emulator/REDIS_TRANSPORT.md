# Redis trace materialization

TraceUpdates still enter `TraceProcessor` in their existing order. The processor
assembles and classifies a trace and submits a prepared `RedisWriteBatch` to
`RedisMaterializer`. Its limit of 16 outstanding writes remains unchanged.

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
