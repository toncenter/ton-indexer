# ton-tvm-executor

Answers `ton.tvm.v1.Executor` over gRPC for the masterchain blocks of a TON
node database, which it reads as a RocksDB secondary: the backend of
ton-tvm-router (`../ton-index-firehose/fireton`). The design is in
`../ton-index-firehose/substreams-tvm.md`.

It answers `get_account`, `get_method` and single-transaction `emulate` at
`after_block`, `before_block`, `before_tx` and `after_tx`. Other answers:

- `Unavailable` for a block the database has not reached: a request beyond
  the head makes the secondary catch up with the node at once, and it does
  every second anyway; also for a block or cells the database cannot read;
- `Internal` for data of the network that was read and cannot be used: an
  account or a state that does not parse, such as a TL-B variant the
  converter does not know, a config that TON cannot prepare a transaction
  with, a state at a block boundary that is not the one of the block's
  transactions. It would fail the same way again, so fireton ends the request
  with an error instead of waiting, and nothing is cached. Cells are read
  before they are parsed to tell the two apart: an account before it is
  converted, and the config of a masterchain view whole before a VM context
  or a replay uses it;
- `FailedPrecondition` for a root hash that is not the one of the block;
- `ResourceExhausted` when the batch reply would exceed 256 MiB: fireton
  retries smaller batches, preserving each call's result.

The constants of v1 are in `src/Calls.h`, the same as in
`../ton-index-firehose/fireton/protocol/limits.go`.

## Calls

### get_account

An account is read as liteserver reads it: the shard of the address in the
masterchain state of the block, its top block then, and the account in the
`ShardAccounts` of that state, all loaded lazily from the cell db. The
`ShardAccount` BOC is omitted over 1 MiB, the largest BOC a call can give
back; that is found out before every cell of a large account is loaded. A
nonexistent account is `account_none` with no last transaction, even when it
had transactions in the block: TON does not store such accounts.

### The VM context

A get-method and an emulation run in the context of block N, whatever the
point of their account, and so does a state given as `shard_account_boc`:
now and lt of the state of the shard of the account at N; config, public
libraries and prev blocks of the masterchain state of N; the seed of that
shard block, which is read from the block itself, one block read per shard
block and request. `VmContext.lt` is BLOCKLT and LTIME of a get-method, as in
liteserver; for a transaction it is the requested LT, which TON rounds down
to a multiple of 1,000,000 for BLOCKLT and raises past the previous
transaction and the inbound message.

The overrides of a call replace config params by number, add libraries by
hash, and set now, lt and the block seed. `block_rand_seed` replaces the seed
of the block, not the seed of TVM, which TON derives for the account by the
global version of the config in effect: from version 8 SHA256(block seed ||
account id), before it with the first 4 bytes of the account id, then its
first 28. A zero seed of a block is a data error, since validators reject such
blocks: the executor checks it before native code, which would make a random
one. Anycast addresses are not supported: TON derives the seed of an anycast
account from its original address, the executor from the rewritten one.

### get_method

A get-method runs as liteserver runs it (`LiteQuery::finish_runSmcMethod`):
the same c7, with the account's own libraries after the public ones. The
result stack has lisp-style lists as `list`, values nested over 32 levels as
`unsupported`, and is `TOO_LARGE` over 8 MiB, which is counted while it is
made: the BOCs of large cells are found to be over it before all their cells
are read.

### emulate

`src/Emulate.cpp` runs TON's unmodified `TransactionEmulator`, an instance
per call, with its seed derivation, BLOCKLT rounding and `ignore_chksig`. The
destination comes from the internal or external-in message and must match
any account given; no account source selects the network state of the
destination after N, and a StateInit in the message deploys a contract. The
answer is a transaction with its outgoing messages, which are not executed,
its BOC and optionally the new state, or `external_not_accepted` with the
exit code of the emulator. The transaction is converted as Firehose converts
one, with the normalized hash of an inbound external message, which a block
has from its parsed transactions.

Before the emulator, the executor prepares the transaction config as the
emulator does first: an error is `INVALID_REQUEST` with config params of the
call and `Internal` without them. An error of the emulator after that is of
this message on this account: `EMULATION_FAILED`. Native C++ precompiled
execution is disabled in this build, so those contracts execute in TVM;
elapsed time is omitted from returned precompiled logs.

### before_tx and after_tx

The executor walks the shard chain of the account from its block at N back to
its block at N−1, across splits and merges, to the block with the
transaction. An LT that is not of a transaction of the account in the round
is `INVALID_REQUEST`. A snapshot names the shard block of the transaction.

The state before the first transaction of the block and after the last are
read from the database and checked against the state hashes of the
transactions. The states between them are replayed (`src/Replay.cpp`): native
execution from the state before the block, with the block's time, start LT
and seed, each transaction's LT, and the config, libraries and prev blocks of
its `master_ref` (N−1 for a masterchain block); public overrides never enter
it. The executor checks that TON's rounded LT of each transaction is the
block's `start_lt`, and every replayed transaction and resulting state must
have the original hashes; otherwise, and for a transaction type the emulator
does not have, the points after it are `REPLAY_MISMATCH`. A replay goes one
transaction per turn of the actor, and is kept for the batch with the states
it made: the points of one account replay its block once. The block data
stays alive with it, since its lazy archive cells refer to it.

Historical compatibility is limited by the pinned TON emulator. At
masterchain block 1,000,000 the elector tick has `gas_used=9600` in the
block, and 7125 in this emulator; the other fields and the resulting state
hash match. The points after it are `REPLAY_MISMATCH`, and the boundary
states are read anyway. Gas or hashes are never replaced with the recorded
values to pass the check.

## Execution model

The gRPC callback service only enqueues an owned request and completes the
RPC when its answer is ready. Foreign threads write to a bounded
`MpscPollableQueue`; they never enter the TON scheduler. An IO actor drains
that queue and creates one CPU actor per RPC batch.

The request actor owns its block views, shard roots and results. DbScanner
replies arrive as actor messages through `td::Promise`. Calls within a batch
run in order, yielding between calls; separate batches run concurrently.
Cell reads and BOC serialization are synchronous operations on TON workers.
`--threads` sets the worker count, and `--max-requests` bounds the total
queued and executing batches (default 128). Excess requests get `Unavailable`.
Cancellation is checked between calls, on database replies and by an actor
timer while waiting. Shutdown stops admission and keeps the scheduler
running while gRPC drains requests; after five seconds gRPC cancels the
remaining RPCs.

`before_block` follows the predecessor ID of the validated masterchain
block, including its zerostate parent for block 1, rather than looking up
zerostate in the archive's seqno index.

## Build

It needs grpc++ and its protoc plugin, and is skipped where they are missing:

```bash
sudo apt-get install libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler
```

```bash
cmake --build build --target ton-tvm-executor
```

Stack conversion, ShardAccount metadata, call context and VM log bounds have
regression tests:

```bash
cmake --build build --target test-ton-tvm-calls test-ton-tvm-link
./build/ton-index-worker/ton-tvm-executor/test-ton-tvm-calls
./build/ton-index-worker/ton-tvm-executor/test-ton-tvm-link
```

On Linux with TON's static OpenSSL and system gRPC, the executor includes
the complete static crypto libraries and hides their symbols from shared
libraries. This keeps TON's EVP objects out of the system OpenSSL used by
gRPC. `test-ton-tvm-link` starts gRPC and verifies a valid Ed25519 signature
and a corrupted one in that same process.

grpc++ from the system packages is built with the system Abseil, TON with
its own. Their symbols do not clash (each Abseil has its own inline
namespace), but their headers must not meet in one source: the gRPC server is
a library of its own (`src/ExecutorServer.cpp`) that sees no TON header, with
an interface of protobuf types only, and grpc++ is linked to it privately so
that its include directories do not reach TON sources. protobuf 3 does not
use Abseil, so the generated messages are used on both sides. The messages
of `sf.ton.type.v1` and the conversion of accounts come from
ton-index-firehose (`ton-firehose-proto`, `ton-firehose-convert`).

## Run

As the owner of the node database, with a high limit of open files: an
archive node opens thousands of package files.

Replace the example address `192.0.2.10` with the archive node's private
address in the commands below.

```bash
ton-tvm-executor --db /var/ton-work/db --working-dir /var/tmp/tvm-executor/work --listen-addr 192.0.2.10:10060
```

## Checks

`check-liteserver.py` compares `get_account` with `getaccount` of
lite-client on random blocks of the history and near the head: the shard
block of the state, the last transaction, the balance, whether the account
exists, and the last transaction in the `ShardAccount` BOC. On an archive node
(2026-10-21), 540 accounts over 45 blocks from 1M to the head agreed. A
request of a dozen accounts took about 1 s cold, mostly reading the data of
the elector and the config account, and 30 ms warm.

```bash
./check-liteserver.py --executor 192.0.2.10:10060 --liteserver 127.0.0.1:20700 --liteserver-key /tmp/ls.pub --proto-dir ~/tvm-proto --samples 40
```

With `--get-methods` it compares get-methods instead, against `runmethod`
of lite-client at the same block, with the gas limit of liteserver (300 000):
the exit code and the result stack as lite-client prints it, cells by their
hashes (cells of trees with Merkle proofs, as in the complaints of the
elector, are not hashed by the script). On an archive node (2026-10-22), 2353 calls
over 90 blocks agreed, among them 69 `participant_list` of the elector with
up to a few hundred participants and 41 `get_plugin_list`; 7 more were lists
liteserver cannot serialize. After the step of emulation (2026-10-22), 552
accounts and 1,280 get-methods over 45 blocks each agreed again.

```bash
./check-liteserver.py --executor 192.0.2.10:10060 --liteserver 127.0.0.1:20700 --liteserver-key /tmp/ls.pub --proto-dir ~/tvm-proto --samples 80 --get-methods
```

Inside substreams, the modules of `../ton-index-firehose/substreams/ton-tvm-probe`
check the calls on the stand (`../ton-index-firehose/fireton/README.md`). On
2026-10-22, in tier2 and again from the cache, with no mismatches:

- `map_emulate_check`: 2,000 blocks from 80,042,000, 14,380 calls, 8,180
  transactions and 2,040 rejected external messages, fixtures on 20 of them;
- `map_tx_point_check`: 2,000 blocks from 80,036,000, 54,932 calls, 27,416
  snapshots, 12,510 of them replayed, 4,879 pairs of get-methods and of
  emulations on states against their BOCs;
- `map_get_method_check` and `map_account_check`: 500 blocks from 60,000,000;
  100 blocks at the head for transaction points and emulation.

Earlier runs passed 20 blocks of transaction points near 40M and 100 blocks
of get-methods near 1M.
