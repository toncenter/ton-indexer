# ton-tvm-executor

Answers `ton.tvm.v1.Executor` over gRPC for the masterchain blocks of a TON
node database, which it reads as a RocksDB secondary: the backend of
ton-tvm-router (`../ton-index-firehose/fireton`). The design is in
`../ton-index-firehose/substreams-tvm.md`.

It answers `get_account` at `after_block` and `before_block`. A request with
any other call is `Unimplemented`, an error of infrastructure, until the
steps of get-methods and emulation. Other answers:

- `Unavailable` for a block the database has not reached: a request beyond
  the head makes the secondary catch up with the node at once, and it does
  every second anyway; also for a block or cells the database cannot read;
- `Internal` for a state or an account that was read and cannot be parsed,
  such as a TL-B variant the converter does not know: it would fail the same
  way again, so fireton ends the request with an error instead of waiting;
  the cells of an account are read before it is parsed to tell the two apart;
- `FailedPrecondition` for a root hash that is not the one of the block;
- `ResourceExhausted` when the batch reply would exceed 256 MiB: fireton
  retries smaller batches, preserving each call's result.

An account is read as liteserver reads it: the shard of the address in the
masterchain state of the block, its top block then, and the account in the
`ShardAccounts` of that state, all loaded lazily from the cell db. The
`ShardAccount` BOC is omitted over 1 MiB, the largest BOC a call can give
back; that is found out before every cell of a large account is loaded. A
nonexistent account is `account_none` with no last transaction, even when it
had transactions in the block: TON does not store such accounts.

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

## Check against a liteserver

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

Inside substreams, `map_account_check` of `../ton-index-firehose/substreams/ton-tvm-probe`
compares the accounts that changed in each block with the states the block
has for them.
