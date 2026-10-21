# fireton and ton-state-router

The ton_v1 extension of substreams modules: account states, get-methods and
the emulation of one transaction, on the masterchain block a module is
processing. The design is in `../substreams-tvm.md`; the protocols are in
`../proto/ton`, the Rust SDK of modules in `../substreams/substreams-ton`.

- **fireton** (`cmd/fireton`) is firecore with the extension: the same apps
  and flags, plus `--substreams-ton-router-addr` on tier1. tier1 sends the
  address to tier2 with every request, so tier2 needs no flag of its own; it
  calls the router itself, so the address must be reachable from tier2 too.
  The import `ton_v1.execute` sends a module's batch of calls with the block
  to the router, and retries what the router cannot answer yet (`Unavailable`:
  no backend has reached the block, the router is down) for as long as the
  request lives.
- **ton-state-router** (`cmd/ton-state-router`) serves `ton.tvm.v1.Executor`:
  it answers the calls that break a rule of requests itself, for any block,
  and has a backend answer the others. For now the only backend is the stub
  (`router/stub.go`), which knows no accounts; `--stub-lag` makes it reach
  each block that long after its gen_utime, as a lagging backend would.

As fireeth is for Ethereum, fireton is a chain binary built on
`firehose-core`, which supplies the apps, the substreams engine and the
passing of extension params from tier1 to tier2. Blocks stay opaque, as in
firecore: tier1 still needs `--substreams-tier1-block-type=sf.ton.type.v1.Block`.
As in fireeth's RPC extension, connections belong to the extensioner and are
shared by tier2 jobs, and the calls inside a batch are metered by kind
(`metering.AddExternalCalls`: `external_calls_ton_v1_get_method` and so on).
Unlike it, there is no fallback to another block than the one asked for.

## Limits and errors

The limits of ton_v1 in `protocol/limits.go` are provisional: 1 MiB per input
BOC and 8 MiB per serialized `Call`, with all its fields; a call over 8 MiB
gets `TOO_LARGE` in its place and the others are executed. Splitting is the
host's alone: it sends a batch to the router in requests of at most 1000 calls
and 16 MiB, which changes no answer; the SDK makes one host call per batch.

A request with a field the host or the router does not know, such as a kind
of call from a newer SDK, is an error of the extension: answered as
`INVALID_REQUEST` it would be cached, and differ from what a newer server
answers the same module. Only `Unavailable` is retried; `ResourceExhausted`
is not, since gRPC also gives it for messages over the size limit. Requests
have no deadline of their own: a long batch is not run again, and gRPC
keepalive finds a dead connection. The connection to a router that was down
is tried again at least every 2 s, as requests are.

A tier1 request waiting on the router at the head falls behind its
subscription to live blocks; after `--common-live-subscriber-catch-up-timeout`
(30 s) substreams opens a new one, which the user does not see. On the stand,
45 s without the router stopped the stream for that long and then it went on.

## Build

The module builds with firehose-core v1.21.1 and the forks it builds with
(the `replace` block of `go.mod`), on Go 1.27 or newer; wasmtime needs cgo.

```bash
go build ./cmd/fireton ./cmd/ton-state-router
```

```bash
go test ./...
```

After a change of `../proto`, `./gen.sh` generates `pb/` again.

## Test stand

`stand.sh` runs the router with the stub, tier1 and tier2 of fireton as
transient units on the store host, on localhost only and apart from the
production units: binaries in `/srv/scratch/fireton-test/bin`, tier1 on
127.0.0.1:10041. The module `../substreams/ton-tvm-probe` calls every kind of
call on every block.

```bash
sudo ./stand.sh start
```

```bash
substreams run -e 127.0.0.1:10041 --plaintext ton-tvm-probe.spkg map_probe -s 100100000 -t +2000 --production-mode --limit-processed-blocks 0 -o jsonl
```

```bash
sudo ./stand.sh stop
```

Metrics: `substreams_tier{1,2}_wasm_extension_call_counter` (host invocations
per tier, by substreams) and `fireton_ton_v1_execute_*` (host invocations,
retries, durations) on 9141 and 9142, `ton_router_*` (requests, and results by
call kind) on 9140.
