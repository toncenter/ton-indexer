# TON Firehose and substreams

The blocks of a TON node as a Firehose stream, substreams modules on them,
and the ton_v1 extension that gives modules account states, get-methods and
the emulation of a transaction on the block they process.

```
node database ──► reader ──► firecore: merger, relayer, firehose
                                          │
                  substreams modules ──► fireton (substreams, ton_v1)
                                          │
                                  ton-tvm-router ──► tvm-executor ──► node database
```

| Directory | What | Built with |
| --- | --- | --- |
| `proto/` | The schemas: `sf.ton.type.v1` (the block), `sf.bstream.v1`, `ton.state.v1` and `ton.tvm.v1` (the ton_v1 calls and the executor service) | each part generates its code |
| `reader/` | `ton-index-firehose`, which firecore runs as its reader on a node database: the conversion of blocks to `sf.ton.type.v1`, also a library of the executor | C++, CMake of `ton-index-worker` |
| `tvm-executor/` | `ton-tvm-executor`, the backend that answers ton_v1 calls from a node database; `ton-tvm-replay` and the reference replay of history; the checks against a liteserver | C++, CMake of `ton-index-worker` |
| `fireton/` | `fireton`, firecore with the ton_v1 extension; `ton-tvm-router`; the test stand (`stand.sh`) | Go |
| `substreams/` | The Rust workspace: the SDK `substreams-ton`, `modules/` to run, `tests/` that check the stack on the stand, `tools/pbgen` | Rust, cargo |
| `ops/deploy/` | The systemd units of firecore on the store host | |
| `ops/backfill/` | The conversion of history in chunks to the store | Python |
| `docs/substreams-tvm.md` | The design of ton_v1: semantics, rules of requests, limits, determinism | |

The operation of the stack is in `docs/runbooks/procedures/firehose.md` at the
root of the repository.

## Building

C++, from the root of the repository (`deploy-firehose.yaml` builds the
reader the same way); the binaries are in `build/ton-index-firehose/reader`
and `build/ton-index-firehose/tvm-executor`:

```bash
cmake -S ton-index-worker -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
```

```bash
cmake --build build --target ton-index-firehose ton-tvm-executor ton-tvm-replay
```

Go, in `fireton/`:

```bash
go build ./cmd/fireton ./cmd/ton-tvm-router
```

Rust, in `substreams/`, a module by itself (a build of the whole workspace
gives another wasm, see `Cargo.toml`):

```bash
cargo build --release --target wasm32-unknown-unknown -p ton_traces
```

## A change of `proto/`

The C++ code is generated at build time. The Go code is checked in
(`fireton/pb`, `fireton/gen.sh`), and so is the code of the SDK
(`substreams/substreams-ton/src/pb`, `cargo run -p pbgen`); modules generate
the code of their own outputs at build time.

`pbgen` generates into a temporary directory before replacing the SDK
bindings, so a compilation error leaves the checked-in files intact. CI
regenerates them and checks that they match the committed files.
