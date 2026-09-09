# Pinned Acton Catalog

Complete 288-contract compiler-ABI catalog, exact input snapshot, and generated
native Go bindings. Production code never reads or parses `catalog.json`.
This package is a consumer of the generator and runtime maintained in
[Acton's `packages/abi-go`](https://github.com/ton-blockchain/acton/tree/HEAD/packages/abi-go).
The snapshot, generated bindings, and catalog-specific tests and TypeScript
goldens remain in TON Indexer.

## Provenance

- Source: [ton-blockchain/acton](https://github.com/ton-blockchain/acton), commit
  `5dd8d80af21734efc31480849f3d313f4d89e751`, verified against the local checkout.
- Source file: [`crates/acton-abi-catalog/data/data-abis.json`](https://github.com/ton-blockchain/acton/blob/5dd8d80af21734efc31480849f3d313f4d89e751/crates/acton-abi-catalog/data/data-abis.json).
- Snapshot: `catalog.json`, created byte-for-byte by the generator, not manually copied.
- SHA-256 / generated `Revision`:
  `b442556faa253aba85e59cf7372aa90d9701fab684b4ec565480fa74a6efde38`.
- Underlying ABI definitions: [ton-blockchain/abis](https://github.com/ton-blockchain/abis).
  This pins Acton's bundle, not a separate abis revision that the bundle does not record.

## Generation

To refresh the snapshot from an explicitly selected Acton checkout, run from
`ton-index-go`:

```sh
CGO_ENABLED=0 go run github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go \
  --catalog /path/to/acton/crates/acton-abi-catalog/data/data-abis.json \
  --output-dir index/acton/catalog --package catalog --snapshot
```

Reproduce from the local snapshot without the Acton checkout, Rust, JS, or a
Tolk compiler (offline once Go dependencies are cached):

```sh
CGO_ENABLED=0 go generate ./index/acton/catalog
CGO_ENABLED=0 go run github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go \
  --catalog index/acton/catalog/catalog.json \
  --output-dir index/acton/catalog --package catalog --check
CGO_ENABLED=0 go test ./index/acton/catalog ./index/actonapi -count=1
```

Use the refresh command with `--snapshot --check` to verify both the snapshot and
generated files against an external upstream input. `--snapshot` writes the
deliberately named `catalog.json`. An existing JSON file is replaceable only when
its digest matches the input digest recorded in the previously generated
`registry_gen.go`. An unrelated or locally modified snapshot is not overwritten.
Reproduction without `--snapshot` does not write the JSON input. Writes are atomic
per file, not per directory. Updating the pinned catalog requires updating the
provenance, digest and capability expectations in tests.

Output: 288 contract files, one registry file, all `gofmt` formatted, and the
snapshot. Handwritten `generate.go`, tests, documentation and licenses are preserved.

## Integration

```text
consumer module: github.com/toncenter/ton-indexer/ton-index-go
runtime (package acton): github.com/ton-blockchain/acton/packages/abi-go
catalog: github.com/toncenter/ton-indexer/ton-index-go/index/acton/catalog
generator: github.com/ton-blockchain/acton/packages/abi-go/codegen
command: github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go
```

The versionless `go run` commands select the Acton Go module version from
`ton-index-go/go.mod`, with checksums recorded in `go.sum`. Generator and runtime
updates are dependency updates to that canonical module. Ordinary Go builds do
not require an installed Acton Rust CLI or a separately installed generator.

Other consumers can invoke the same Go command with `--abi FILE` for a raw
compiler ABI or `--catalog FILE` for a bundle, followed by `--output-dir DIR`
and `--package NAME`. Emitted code imports Acton's runtime above and uses the
`github.com/xssnick/tonutils-go v1.15.5` dependency. Consumers depend directly on
the published Acton Go module; they do not need TON Indexer to generate or use
their own bindings. See the
[Acton binding reference](https://github.com/ton-blockchain/acton/blob/HEAD/packages/abi-go/README.md)
for the generator and runtime API.

Exports: `Contracts`, `Revision`, `ByID`, `ByCodeHash`. Both lookups use maps
initialized once, not scans. Code hashes accept hex/base64 and preserve all
matches in stable contract-ID order. Hash results use a copied pointer slice;
callers cannot mutate the index. Pointed-to contract metadata remains immutable
by convention.

## Capabilities

Counts refer to declared roots, not variants within union roots. Absent
declarations are not classified as unsupported.

| Root | Supported | Unsupported | Total |
| --- | ---: | ---: | ---: |
| Runtime storage | 280 | 1 | 281 |
| Deployment storage | 38 | 0 | 38 |
| Incoming internal messages | 1,854 | 4 | 1,858 |
| Incoming external messages | 62 | 0 | 62 |
| Outgoing messages | 1,173 | 2 | 1,175 |
| Emitted events | 23 | 0 | 23 |
| Getters | 1,199 | 10 | 1,209 |
| **All roots** | **4,629** | **17** | **4,646** |

Combined storage: 318/319 supported. Combined messages/events: 3,112/3,118.
This is codec capability, not proof of all contract behavior or on-chain states.
Strict consumption, ambiguity rejection and resource limits still apply. Codecs
do not execute or authenticate transactions.

All unsupported roots follow. The CLI prints full field paths and reasons;
generated metadata retains `Unsupported` with nil callbacks.

| Contract | Root | Concrete reason |
| --- | --- | --- |
| `bidask.BidaskRange` | `incoming_messages[1]`: `BidaskInternalSwap` | `slippage` consumes the remainder before consuming field `refCell`. |
| `bidask.BidaskRange` | `incoming_messages[2]`: `BidaskInternalSwapV2` | `slippage` consumes the remainder before consuming field `farmingCell`. |
| `bidask.BidaskRange` | `incoming_messages[3]`: `BidaskInternalContinueSwap` | `slippage` consumes the remainder before consuming field `refCell`. |
| `bidask.BidaskRange` | `incoming_messages[4]`: `BidaskInternalContinueSwapV2` | `slippage` consumes the remainder before consuming field `swapAdditionalData`. |
| `bidask.BidaskRange` | `outgoing_messages[0]`: `BidaskInternalContinueSwap` | `slippage` consumes the remainder before consuming field `refCell`. |
| `bidask.BidaskRange` | `outgoing_messages[1]`: `BidaskInternalContinueSwapV2` | `slippage` consumes the remainder before consuming field `swapAdditionalData`. |
| `system.Config` | Runtime storage | `config: ConfigParamDict` requires custom hooks; the ABI contains only flags, not implementations. |
| `frt-gram-adapter.FrtGramAdapterCoordinator` | `get_method_75874_eadb` | Return contains `array<unknown>`. |
| `gaspump.GasPumpMasterV0` | `get_full_jetton_data` | Return `tuple` contains `array<unknown>`. |
| `gaspump.GasPumpMasterV1` | `get_full_jetton_data` | Return `tuple` contains `array<unknown>`. |
| `gaspump.GasPumpMasterV2` | `get_full_jetton_data` | Return `tuple` contains `array<unknown>`. |
| `gaspump.GasPumpMasterV4` | `get_full_jetton_data` | Return `tuple` contains `array<unknown>`. |
| `gaspump.GasPumpMasterV5` | `get_full_jetton_data` | Return `tuple` contains `array<unknown>`. |
| `payment_channels.AsyncPaymentChannel` | `get_channel_data` | Return contains `array<unknown>`. |
| `storages.StorageAggregateContract` | `get_providers` | `providers -> tuple` contains `array<unknown>`. |
| `tonco.Pool` | `getTickInfosFrom` | Return contains `array<unknown>`. |
| `tonkeeper_2fa.Tonkeeper2fa` | `get_delegation_state` | `stateParams -> tuple` contains `array<unknown>`. |

Print the live generated counts and every unsupported root with:
`CGO_ENABLED=0 go test -v ./index/acton/catalog -run TestCatalogCapabilities`.

The Bidask layouts are disabled without altering the source snapshot or guessing
a wire layout. `cocoon.CocoonRoot` storage is supported: raw `slice` dictionary
values have a known boundary at the end of each leaf, as specified by the Tolk
compiler. This does not enable bare slices in arbitrary struct fields.

## Tests And Licenses

Tests regenerate the full catalog in memory, verify output and snapshot without
writing, assert the pinned digest, and verify formatting and capabilities.
Wallet V4 R1/R2 storage/plugin-key vectors and Wallet V5 extension-message values
come from the pinned Acton files below. Reference cells use independent native
builders and the original golden values, not the generated codecs:

- `packages/explorer-core/tests/walletV4Builder.test.ts` and its
  `__snapshots__/walletV4Builder.test.ts.snap`.
- `packages/explorer-core/tests/walletV5Builder.test.ts` and its
  `__snapshots__/walletV5Builder.test.ts.snap`.

WalletTg tests additionally cover qualified generic instantiation names, the bulk
array client override, explicit union prefixes and ref-first chunking. Tests need
neither an installed Acton executable nor a live TON node.

Independent-review regressions cover concrete-instantiation hook flags,
nonterminal remainders through nested types, signed 257-bit getter integers,
leaf-bounded raw slices, and nullable native cell pointers. The committed
`testdata/ts-reference.json` contains 132 cross-language vectors: 120 supported
cases and 12 samples of the six explicitly blocked Bidask roots. It runs in Go
without Node. The optional test-only `generate-reference.cjs` can produce a full
corpus using the installed TypeScript reference runtime; point
`ACTON_REFERENCE_CORPUS` at that output to run the same differential test over it.

`ton-blockchain/abis` material is MIT licensed, Copyright (c) 2026 TON Core. Its
full notice is preserved in [LICENSE-ABIS-MIT](LICENSE-ABIS-MIT). Acton is offered
under `MIT OR Apache-2.0`; this redistribution uses the MIT option for the bundle
and test material. Its Copyright (c) 2025 TON Core notice is preserved in
[LICENSE-ACTON-MIT](LICENSE-ACTON-MIT). The alternative
[Apache-2.0 license](https://github.com/ton-blockchain/acton/blob/5dd8d80af21734efc31480849f3d313f4d89e751/LICENSE-APACHE)
remains available upstream. These notices do not replace this repository's own
license or dependency licenses.
