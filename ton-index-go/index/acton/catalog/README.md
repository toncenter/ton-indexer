# Pinned Acton catalog

`catalog.json` is a checked-in snapshot of Acton's 288-contract compiler-ABI
bundle. The `*_gen.go` bindings beside it are gitignored build artifacts; CMake,
Docker and CI generate them, and a direct `go build`/`go test` needs
`CGO_ENABLED=0 go generate ./index/acton/catalog` first.

The generator and runtime are maintained upstream in
[Acton's `packages/abi-go`](https://github.com/ton-blockchain/acton/tree/HEAD/packages/abi-go).

## Provenance

- Source: [ton-blockchain/acton](https://github.com/ton-blockchain/acton) at
  `5dd8d80af21734efc31480849f3d313f4d89e751`,
  [`crates/acton-abi-catalog/data/data-abis.json`](https://github.com/ton-blockchain/acton/blob/5dd8d80af21734efc31480849f3d313f4d89e751/crates/acton-abi-catalog/data/data-abis.json).
- SHA-256, exposed as `catalog.Revision`:
  `b442556faa253aba85e59cf7372aa90d9701fab684b4ec565480fa74a6efde38`.
- Underlying ABI definitions: [ton-blockchain/abis](https://github.com/ton-blockchain/abis).
  This pins Acton's bundle, which does not record a separate abis revision.

## Re-pinning the snapshot

```sh
CGO_ENABLED=0 go run github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go \
  --catalog /path/to/acton/crates/acton-abi-catalog/data/data-abis.json \
  --output-dir index/acton/catalog --package catalog --snapshot
```

Then update the provenance above and `pinnedRevision` in `catalog_test.go`.

## Licenses

`ton-blockchain/abis` material is MIT, Copyright (c) 2026 TON Core — see
[LICENSE-ABIS-MIT](LICENSE-ABIS-MIT). Acton is `MIT OR Apache-2.0`; this
redistribution takes the MIT option, Copyright (c) 2025 TON Core — see
[LICENSE-ACTON-MIT](LICENSE-ACTON-MIT). Neither replaces this repository's own
license.
