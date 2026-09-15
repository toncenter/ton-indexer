# Pinned ABI catalog

`catalog.json` is the compiled contract catalog from a pinned
[ton-blockchain/abis](https://github.com/ton-blockchain/abis) release, and the
native Go bindings beside it (`*_gen.go`) are generated from it. Neither is
tracked in Git: CMake, Docker and CI download the catalog, verify its SHA-256 and
generate the bindings before building. A direct `go build` or `go test` needs the
same steps first, from `ton-index-go`:

```sh
curl -fsSL -o index/acton/catalog/catalog.json \
  https://github.com/ton-blockchain/abis/releases/download/v0.1.0/abi-catalog.json
echo "9f023acf918493dfc908c66cfd38200e99d393beaaf27296da4563479638fea5  index/acton/catalog/catalog.json" \
  | shasum -a 256 -c
CGO_ENABLED=0 go tool tolk-abi-to-go --catalog index/acton/catalog/catalog.json \
  --output-dir index/acton/catalog --package catalog
```

The generator is pinned by the `tool` directive in `go.mod`, at the same version
as the runtime the bindings import. Both are maintained upstream in
[`ton-blockchain/tolk-abi-to-go`](https://github.com/ton-blockchain/tolk-abi-to-go).

## Provenance

- Release: [abis `v0.1.0`](https://github.com/ton-blockchain/abis/releases/tag/v0.1.0),
  built by its release workflow from `24d608491355205cc7b4dccbf7b852297f798a7a`
  with Acton 1.1.0 (Tolk 1.4.1).
- SHA-256, exposed as `catalog.Revision`:
  `9f023acf918493dfc908c66cfd38200e99d393beaaf27296da4563479638fea5`.

## Re-pinning

Set the release version and its SHA-256 in `ton-index-go/CMakeLists.txt`, both
builder stages of `Dockerfile`, `.github/workflows/tests.yml` and the commands
above, and `pinnedRevision` in `catalog_test.go`. `TestTSDifferential` then checks
that decoding still matches the TypeScript reference; regenerate
`testdata/ts-reference.json` with `testdata/generate-reference.cjs` only when a
release intentionally changes decoded values.

## Licenses

`ton-blockchain/abis` material is MIT, Copyright (c) 2026 TON Core — see
[LICENSE-ABIS-MIT](LICENSE-ABIS-MIT). Acton is `MIT OR Apache-2.0`; this
redistribution takes the MIT option, Copyright (c) 2025 TON Core — see
[LICENSE-ACTON-MIT](LICENSE-ACTON-MIT). Neither replaces this repository's own
license.
