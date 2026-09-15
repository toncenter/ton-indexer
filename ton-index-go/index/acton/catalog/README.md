# Pinned ABI catalog

`catalog.json` is the compiled contract catalog from a pinned
[ton-blockchain/abis](https://github.com/ton-blockchain/abis) release, and the
native Go bindings beside it (`*_gen.go`) are generated from it. Neither is
tracked in Git: CMake, Docker and CI download the catalog, verify its SHA-256 and
generate the bindings before building. A direct `go build` or `go test` needs the
same steps first, from `ton-index-go`:

```sh
. index/acton/catalog/catalog.lock
curl -fsSL -o index/acton/catalog/catalog.json "$ABI_CATALOG_URL"
echo "$ABI_CATALOG_SHA256  index/acton/catalog/catalog.json" | shasum -a 256 -c
CGO_ENABLED=0 go tool tolk-abi-to-go --catalog index/acton/catalog/catalog.json \
  --output-dir index/acton/catalog --package catalog
```

The generator is pinned by the `tool` directive in `go.mod`, at the same version
as the runtime the bindings import. Both are maintained upstream in
[`ton-blockchain/tolk-abi-to-go`](https://github.com/ton-blockchain/tolk-abi-to-go).

## Provenance

`catalog.lock` names the pinned release and the SHA-256 of its catalog, which is
also exposed as `catalog.Revision`. The release notes on that page record the
abis commit, the Acton version and the checksum it was built with.

## Re-pinning

Change both lines of `catalog.lock`. `TestOfflineGeneration` then holds the
generated bindings to the new checksum, and `TestTSDifferential` checks that
decoding still matches the TypeScript reference; regenerate
`testdata/ts-reference.json` with `testdata/generate-reference.cjs` only when a
release intentionally changes decoded values.

## Licenses

`ton-blockchain/abis` material is MIT, Copyright (c) 2026 TON Core — see
[LICENSE-ABIS-MIT](LICENSE-ABIS-MIT). Acton is `MIT OR Apache-2.0`; this
redistribution takes the MIT option, Copyright (c) 2025 TON Core — see
[LICENSE-ACTON-MIT](LICENSE-ACTON-MIT). Neither replaces this repository's own
license.
