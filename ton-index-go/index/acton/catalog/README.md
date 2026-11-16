# Shared pinned TON ABI catalog

Consumed by the Gramstat aggregator and available to the main Go API.
The catalog and native bindings are build artifacts, not tracked source.
The upstream integration follows `feature/acton-api`.

From `ton-index-go`, before a direct build or test:

```sh
sh index/acton/catalog/prepare.sh
go build ./cmd/gramstat-aggregate
# Verify reproducibility without rewriting generated Go files:
sh index/acton/catalog/prepare.sh --check
```

`catalog.lock` pins the release and SHA-256. A valid cached JSON is reused.
`go.mod` pins both the runtime library and its `go tool` generator.
Preparation runs the generator for the host even during a cross-build.
The resulting binary includes the catalog and requires no runtime download.

Public registry: `Contracts`, `ByID`, `ByCodeHash`, and `Revision`.
Hash lookup retains every matching ABI description. Classification is owned by
`internal/gramstat/handlers/accounttypes`, not by this shared catalog.
