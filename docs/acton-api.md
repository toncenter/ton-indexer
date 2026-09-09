# Acton API And Builds

## Architecture

The generator and shared Go runtime are owned by
[Acton](https://github.com/ton-blockchain/acton/tree/d6e28585b6f9e2a37c241de4d514f77210a99da2/packages/abi-go), in the
module `github.com/ton-blockchain/acton/packages/abi-go` (root package `acton`).
TON Indexer consumes that module at the version selected by `ton-index-go/go.mod`.

The current pin is `v0.0.0-20260909215956-93cc404a4a95`. While
[Acton PR #1272](https://github.com/ton-blockchain/acton/pull/1272) is unmerged,
Go cannot resolve its commit through the upstream repository. The Go manifests
therefore use a version-pinned remote replacement from `1IxI1/acton`; all source
imports retain the canonical `ton-blockchain/acton` module path. This needs no
local Acton checkout or workspace. Remove the remote replacements and update the
pin after the module lands upstream. The emulate and streaming modules carry the
same replacement because Go does not inherit dependency modules' replacements.

`ton-index-go/index/acton/catalog/catalog.json` is the pinned, checked-in catalog
snapshot. It contains compiler ABIs and catalog metadata. Acton's pure-Go
`cmd/tolk-abi-to-go` tool generates Go types, native codec bindings, and a registry
in `index/acton/catalog/*_gen.go` before compilation. These files are ignored build
artifacts, absent from a fresh checkout. The snapshot, static `generate.go`, and
catalog-specific tests and TypeScript goldens remain checked in with this consumer.

The API uses these compiled bindings and Acton's shared codecs backed by
`tonutils-go`. It does not interpret compiler ABI JSON on each request, invoke
the Acton CLI, or fetch a catalog at startup. The generated `catalog.Revision`
is the SHA-256 of the exact snapshot bytes, also exposed in API responses.

The generator and Acton handlers are pure Go. The full `ton-index-go` executable
still uses the existing `ton-marker` CGO library for legacy functionality; this
integration does not remove that native build dependency or change the old
`/api/v3/runGetMethod` endpoint.

## Endpoints

All routes use the existing `/api/v3/acton` prefix and inherit `/api/v3/`
middleware and routing. No separate root `/acton` reverse-proxy route is needed.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v3/acton/contracts` | Paginated catalog metadata. |
| GET | `/api/v3/acton/abi` | Full compiler ABIs for repeated `code_hash` parameters. |
| GET, POST | `/api/v3/acton/accounts` | Batch identification, optionally decoding storage. |
| GET | `/api/v3/acton/getMethods` | Getter metadata by address, code hash, or catalog ID. |
| POST | `/api/v3/acton/decode` | Native decoding of a supplied storage or message BOC. |
| POST | `/api/v3/acton/runGetMethod` | Typed getter execution pinned to an upstream seqno. |

See the [handler reference](../ton-index-go/index/actonapi/README.md) for request
and response schemas, limits, errors, and pinning guarantees. Swagger annotations
live on the [handlers](../ton-index-go/index/actonapi/api.go). Catalog links are
assertions, not source verification; getter pinning trusts the configured v2
upstream and does not verify blockchain proofs.

## Generation

Use the Go version declared in `ton-index-go/go.mod`. Run these commands from
`ton-index-go`:

```sh
CGO_ENABLED=0 go run github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go \
  --catalog ./index/acton/catalog/catalog.json \
  --output-dir ./index/acton/catalog --package catalog

# Verify the just-generated output against another generation in memory.
CGO_ENABLED=0 go run github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go \
  --catalog ./index/acton/catalog/catalog.json \
  --output-dir ./index/acton/catalog --package catalog --check

CGO_ENABLED=0 go test ./index/acton/catalog ./index/actonapi -count=1
```

`CGO_ENABLED=0 go generate ./index/acton/catalog` is the package-local generation
shortcut. The versionless `go run` command uses the Acton Go module version
selected by `go.mod`, with dependency checksums in `go.sum`; it needs no installed
Acton Rust CLI or separately installed generator. The
[Acton binding reference](https://github.com/ton-blockchain/acton/blob/d6e28585b6f9e2a37c241de4d514f77210a99da2/packages/abi-go/README.md)
documents single-ABI generation, the catalog envelope, value formats, and
supported layouts.

Direct Go commands do not run generators automatically. From a fresh checkout,
or after changing the catalog or generator dependency, generate before building
or running the full test suite (the full API still requires the marker library):

```sh
CGO_ENABLED=0 go generate ./index/acton/catalog
go build ./...
go test ./... -count=1
```

With an installed Acton build that includes Go wrapper support (currently the
[PR branch](https://github.com/ton-blockchain/acton/pull/1272)), the CLI alternative
uses Acton's bundled generator:

```sh
acton wrapper --catalog ./index/acton/catalog/catalog.json --go \
  --output-dir ./index/acton/catalog --go-package catalog
```

CMake, Docker, CI, and `go generate` use the pinned Acton Go module command so
their generator matches the runtime dependency without waiting for a public
Acton release containing `--go`. Both entry points are maintained in Acton.

When updating the catalog, pin and record the upstream revision used to produce
the snapshot, regenerate, and review the snapshot and capability test results.
Commit the snapshot, provenance, and any changed test expectations or module pin;
do not commit generated Go files. Review unsupported-root diagnostics:
they explicitly disable individual bindings, but do not by themselves fail
generation or mean that every catalog contract is fully supported. Malformed
catalogs fail generation. Do not edit generated Go files by hand or run competing
generators against the same directory.

## Builds

From the repository root, with CMake already configured in `build`:

```sh
# Bindings only, without building the native marker library or Swagger.
cmake --build build --target ton-index-go-abi

# Generates bindings before Swagger and Go compilation.
cmake --build build --target ton-index-go

# Equivalent for a CMake Unix Makefiles build directory.
make -C build ton-index-go

docker build --target index-api -t ton-indexer-api .
```

The CMake `ton-index-go-abi` target invokes the pinned generator on every dependent
build, repairing any missing output without globbing generated files or relying on
a stale stamp. Unchanged bindings retain their contents and modification times.
The ignored source-tree artifacts survive `cmake --build build --target clean`;
if removed, the next build recreates them. Generation finishes before Swagger and
Go compilation. The emulate and streaming targets also depend on this step because
they import the catalog indirectly through `index/crud`.

CI starts without generated Go files, generates them, then runs `--check` and the
catalog/API tests. This checks reproducibility and behavior, not freshness of
tracked generated files.

The Docker context excludes `*_gen.go` catalog artifacts. The API builder copies
the local snapshot and resolves the generator through the copied `go.mod` and
`go.sum`. It runs the same generation command before `swag init` and `go build`;
the emulate builder generates the catalog in its copied `ton-index-go` dependency
too. These commands use normal Go module/build caches and disable CGO only for
generation. Neither build path
downloads a latest catalog or needs a local `acton/` checkout, Rust, or Tolk
compilation. Catalog data is entirely local; ordinary Go dependencies and build
tools may still require network access on a cold cache. This pins ABI generation,
not unrelated Docker base images or tools.

## Explorer Integration

### Account Pages And Batch Preloading

TonScan and other clients can collect visible account addresses and issue one
request instead of fetching metadata for each hover:

```http
POST /api/v3/acton/accounts
Content-Type: application/json

{"addresses":["<account address>","<another account address>"],"include_storage":false}
```

The response's `accounts` array preserves first-occurrence input order after
canonical address deduplication. Key UI state by the returned `address`, not the
original friendly-address spelling. Each item has an independent status, state
identifiers, and a `types` array. Catalog matches contain display names and source
links; public-interface hints are explicitly distinguished from exact code-hash
matches. Unknown accounts remain in the response. Fetch `include_storage=true`
when opening the storage panel rather than for every hover.

For the getter panel, call `/api/v3/acton/getMethods?address=...`. Its compiler ABI
contains the field/type descriptions required to render forms; the native getter
metadata contains the matching parameter and result type references. The getter
execution endpoint accepts named arguments and retains the raw stack alongside
the decoded result. Render unnamed tuple results positionally: an ABI cannot
supply semantic names that were never declared.

### Trace Hovers And Code Changes

Confirmed trace responses include a lightweight `trace.contract_info` object:

```text
contract_info.accounts[transaction.account]
    -> encountered code hashes
contract_info.by_code_hash[hash]
    -> interfaces and catalog candidates
```

For a particular transaction, prefer the state's `contract_info_key`:

```ts
const key = transaction.account_state_before?.contract_info_key
const info = key ? trace.contract_info?.by_code_hash[key] : undefined
```

Use `account_state_after` for the resulting implementation after an upgrade.
The account-level list contains all encountered versions, not just the latest
one. Hash keys use standard padded base64. Multiple candidate labels indicate
ambiguity; an empty summary means an unknown hash. No ABI documents or decoded
storage are repeated on every trace node, and this enrichment adds no database
queries or getter calls.

Do not replace missing historical metadata with the latest `/acton/accounts`
result and display it as historical fact. Pending payloads currently lack
per-state code hashes, so those placeholders have no contract metadata. See the
[trace reference](../ton-index-go/index/crud/trace_contracts.md) for the complete
shape, pending limitations, and integration tests.

### ActonScan ABI Registry

`GET /api/v3/acton/abi?code_hash=...&code_hash=...` returns the existing ActonScan
`ExtendedContractABI` shape: `compiler_abi`, `code_hashes`, `catalog_id`,
`display_name`, `known_addresses`, and `links`. The result is keyed by the
requested hash spelling, with null for unknown hashes. An ActonScan metadata
registry adapter can consume it directly; the existing frontend does not switch
to this endpoint automatically. The original ABI remains available for browser
forms and local decoding even though the Go server uses compiled bindings.

Use the returned catalog revision (or `X-Acton-Catalog-Revision` for ABI lookup)
to invalidate metadata caches. Latest account metadata additionally depends on
the account state/code hash; historical trace metadata depends on the state
inside that trace. Avoid caching a decoded body solely by its BOC when the chosen
contract ABI can differ.

### Adapting An Existing ActonScan Client

ABI metadata is shared, but the typed execution endpoint is not a drop-in URL
replacement for the existing `/runGetMethod` provider. A client adapter should:

- Send the displayed implementation's `code_hash` with a getter request. A code
  upgrade between opening the form and execution then produces a conflict instead
  of combining the browser's old ABI with a new implementation.
- Treat `success` as the execution outcome (TVM exits 0 and 1 are successful).
  Native stack numbers have type `int`; older providers expect `num`. Normalize
  this recursively at the boundary. If `stack` is null or `stack_error` is set,
  retain and show `raw_stack` and the error rather than calling `.map` on null.
- Adapt native values explicitly before passing them to TypeScript ABI editors
  or serializers. Native `Cell<T>` values are direct decoded payloads, dictionaries
  are typed key/value entry arrays, and unions use `{"$":"Type","value":...}`.
  Existing TS forms use referenced-value wrappers, dictionary objects, and
  flattened struct-union fields. Use the ABI type graph for conversion; do not
  flatten every object or invent names for tuple positions. Integer values travel
  as decimal strings, including values beyond JavaScript's safe integer range.
- Preserve conflict and size errors separately from unknown hashes. `/abi` can
  return 409 for an ambiguous hash or 413 for an oversized batch; neither means
  that the hash is unknown. Split batches as needed, retain ambiguity, and use
  `/getMethods?code_hash=...` to inspect candidates. Do not silently fall back to
  the first bundled ABI for a conflicting hash.
- Capture `X-Acton-Catalog-Revision` and HTTP error status before unwrapping the
  response. Cross-origin clients need the gateway to expose this header and
  support JSON POST preflight. Gateway/browser behavior is deployment-specific
  and has not been verified by the in-memory handler tests.

For traces, join transactions by hash or canonical account plus LT, never LT
alone. Use per-state `contract_info_key` links and preserve missing historical
hashes. The current explorer's latest-account/per-address fallbacks must not
override the historical state identity provided by the trace.

Catalog revision identifies the input catalog, not the codec implementation.
Caches of decoded results should additionally be namespaced by the deployed API
or adapter version, selected ABI/type/direction, and state/body identity. Keep
intentional user ABI overrides distinct from remotely cached catalog metadata.
