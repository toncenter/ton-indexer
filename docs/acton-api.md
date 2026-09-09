# Acton API

Native ABI endpoints under `/api/v3/acton`: identify a contract from its code
hash, list and run typed getters, and decode storage and message bodies.

Identification is bytecode-hash matching against a pinned catalog. It is not
source verification, and catalog links and known addresses are assertions, not
evidence.

Request and response schemas, limits and error codes are in the Swagger UI at
`/`. This page covers only what Swagger cannot express.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/contracts` | Paginated catalog metadata. |
| GET | `/abi` | Compiler ABIs for repeated `code_hash` parameters. |
| GET, POST | `/accounts` | Batch identification, optionally decoding storage. |
| GET | `/getMethods` | Getter metadata by address, code hash, or catalog ID. |
| POST | `/decode` | Decode a supplied storage or message BOC. |
| POST | `/runGetMethod` | Typed getter execution pinned to a seqno. |

## Catalog and bindings

`ton-index-go/index/acton/catalog/catalog.json` is a checked-in snapshot;
`*_gen.go` next to it are gitignored build artifacts. CMake, Docker and CI
generate them. For a direct `go build` or `go test`, run this first from
`ton-index-go`:

```sh
CGO_ENABLED=0 go generate ./index/acton/catalog
```

Provenance, licenses and how to re-pin the snapshot:
[`index/acton/catalog/README.md`](../ton-index-go/index/acton/catalog/README.md).

## Notes for explorer clients

**Batch account preloading.** Collect the visible addresses and issue one
`POST /accounts` instead of one request per hover. The response preserves
first-occurrence input order after canonical deduplication — key UI state by the
returned `address`, not by the friendly-address spelling the client sent. Request
`include_storage=true` only when the storage panel opens.

**Trace hovers.** Confirmed traces carry `trace.contract_info`. Prefer the
per-state link over the account-level list, so a code upgrade inside the trace
resolves to the implementation that was actually running:

```ts
const key = transaction.account_state_before?.contract_info_key
const info = key ? trace.contract_info?.by_code_hash[key] : undefined
```

`contract_info.accounts[address]` lists every code hash encountered in that
trace, sorted — not a timeline and not the account's current type. Do not fill a
missing historical entry from `/accounts` and present it as historical fact.

**Adapting an existing getter client.** `/runGetMethod` here is not a drop-in
replacement for the v2 provider:

- Send the displayed `code_hash` with the request. An upgrade between opening the
  form and executing then fails with a conflict instead of decoding new output
  against the browser's old ABI.
- Native stack numbers have type `int`; older providers emit `num`. Normalize
  recursively at the boundary.
- `success` is the execution outcome — TVM exits 0 and 1 are both successful.
  When `stack` is null or `stack_error` is set, show `raw_stack` and the error.
- Native values are not the TypeScript ABI shapes: `Cell<T>` is the decoded
  payload directly, dictionaries are typed key/value entry arrays, and unions are
  `{"$":"Type","value":...}`. Convert via the ABI type graph rather than
  flattening. Integers travel as decimal strings, including beyond 2^53.
- `/abi` returns 409 for an ambiguous hash and 413 for an oversized batch.
  Neither means the hash is unknown — do not fall back to the first bundled ABI.

**Caching.** `X-Acton-Catalog-Revision` identifies the input catalog, not the
codec implementation. Namespace decoded-result caches by that revision plus the
selected ABI, type, direction and state identity, and keep user ABI overrides
separate from cached catalog metadata.
