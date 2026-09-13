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
| GET | `/contracts` | The whole catalog, or selected entries with their compiler ABIs. |
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

**Batch account preloading.** Account identification is not a separate endpoint.
`GET /api/v3/accountStates` returns `code_book` alongside the accounts it already
served, and `include_storage=true` additionally decodes each data cell with the
entry that code book names first, reporting `storage` or `storage_error` per
account. Ask for it only when the storage panel opens: the batch shares one decode
budget, so one that demands too much work is refused with 413 rather than served
slowly.

**Trace hovers.** Trace and transaction responses carry a top-level `code_book`,
keyed by the code hash exactly as the account states in the same response spell
it. Index it per state rather than per account, so a code upgrade inside the
trace resolves to the implementation that was actually running:

```ts
const hash = transaction.account_state_before?.code_hash
const code = hash ? response.code_book?.[hash] : undefined
```

`code_book` describes code, never an account: a hash appears because it was seen
somewhere in this response, which says nothing about which account currently runs
it. `contracts` is ordered most specific first. A hash absent from the book is one
neither the catalog nor the interface table recognizes.

**Fetching the catalog.** `/contracts` without a selector is the entire catalog:
identity plus getters with their types rendered as names, about 400 KB for 288
entries and no type tables. No getter in the catalog takes a structural
parameter, so this is enough to build a getter form — fetch it once at start-up
and resolve locally. Add `code_hash` or `catalog_id` only to obtain a contract's
full `abi`, which is what decoding a message or storage cell by hand needs. An
ambiguous hash returns every candidate, most specific first.

**Adapting an existing getter client.** `/runGetMethod` here is not a drop-in
replacement for the v2 provider:

- The request names an address, a getter and its arguments, and selects no ABI.
  The entry used is the one declaring that getter for the code running at the
  execution seqno, and it comes back as `catalog_id`. Nothing is guessed by doing
  so: 22 of the catalog's 333 hashes are claimed by two entries, jetton wallets
  and NFT items among them, but 21 of those pairs are one contract entered under
  two vendor names, and the remaining pair agrees on every getter it shares. Pin
  `seqno` when the result must match the state the form was rendered against.
- A raw TVM stack is not accepted here; `POST /api/v3/runGetMethod` already takes
  one and is unchanged.
- Native stack numbers have type `int`; older providers emit `num`. Normalize
  recursively at the boundary.
- `success` is the execution outcome — TVM exits 0 and 1 are both successful.
  When `stack` is null or `stack_error` is set, show `raw_stack` and the error.
- The standard wire schema carries `int`, `cell`, `slice`, `tuple` and `null`.
  Builder, NaN and continuation values cannot be sent as arguments, and an
  upstream that returns one serializes it as `tvm.stackEntryUnsupported`, which
  cannot be decoded losslessly — those results arrive as `raw_stack` plus
  `stack_error`. A getter having a native codec does not mean the transport can
  carry its arguments or results.
- Native values are not the TypeScript ABI shapes: `Cell<T>` is the decoded
  payload directly, dictionaries are typed key/value entry arrays, and unions are
  `{"$":"Type","value":...}`. Convert via the ABI type graph rather than
  flattening. Integers travel as decimal strings, including beyond 2^53.

**Caching.** `/contracts` is a pure function of the pinned catalog and the
request, so its `ETag` is a strong validator: re-fetch with `If-None-Match` and
an unchanged catalog answers 304. `X-Acton-Catalog-Revision` identifies the input
catalog, not the codec implementation. Namespace decoded-result caches by that
revision plus the selected ABI, type, direction and state identity, and keep user
ABI overrides separate from cached catalog metadata.
