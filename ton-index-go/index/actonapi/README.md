# Acton API

All routes live under `/api/v3/acton`. They do not change the old marker or
`/api/v3/runGetMethod`. Existing request settings and error handling are injected
in `main.go`; the handler package has no database, models, or CGO dependency.

## Routes

| Method | Path | Request | Response |
| --- | --- | --- | --- |
| GET | `/contracts` | `limit` (default 100, maximum 1000), `offset` (default 0) | `{contracts: ContractSummary[], revision, total, limit, offset, transport_capabilities}` |
| GET | `/abi` | Repeated `code_hash`, 1-1000 values, hex or base64 | `{original_input_hash: ExtendedContractABI \| null}` |
| GET | `/accounts` | Repeated `address`, 1-1000 values; `include_storage=false` | `{accounts: Account[], revision}` |
| POST | `/accounts` | `{addresses: string[], include_storage?: boolean}` | Same as GET |
| GET | `/getMethods` | Exactly one `address`, `code_hash`, or `contract_type` | `{contracts: ContractMethods[], account?: Account, revision, transport_capabilities}` |
| POST | `/decode` | `{contract_type?: string, code_hash?: string, direction: string, body: string}` | `{catalog_id, direction, type, decoded, revision}` |
| POST | `/runGetMethod` | `{address, method, args?: object, stack?: StackValue[], seqno?: int32, contract_type?: string, code_hash?: string}` | `RunResponse`, described below |

`contract_type` means a catalog ID, not an interface name. Decode requires exactly
one of `contract_type` and `code_hash`. Getters can infer it from pinned account
code, or accept one explicit selector that must match that same code.

`ContractSummary` contains `catalog_id`, `display_name`, `code_hashes`,
`known_addresses`, `links: [{kind, title, url}]`,
`links_provenance: "catalog_asserted"`, and `source_verified: false`.
`ExtendedContractABI` adds the full, unmodified `compiler_abi` object.
Known addresses and source links are catalog assertions, not verification of
deployed source or grounds for overriding a code-hash mismatch.

ABI lookup preserves each exact input spelling as a map key. Unknown hashes map
to JSON null. Multiple catalog contracts for one hash cause HTTP 409 rather than
first-match selection. The revision is also returned in
`X-Acton-Catalog-Revision`. Contract/getter listings retain multiple matches.
Whitespace in hash selectors is rejected. Metadata endpoints enforce a
conservative aggregate 8 MiB response budget **before** serialization, counting
every distinct spelling of a hash. Reduce the batch/page size after HTTP 413.
The budget allows for JSON escaping and can reject a response whose compact
form would be smaller than 8 MiB. Single oversized ABIs are also rejected.

`ContractMethods` adds `get_methods: [{name, id, parameters, return,
description?, unsupported?}]` to `ExtendedContractABI`. Each parameter contains
`name`, `type`, and optional `default`. Type references are
`{ty_idx, name}`; their definitions are in the included compiler ABI type table.
Listing contracts, accounts, ABIs, or getters never executes getters.
Address-selected getter listings currently require exact catalog code matches.
Public-interface hints alone do not select a generic TEP ABI for execution or
storage decoding; explicit getter selection must still match pinned code.

## Account Snapshots

Addresses are converted to uppercase raw workchain/hash form and deduplicated
in first-occurrence order. The limit is checked before deduplication. A malformed
address rejects the request with HTTP 422. There is exactly one
`db.QueryAccountStates` call per request, with `NoAddressBook` and `NoMetadata`
forced true and `IncludeBOC` equal to `include_storage`. This uses the existing
PostgreSQL/Kvrocks abstraction and does not perform per-address enrichment.
Friendly addresses accept both standard and URL-safe base64 with exactly 36
decoded bytes, valid CRC16, and tags `0x11`, `0x51`, `0x91`, or `0xd1`. Raw
standard addresses accept signed 8-bit workchains (`-128..127`).

Each account contains:

```json
{
  "address": "0:...",
  "status": "identified",
  "account_status": "active",
  "account_state_hash": "...",
  "code_hash": "...",
  "data_hash": "...",
  "last_transaction_hash": "...",
  "last_transaction_lt": "9007199254740993",
  "pinning": "indexed_account_state",
  "proof_verified": false,
  "types": [
    {"type": "catalog-id", "provenance": "exact_code_hash", "contract": {}},
    {"type": "interface-name", "provenance": "public_interface"}
  ]
}
```

`contract` above is a full `ContractSummary`. Account `status` is `identified`,
`unknown` (row exists but no identification), `not_found` (no indexed row), or
`error` (row-specific failure, with `error` text). A batch-wide database failure
uses the existing HTTP error path, not misleading `not_found` entries.
Snapshot fields can be null when unavailable. This is the latest **indexed**
state of each account, not a claim that every account was read at one masterchain
height. State/data hashes and the last transaction identify the returned state.

With `include_storage=true`, `storage` is a map keyed by exact-match catalog ID,
with values `{type, decoded, error?}`. Storage failures do not erase the account
or its identification. Public-interface hints never authorize storage decoding.
Current storage is decoded with the advertised `Storage` binding; the runtime's
untyped `DecodeStorage` fallback is intentionally not used because it can select
deployment storage without reporting which type matched. Deployment storage
remains explicitly available through `/decode`.

Storage-enabled batches allow **at most 8 canonical addresses** (validated
before the one database query) and at most 8 native decode operations. Combined
code/data BOC bytes returned by the store plus conservatively estimated decoded
JSON and metadata share an **8 MiB aggregate budget**. Oversized BOC batches fail
before native decoding; decoded values are budgeted before retention and JSON
serialization, counting repeated references repeatedly and bounding traversal.
The native runtime's per-call Context, cell and item limits still apply while
constructing each value. Budget violations return HTTP 413; overlarge storage
address batches return 422. Use smaller storage batches. Ordinary hover batches
retain the 1000-address limit and avoid BOCs entirely. The database abstraction
materializes its bounded row batch before the adapter checks BOC lengths; this
is not a pre-read limit on the database connection or a strict process-RSS cap.

## Native Decoding

`body` is a base64 BOC. Directions are `storage`, `deployment_storage`,
`incoming_messages`, `incoming_external`, `outgoing_messages`, and
`emitted_events`, subject to bindings present in the selected contract.
Decoders/encoders are generated native Go functions; compiler ABI JSON is not
interpreted by the request handlers. Unsupported native bindings produce
explicit errors. `/decode` only interprets supplied bytes; it does not claim
they came from a blockchain account or transaction.

## Getter Execution

`method` is a getter name, a signed int32 TVM ID, or its decimal string form.
The selected catalog getter's numeric ID is always sent upstream. Unknown or
ambiguous methods are rejected, including different names with the same TVM ID.
`args` and `stack` are mutually exclusive even when empty or null. `args` must be
an object, `stack` must be an array. If both are absent, empty named arguments
are passed to the native encoder, which validates required/default parameters.
Unknown named arguments are rejected. JSON is decoded with `UseNumber`.

Execution follows these steps:

1. If `seqno` is absent, call configured v2 `getMasterchainInfo` once.
2. GET `getAddressInformation?address=...&seqno=N` and hash its code/data BOCs.
3. Select the ABI by that code hash or a library root's implementation hash,
   never by a latest indexed account read.
4. POST `runGetMethodStd` with the same `seqno=N`, address, numeric method ID,
   and standard typed recursive stack.

Explicit historical seqnos must be positive and skip step 1. Zero and negative
values are rejected with HTTP 422 before upstream access. An explicit ABI that disagrees with the
code at that height is HTTP 409, even if it matches today's code. Failed block
selection or unsupported standard transport does not silently retry at latest
or downgrade to the legacy endpoint. Only the configured server-side
`V2Endpoint`/`V2ApiKey` are used; clients cannot supply upstream URLs.
Only `runGetMethodStd` is used. There is no legacy transport option and no errors
or unsupported results trigger automatic endpoint switching.

Snapshot code/data and opaque stack cells use the bounded `DecodeOpaqueBOC`
parser, which accepts validated exotic cells. For a library-reference code root,
`snapshot.code_hash` remains the hash of the **actual code cell** and
`snapshot.implementation_hash` contains the embedded library implementation
hash, following ActonScan's `codeCell.ts` lookup rule. Both hashes participate in
catalog selection; different matches remain ambiguous unless explicitly selected.
An explicit `code_hash` can pin either identity, but must match this snapshot.
An ABI selected only through the embedded hash reports
`identification: "library_reference"`, not `exact_code_hash`. This identifies
the referenced code; it is not proof that the upstream executed that library.
Batch indexed accounts do not fetch code BOCs solely to discover library hashes.

This is **upstream seqno pinning**, not proof verification. The standard getter
result does not include an account hash/block selector to independently verify
execution. Responses say `snapshot.pinning: "upstream_seqno"` and
`snapshot.proof_verified: false`, trusting the configured v2 server to honor
`seqno`. `getAddressInformation` does not supply an account-state hash, so this
field stays null. Code/data hashes and last transaction fields are returned;
`block_id` is preserved when supplied and may identify a shard block.

`RunResponse` contains `catalog_id`, `method`, `identification`
(`exact_code_hash` or `library_reference`), `snapshot`, `revision`, and these
independent result fields:

| Field | Meaning |
| --- | --- |
| `exit_code` | Raw signed VM exit code |
| `gas_used` | Exact decimal string |
| `transport` | Always `standard` |
| `raw_stack` | Untouched standard upstream stack JSON |
| `stack` | Native `[{type, value}]` stack using `int`, not `num`; null if conversion failed |
| `stack_error` | Optional wire conversion error |
| `success` | True for TVM exits 0 or 1 |
| `decoded` | Native decoded value, or null |
| `decode_error` | Optional ABI, wire, or VM error |

A VM failure is HTTP 200 with these raw results, not a transport error. ABI
decode failure also remains HTTP 200 without losing gas, stack, or exit code.

## Transport Limits

`/contracts` and `/getMethods` expose `transport_capabilities` with the standard
endpoint, supported input/output types, accepted aliases, and concrete warnings.
Native codec availability is **not** a promise that an upstream can execute every
getter. Capabilities describe the adapter's wire format, not a live backend probe.

Supported native stack types are `int`, `cell`, `slice`, `tuple`, and `null`.
Public `num` is accepted as an alias and normalized to `int` on every
ingress/egress boundary. `list` is an alias for a flattened Lisp list, normalized
to tuple pairs ending in null. Empty list is null, not an empty tuple. Tuples
remain tuples. Tuple/list values are arrays of typed entries; numbers can be exact JSON
integers, decimal strings, or `-0x...`/`0x...` strings. Values are range checked
to `[-2^256, 2^256-1]` and converted to decimal on the standard wire.
Fractions, exponents, floating-point values and unsupported tags are rejected.

Standard mode follows native Tonlib: `tvm.stackEntryList` represents a flattened
Lisp list, and an empty `tvm.list` losslessly represents null, including nullable
arguments and list terminators. Some Rust/localnet adapters deviate from that
protocol and interpret lists as tuples; those backends cannot execute standard
null arguments correctly and must be fixed upstream. Public TONcenter's standard
null and flattened-list behavior has been verified. No `tvm.stackEntryUnsupported` is ever guessed
to be null: raw output is retained with a decoding error. This matters for
`get_plugin_list` on upstreams that erase null list tails as Unsupported.

Builder inputs are explicitly rejected with HTTP 422; NaN and continuation
values are also unsupported. Public legacy `runGetMethod` is not a lossless
substitute: it rejects null/builder inputs and can lose slice type information.
Full compiler ABIs are still served even when the transport or backend cannot
execute a binding. No generic supported-getter count is advertised.

Requests are limited to 1 MiB. Recursive stacks allow at most 1024 total entries,
32 nesting levels (including expanded Lisp pairs), 255 tuple members, and 1 MiB
of encoded BOC strings. The runtime's BOC safety limits also apply. An isolated,
reusable `net/http` pool checks status codes and limits upstream bodies to 4 MiB
while reading, including unknown-length/chunked responses. Non-2xx status is an
error even with `ok: true`; redirects are not followed. URLs, API keys, and
upstream error text are not included in error messages. A single deadline,
bounded to 3 seconds using request settings, covers masterchain discovery, state
loading, execution and body reads. The old proxy and its pool are unchanged.
Client validation uses 422, selection conflicts 409, missing catalog entries
404, configuration unavailability 503, upstream failures 502, and timeout 504.

Wire contracts were checked against:

- `acton/crates/ton-api/src/toncenter/v2/requests.rs`
- `acton/crates/tvm-ffi/src/json_stack.rs`
- `acton/crates/ton-localnet/src/server/handlers/toncenter_v2.rs`
- `acton/crates/ton-localnet/src/api/toncenter_v2.rs`
- [TonlibClient.cpp at 9a42919dce98971a6653d326347efcce40bad026](https://github.com/ton-blockchain/ton/blob/9a42919dce98971a6653d326347efcce40bad026/tonlib/tonlib/TonlibClient.cpp#L4896-L5026),
  `to_tonlib_api` / `from_tonlib_api` for actual Lisp-list wire semantics
- `acton/packages/transaction-ui/src/lib/codeCell.ts`
- [TON address formats](https://docs.ton.org/llms/foundations/addresses/formats/content.md)
- [TON TVM exit codes](https://docs.ton.org/llms/tvm/exit-codes/content.md), which
  defines exits 0 and 1 as successful compute termination.

## Tests

Run from `ton-index-go`:

```sh
CGO_ENABLED=0 go test ./index/actonapi -count=1
go test ./index/actonapi ./index . -count=1
go test -race ./index/actonapi ./index . -count=1
go vet ./index/actonapi ./index .
# Optional public read-only smoke, with a 3s deadline per state/execution chain:
ACTON_LIVE_SMOKE=1 go test ./index -run TestActonLiveReadOnlySmoke -v -count=1
```

Handler tests inject state queries and executors. Integration tests use actual
`coffee.CoffeeStakingMaster`, `system.Elector`, and WalletV4r2 catalog codecs,
including integer arguments/results and Lisp lists. Transport tests use an
in-memory mock upstream, with no external network or state changes. Main build
also requires the generated `index/acton/catalog` package. Swagger annotations
are on the exported handlers for `swag --parseDependency`; generated Swagger
files are intentionally left to the parent task.
