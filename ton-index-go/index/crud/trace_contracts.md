# Trace Contract Metadata

Confirmed `/api/v3/traces`, deprecated `/api/v3/events`, and
`/api/v3/pendingTraces` share the additive `Trace.contract_info` model.
`EnrichTraceContracts` runs after available transaction account states have been
attached. It does not query PostgreSQL, Kvrocks, or a node, parse code, or run get
methods. Catalog lookups and interface detection are indexed; each distinct code
hash, including misses, is looked up once per response.
Account states are copied before attaching trace-only links so shared pending
context states cannot leak those links into later non-trace responses.

## UI Lookup

- For an account hover, read `trace.contract_info.accounts[transaction.account]`.
  This is a sorted, distinct list of encountered code hashes, not a timeline or
  the account's latest type. Look up each hash in `contract_info.by_code_hash`.
- For a particular transaction state, use
  `transaction.account_state_before.contract_info_key` or
  `transaction.account_state_after.contract_info_key` in the same map.
- Keys use padded standard base64. The original `code_hash` field is unchanged,
  even if the input used hex or base64url. Repeated hashes share one summary per
  trace, including when several accounts use the same code.

For example, the following is a partial trace showing the exact-hash interface
match for Wallet V5R1. Catalog candidates, when available, are additional labels:

```json
{
  "transactions": {
    "tx-hash": {
      "account": "0:account-hash",
      "account_state_after": {
        "code_hash": "IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=",
        "contract_info_key": "IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8="
      }
    }
  },
  "contract_info": {
    "catalog_revision": "<embedded catalog revision>",
    "by_code_hash": {
      "IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=": {
        "match": "code_hash",
        "interfaces": ["wallet_v5r1"]
      }
    },
    "accounts": {
      "0:account-hash": ["IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8="]
    }
  }
}
```

`candidates` contains every catalog match, sorted by `id`, with only `id`,
`display_name`, and optional `links` (`kind`, `title`, `url`). It never includes
the ABI, storage bindings, method definitions, or known-address lists. A
`match: "code_hash"` value means bytecode-hash recognition, **not source
verification, identity, or trust**. Do not choose the first candidate when more
than one is returned.

## Unknown And Pending States

A valid but unrecognized code hash appears in the account list and hash map with
an explicit empty summary `{}`. It still has a per-state link. Missing, empty,
or malformed code hashes have no link and contribute no summary. If no valid
hashes are available, `contract_info` is omitted altogether. This also covers
large/incomplete traces with no loaded transactions.

The current pending writer supplies transaction state hashes but not their
per-state code hashes. Its root-only `root_account_code_hash` hint cannot identify
before/after versions safely and is deliberately not used. Pending traces with
only those placeholders omit contract metadata; if states are populated before
enrichment, they use the same scheme as confirmed traces. There is no latest-state
or address-based fallback. Full historical pending coverage requires upstream
per-state code hashes or state-hash-keyed state data.

Existing `address_book.interfaces` retains its current-account semantics and is
not copied into historical summaries. Only existing interfaces with an exact
code-hash match are returned here. No method-ID inference is attempted because
historical transaction states do not provide method data.

## Tests

Run the unit tests with `go test -race ./index/crud ./index/detect ./index/models`.
To also exercise public CRUD calls and their serialized route response models:

```sh
TON_INDEX_TRACE_INTEGRATION=1 go test -race ./index/crud ./index/detect ./index/models -count=1 -v
```

The integration test requires `initdb`, `pg_ctl`, and `redis-server` on `PATH`.
It starts and stops isolated local servers with temporary data; it never connects
to an existing database. It covers PostgreSQL historical state loading, the real
`KvrocksStore` code path against a Redis-compatible server, the legacy trace
loader, and populated pending traces. It does not test a RocksDB-backed Kvrocks
deployment or replica failover. Token metadata is disabled in this fixture;
address-book enrichment is enabled and deliberately describes a newer code
version than the transaction states.
