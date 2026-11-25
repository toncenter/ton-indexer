# Account types

`gramstat-aggregate --handlers account_types` prepares an exclusive account-type
tree from a completed [code-hash observation](gramstat-code-hashes.md). The daemon
runs it every ten minutes, after `account_code_hashes`, with its own two-minute
deadline. It reads ClickHouse only.

## Population and evidence

The denominator is the complete saved `latest-code-hash-indexes` population,
including the long tail. The source parser supplies code only for active states.
Active accounts without code are outside this population; `uninit`, `frozen` and
`nonexist` are outside the code-hash indexes. Observation start/end and the MC
range are provenance, not an exact state at a single block.

Every code hash has exactly one terminal classification:

1. A reviewed rule for an exact ABI code-hash match.
2. Otherwise, a standard-interface hypothesis from static method IDs.
3. Otherwise, Unknown, distinguishing a method record without a supported
   interface from absence of a static record.

The source `mainnet_gramstat_full.contract_methods` is sparse. It stores dispatcher
IDs, not successful getter calls, return types or the parser's completeness flag.
Absence of a method/record cannot establish absence of an interface.

The four inference rules are:

| Role | All required method IDs |
| --- | --- |
| Jetton wallet | 97026 (`get_wallet_data`) |
| Jetton master | 106029 (`get_jetton_data`), 103289 (`get_wallet_address`) |
| NFT item | 102351 (`get_nft_data`) |
| NFT collection | 102491, 92067, 68445 (the three TEP-62 collection getters) |

Multiple independent roles form an exact, sorted combination under Mixed.
SBT/DNS getters are additional NFT traits, not extra independent roles. A wallet
used by a DEX does not become a DEX pool. LP wallets and position NFTs remain
specializations of their token roles; an actual pool that also acts as a token
master/collection is Mixed.

## Shared ABI registry and rules

[`ton-index-go/index/acton/catalog`](../ton-index-go/index/acton/catalog/README.md)
is shared with future main-API consumers. Preparation downloads the release in
`catalog.lock`, verifies SHA-256, and invokes the pinned `go tool tolk-abi-to-go`.
Both downloaded JSON and native generated bindings are ignored by Git. The
binary includes the catalog and does not download ABI data at runtime.

The initial catalog is `abis v0.1.3`, SHA-256
`8397e3496291590590e16676ac3ab99a32167214dea7491ea315e1b34056d203`.
All 299 entries and 346 hashes are retained. Unsupported codec roots still
provide metadata; these codecs are not executed by this handler.

[`account-type-rules.json`](../ton-index-go/internal/gramstat/handlers/accounttypes/account-type-rules.json)
assigns roles and paths to stable catalog IDs. Validation rejects missing rules,
unknown IDs, duplicate assignments and conflicting classifications of shared
code. The generic `cron.Cron` entry supplements the more specific STON.fi escrow
entry where their hashes overlap; standalone Cron code retains its automation
classification. Aliases remain visible in implementation details.

Names and signatures come from compiler ABI metadata, including punctuated
names such as `processed?`. A global numeric ID can have multiple possible names
(`76407`: `version` / `is_plugin_installed`). Known-contract declarations and
static observations are kept separate. Matching an implementation is not proof
that an account belongs to its originating project.

Changes to role semantics require a reviewed rules/version update. Publication
records contain both `abi_revision` and `classification_version` (including the
rules digest). Existing historical publications remain unchanged.

## Storage and atomic publication

Writer-owned DDL:
[`schema.sql`](../ton-index-go/internal/gramstat/handlers/accounttypes/schema.sql).

- `account_type_features`: 64 prefix maps of code-to-leaf assignments and method
  IDs, keyed by a fresh feature UUID. Only hashes with metadata or a known ABI
  need an assignment. All other source hashes use the explicit Unknown leaf.
- `account_type_feature_sets`: completion metadata and compact ABI descriptions,
  written after read-back verification of all 64 feature rows.
- `account_type_snapshots`: a small complete tree, source count UUID, feature
  UUID, observation interval, totals and classification provenance. Written last.

A deterministic digest allows an unchanged feature set to be reused across new
count snapshots. Each failed attempt uses a new staging UUID. Incomplete stages
never replace a published generation. Feature rows are inserted in eight bounded
batches to avoid excessive tiny parts.

The SQL flattens the sparse assignment maps before joining them to counts.
It then groups by leaf in ClickHouse: the aggregator receives only leaf totals,
not all ~457k count pairs. Joining an entire prefix map onto every account-code
row would multiply memory use, so both publisher and reader join flat hash/leaf
columns instead.

The publisher checks that leaf account/hash totals equal the captured count
snapshot and constructs additive parent nodes with separate known/inferred/
unknown subtotals. There is no top-N truncation in publication and no automatic
TTL or deletion.

## Deployment

Apply DDL administratively. Grant:

```sql
GRANT SELECT ON mainnet_gramstat_full.contract_methods TO gramstat_aggregate;
GRANT SELECT, INSERT ON gramstat.account_type_features TO gramstat_aggregate;
GRANT SELECT, INSERT ON gramstat.account_type_feature_sets TO gramstat_aggregate;
GRANT SELECT, INSERT ON gramstat.account_type_snapshots TO gramstat_aggregate;
GRANT SELECT ON gramstat.account_type_features TO gramstat_api;
GRANT SELECT ON gramstat.account_type_feature_sets TO gramstat_api;
GRANT SELECT ON gramstat.account_type_snapshots TO gramstat_api;
```

Add `account_types` after `account_code_hashes` in the daemon handler list.
The existing `--code-hashes-interval` and `--code-hashes-timeout` apply to both;
the scheduler dependency preserves their order without coupling their failures.

## Reader

Gramstat's `/api/v1/accounts/types` serves the prepared tree. All drill-down
requests capture one `publicationId`: counts, classification and observed methods
therefore remain consistent across later publications and service restarts.
Pagination/search never changes a parent total. Details show both source hash
encodings, ABI aliases, declared signatures, observed IDs and their evidence.

The UI is a latest-observation view independent of the global time range, with
a gray UTC timestamp. Accounts/types use a desktop 2:1 row and stack on narrow
screens. Automatic refresh waits while the user explores a captured tree.

## Verification notes

Publication tests use real `clickhouse local` fixtures, including an acknowledged
incomplete feature batch, fresh retry UUIDs, and reuse of completed generations.
Classification tests cover all catalog IDs, aliases, exact role combinations and
integer totals above JavaScript's safe range.

The first production observation covered 457,044 hashes / 191,353,286 accounts
and produced 378 tree nodes. All parent and evidence sums agreed. The initial
publisher used about 117 MiB peak RSS. The reader's flattened hash join used
about 59 MiB of ClickHouse query memory and roughly 60 ms for an NFT inference
member query on that observation.

Local wider-repository verification is distinct from these tests: CRUD/streaming
test binaries require the native `ton-marker-core` library, absent from this
macOS build tree. The metadata-cache suite also encounters the existing
`AccountAddress.UnmarshalText` call against the current string-based model.
The new handler/catalog, Gramstat API, JS tests and browser drill-down checks
were verified independently.
