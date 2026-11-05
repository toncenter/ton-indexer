# Validator round results

The indexer records the final locked principal and accumulated reward of each
validator election when the Elector releases its stakes. These values are stored
separately in `validator_round_results`.

An election selects the validator set for a validation round. Its stakes remain
locked for a period after validation ends, during which approved complaints can
reduce the principal. The result is captured immediately before the successful
tick/tock transaction that releases the stakes, including changes earlier in the
same masterchain block.

## Fields

| Column | Meaning |
| --- | --- |
| `election_id` | Election identifier; primary key in PostgreSQL and replacement key in ClickHouse |
| `vset_hash` | Validator-set cell hash, hexadecimal |
| `total_stake` | Remaining principal immediately before release, in integer nanoGRAM |
| `bonuses` | Accumulated election reward immediately before release, in integer nanoGRAM |
| `unfreeze_mc_seqno` | Masterchain block containing the release |
| `unfreeze_utime` | Timestamp of the release transaction |
| `unfreeze_tx_lt` | Logical time of the release transaction |
| `unfreeze_tx_hash` | Release transaction hash, base64 |

`total_stake + bonuses` represents **stake and accumulated reward**. The two
amounts use `numeric(38,0)` in PostgreSQL and `Decimal(38,0)` in ClickHouse.
One GRAM equals 1,000,000,000 nanoGRAM.

The amount is recorded before distribution. Actual credits to holders can differ
because of banned positions and integer rounding of individual rewards. Election
cancellation on configuration rejection is excluded from completed-round results.

Existing PostgreSQL tables retain their meanings:

* `validator_elections.total_stake`: accepted applications, including candidates
  not selected and any excess stake returned during selection.
* `validator_cycles.total_stake`: maximum observed selected principal for the
  round. It is used as the selected capital when the full history is indexed.
* `validator_round_results.total_stake`: final remaining principal after fines.

The ClickHouse indexer also stores the existing validator source data. Its cycle
and member tables retain key-block observations; reduce them to maximum observed
stakes before joining results. See [ClickHouse validator tables](validator-clickhouse.md)
for keys, nullable enrichment and read queries.

Join cycles to results by `election_id`. A missing result means the release has
not been indexed; it does not mean zero stake or zero rewards. An open round has
no final result yet.

## Collection

The collector compares Elector data at block boundaries. For changed data it
calls `past_election_ids` to detect removed elections. Blocks without removals
require no additional transaction replay. The detector uses getters rather than
depending on the Elector's persistent-storage layout.

For a removal, the collector identifies the successful tick/tock transition and
reads `total_stake` and `bonuses` from `past_elections` on its preceding state.
Intermediate Account cells are loaded from CellDB when available. Missing
intermediate states are reconstructed with the transaction emulator using only
the necessary Elector transactions in that block. The emulator verifies the
original transaction and resulting account hashes. Replay uses the configuration
from the preceding masterchain state and the block's original random seed.

The getter results contain frozen-position and complaint dictionaries as cell
references. Reading the two election totals does not enumerate these dictionaries.
Both nested and flattened historical `past_elections` tuple formats are supported.

Failure to read or reconstruct a required state fails block processing so the
scheduler can retry. Results are assembled before any are submitted to storage.

## Storage and operation

PostgreSQL migration **1.3.2** creates `public.validator_round_results`. An
installation that already applied this migration can use
`--rerun-last-migration` to apply its additional DDL. Results commit in the same
transaction as blocks, including when current-state storage uses Kvrocks.

The ClickHouse indexer creates `validator_round_results` at startup and inserts
results before blocks. Read with `FINAL` to deduplicate repeated indexing.
PostgreSQL uses an upsert on `election_id`. Corrected replay replaces both amounts;
they are not merged with `GREATEST`.

Collection is enabled during ordinary indexing, independently of `--gramstat`.
Historical results require indexing the blocks containing releases. Existing
block rows do not establish coverage: use an explicit `--from N --to M --force`
range to reprocess already indexed blocks. Processing and storage do not depend
on a cycle row having arrived first, so out-of-order block completion is supported.
