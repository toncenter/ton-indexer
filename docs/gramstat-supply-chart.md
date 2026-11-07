# Gramstat mainnet supply chart: implementation contract

Use this rule when implementing the API, aggregates or frontend supply graph.
`blocks.global_balance` remains the exact value stored by the network; derive
the corrected chart value separately.

## Required formula and exact v17 boundary

For a mainnet masterchain block with sequence number `N`:

```text
B(N) = SUM(blocks.burned for masterchain blocks with seqno <= N)

N < 103190382:   total_supply(N) = global_balance(N) - B(N)
N >= 103190382:  total_supply(N) = global_balance(N)
```

**The last block requiring the correction is `103190381`. The first block
using the stored balance directly is `103190382`.** This is an inclusive
sequence-number boundary, not a timestamp-only comparison.

Before global version 17, the stored balance accumulated issuance without
deducting protocol burning. The full calculator reconstructs existing funds
from accounts, unprocessed outbound messages, dispatch queues and unrecovered
validator fees. With v17 the network writes the calculated balance and accounts
for burning itself, so subtracting `B(N)` again would double-count the burn.
Funds in queues already belong to supply; no queue balance correction is added.

These block numbers are **mainnet-specific**. Other networks require their own
activation boundaries.

## Vertical lines / chart annotations

| Event | Mainnet MC | UTC | Unix seconds |
|---|---:|---|---:|
| **Protocol burning begins: first nonzero `ValueFlow.burned.grams`** | **30410593** | **2023-06-17 06:33:26** | **1686983606** |
| Config parameter 8 changes from global version 16 to 17 (key block) | 103190381 | 2026-11-03 21:25:07 | 1793741107 |
| **First block applying v17 balance accounting; stop subtracting accumulated burn** | **103190382** | **2026-11-03 21:25:08** | **1793741108** |

Draw the requested **burn-start line at MC `30410593` / `1686983606`**, labelled
for example `Protocol fee burning starts`. Its own burn is **113178875 nanotons**
(`0.113178875 TON`), and is included in `B(30410593)`.

The v17 configuration-changing block still uses the previous state's rules.
Its successor is the accounting cutoff. If drawing a second line specifically
for the supply methodology change, use **MC `103190382`**, not the key block.

Machine-readable annotation constants:

```json
[
  {"id":"protocol_burning_start","mc_seqno":30410593,"gen_utime":1686983606},
  {"id":"global_balance_v17_accounting","mc_seqno":103190382,"gen_utime":1793741108}
]
```

## Aggregation and completeness requirements

1. Use **masterchain rows only** (`workchain = -1`). `ValueFlow.burned.grams`
   already includes fee burning and protocol-blackhole burning; do not derive
   it as half of a fee total or subtract those components separately.
2. `B(N)` is the **full accumulated burn through N inclusive**, including N's
   own burn. It does not reset at a day, month, chart viewport or pagination
   boundary. Compute the prefix before applying the displayed date filter, or
   use a verified opening cumulative total plus subsequent block burns.
3. Deduplicate ClickHouse block rows (`FINAL` or an equivalent canonical-row
   selection) before summing. PostgreSQL uses its unique block keys.
4. Verify that the required MC prefix has no missing blocks and no NULL `burned`.
   A missing row/amount is unknown, not zero. Native legacy ValueFlow records
   decode to a genuine zero. The mainnet full run starts at MC 3, before burning;
   its complete prefix through the first burn provides the zero opening baseline.
   For `N < 30410593`, the verified mainnet pre-burning baseline is `B(N) = 0`.
5. If `global_balance(N)` is NULL, the chart value is unavailable. In the
   current full run it begins at **MC 7409467**; earlier NULL values are not
   filled by this chart rule. A later incomplete burn prefix makes the corrected
   pre-v17 value unavailable as well. At/after v17, the stored value does not
   depend on completeness of the old burn prefix.
6. Preserve exact integer nanotons / decimal arithmetic throughout aggregation
   and transport. Do not pass these integers through JavaScript `Number`;
   use decimal strings / `BigInt`. Divide by `10^9` only for display.
7. Derive the corrected **per-MC** value first, then select the last snapshot
   in each hour/day by `(gen_utime, seqno)`. Do not sum supply snapshots or mix
   a balance from one block with a burn prefix ending at another. A NULL final
   snapshot must not silently fall back to an older non-NULL point.
8. Preserve the raw field in `blocks`; the correction belongs to chart/API
   derivation. The former `gramstat.total_supply` table is a frozen validation
   archive, not the input series for the new graph.

## Validation already performed

On **2026-11-07**, the independent full-state calculator matched the stored
balance exactly on **13 pre-burning snapshots**, MC 3300000 through 29574969.
Heavy outbound queues included **986269, 721013, 618672, 338662 and 141865**
scanned entries. Seven available ClickHouse balances also matched chain state;
six older snapshots with NULL CH values were compared directly against NodeDB state.

Five additional snapshots, all with nonzero per-block burn, verified the chart
correction exactly:

| MC | Accumulated burn, nanotons | `global_balance - accumulated_burn - calculator` |
|---:|---:|---:|
| 30410593 | 113178875 | 0 |
| 30635739 | 3665511787033 | 0 |
| 30860884 | 7523697125008 | 0 |
| 31086029 | 12365379104415 | 0 |
| 31311177 | 17070178634917 | 0 |

The deduplicated prefix **MC 3..31311275** contained **31311273 MC rows** with
**zero NULL burned values**; the first positive burn was MC 30410593.
At MC 31311177, exact nanotons:

```text
5090216486296870767 - 17070178634917 = 5090199416118235850
stored global_balance - cumulative burn = full calculator
```

**Remaining validation:** reconcile the cumulative-burn correction across
MC **103190381 -> 103190382** once full-history indexing reaches it, accounting
for that block's issuance and burning. The cutoff is established from on-chain
configuration and validator rules; the June–July 2023 comparisons above do not
claim a completed numerical reconciliation at the v17 transition.

## Evidence and source semantics

- Mainnet config parameter 8: [MC 103190380: v16](https://toncenter.com/api/v2/getConfigParam?config_id=8&seqno=103190380),
  [MC 103190381: v17](https://toncenter.com/api/v2/getConfigParam?config_id=8&seqno=103190381).
- Block timestamps: [burn-start block](https://toncenter.com/api/v2/getBlockHeader?workchain=-1&shard=-9223372036854775808&seqno=30410593),
  [first v17 accounting block](https://toncenter.com/api/v2/getBlockHeader?workchain=-1&shard=-9223372036854775808&seqno=103190382).
- [Original fee-burning implementation: stored balance only adds issuance](https://github.com/ton-blockchain/ton/blob/ef64b92f087a41c5dc9c74085bdab152856039a6/validator/impl/collator.cpp#L3279-L3285).
- [v17 change: calculated previous balance plus issuance minus burned](https://github.com/ton-blockchain/ton/blob/2c6d6ff0cf75c1ed83a5bd203b11d97afb7100fd/validator/impl/collator.cpp#L5326-L5337).
- Full evidence is retained on the indexer host under `/opt/gramstat-full/`:
  `mainnet-v17-activation.json`,
  `balance-parity-combined-result.json`,
  `burn-correction-20261107T205327Z-burn-result.json`, and the probe manifests/logs
  in `balance-parity/`. Calculator TON revision: `31f1bdf1f5d3eb027cc423559203b496036027ad`.

See [Gramstat storage and indexing](gramstat.md) for field definitions and resume
semantics.
