# Fragment Stars payment/refund audit

Read-only investigation on 2026-11-09, TON mainnet; all timestamps UTC.
This report does **not** change the published purchase definition or apply a
price filter. Price-qualified payments would still not prove off-chain delivery.

## Recipient and outgoing history

Investigated Stars recipient:
`0:852443F8599FE6A5DA34FE43049AC4E0BEB3071BB2BFB56635EA9421287C283A`.

The indexed `messages` table contains **1,279 outgoing messages** from this
address, dated 2024-07-12 20:59:20 through 2026-11-02 06:15:02. The raw
transactions table had advanced to 2026-11-09 16:10:20 during the investigation.
Only `direction='out'` is counted, avoiding the inbound copy of each message.

The outgoing messages consist of:

| Count | Observed behavior |
| ---: | --- |
| 1,254 | GRAM transfers to one dominant recipient, `0:E6F3D8824F46B1EFBAB9AFC684793428C55FED69B46A15A49BE69A29BC49E530`, mostly large round amounts |
| 16 | Protocol bounces while the recipient account had no code |
| 4 | Jetton-transfer requests carrying 0.05 GRAM gas to the recipient's jetton wallet; classified as USDT transfers of 10, 10, 10 and 15,375,492.704253 USDT to `0:8C397C43F9FF0B49659B5D0A302B1A93AF7CCC63E5F5C0C4F25A9DC1F8B47AB3` |
| 2 | Self-transfers of 0.68 and 0.75 GRAM; the latter comment is `test quantity 0.75` |
| 3 | 0.01 and 150,000 GRAM to one other recipient, and 100,000 GRAM to another |

The large transfers and small-then-large sequences are consistent with treasury
operations, not a per-customer refund mechanism. Their business purpose is an
inference; there is no order/refund linkage in their comments. No application-level
Stars-underpayment refund was identified from the investigated receiving address.

### The sixteen bounces are not a purchase refund policy

All 16 occurred on **2024-07-12, 20:59:20–21:47:00**. Every corresponding receiver
transaction has `orig_status=uninit`, `compute_skipped=true`,
`skipped_reason=no_state`, and `bounce=ok`. Each returned the incoming value minus
**400,000 nanograms = 0.0004 GRAM**, for example:

| Incoming GRAM | Bounced GRAM |
| ---: | ---: |
| 0.1025 | 0.1021 |
| 5.1384 | 5.1380 |
| 10.2746 | 10.2742 |

The second example carried `2500 Telegram Stars` and is already classified as a
failed `ton_transfer`, excluded by the existing `success=true` filter. Its trace:
`b7aW71MS/fxRoBYVX1tZqGVIIBuUBhmG1pFlNkPUY40=`.

There are no later bounced outgoing messages from this recipient in the inspected
history. The 0.0004 deduction is a protocol/forwarding cost in these transactions,
**not** evidence of a Fragment refund threshold of 0.1 or 1 GRAM.

## Deliberately implausible quantities

On **2025-08-23 12:47:53**, one successful action paid **one nanogram** while its
comment claimed `1000000000000000000 Telegram Stars`:

- trace: `pGCZLffsRv2atRACn4mU9sJTP6og5d7xn2XJnp0izDE=`;
- action: `bZmC4aODuITxaH8a/y0EKbud7r5OzVqdcuVWHzpy5HY=`;
- sender: `0:4DE58CE7213418E8A67C8CA48710BC609C9297DD48219645363A350C880CA029`.

For `ton_transfer`, payment is in **`actions.value`**, not `actions.amount`.
For `jetton_transfer`, token units are in `amount`. Confusing the two would make
GRAM payment validation incorrect.

The same day includes successful **0.1 GRAM** transfers claiming 666,666,666,666
and 990,000,000,000,000,000 Stars. Incoming messages to both senders over August
23–30 contained no messages from the receiving address or the identified treasury
recipients, and no bounces. The one-nanogram sender likewise had no identified
refund in its subsequent incoming history. Unrelated incoming transfers cannot be
classified as refunds merely because they happen after a purchase.

This is evidence against relying on `no refund` plus a fixed payment minimum.
It does not exclude manual refunds through other unidentified service addresses.
A broad outgoing-history scan of the dominant treasury recipient timed out; it
is not counted as a completed audit of every possible Fragment payout address.

## Exact price-based dry run for the anomalous day

The prepared 2025-08-23 snapshot has:

- 47,587 labelled payments;
- 99 unparseable/invalid quantities;
- 47,488 parseable quantities;
- known subtotal **5,138,397,364,439,553,645 Stars**.

A bounded PostgreSQL scan reproduced that known subtotal exactly. It selected
successful GRAM payments to the pinned recipient, `start_utime` in
`[1755907200,1755993600)`, and trace MCs in `[51185824,51287783]` (the latter
extends through August 25 for late trace completion). This audit bound is not a
general replacement for the collector's late-trace handling.

Grouping by `(parsed_quantity, value)` returned 9,478 distinct pairs. Price is
`value / parsed_quantity`, calculated with exact rational arithmetic. A reference
price weighted by **actual money paid**, not declared Stars or payment count,
was **191472000 / 43 nanograms per Star**, approximately 0.00445284 GRAM/Star.
Weighting by actual money prevents arbitrarily many cheap comment-only payments
from automatically controlling the reference price.

Typical 50-Star payments on that day were **0.2181–0.2266 GRAM**.

Testing minimum payments of 50%, 80%, and 90% of `quantity * reference_price`
gave the **same result at all three thresholds**:

- 47,324 qualifying payments;
- **18,327,439 known Stars**;
- 164 rejected parseable payments, with a total payment of **6.292167246 GRAM**;
- the highest rejected per-Star price was only about **10.052%** of the reference;
- the largest rejected payment was **1 GRAM**.

These are sensitivity results for one day, not a production threshold or proof
that every remaining payment delivered Stars. The 99 unknown quantities are not
silently assigned zero or included in the price-qualified subtotal.

## Recommended next rule

Use payment-to-quantity consistency with a historical reference price, separated
by payment asset (and product if their rates differ). GRAM's exchange rate changes,
so a single absolute threshold for the entire history is unsuitable. For example,
an 80% price floor would admit 0.1 GRAM for 50 Stars when their expected cost is
0.11 GRAM, while preventing 0.1 GRAM from representing billions of Stars.

Before adopting a threshold, validate historical rate variation and low-volume
periods, decide how to qualify unknown-size payments, and retain excluded counts
for audit. Do not clamp a suspicious comment down to an invented Stars quantity.
Missing reference prices must remain explicit unknown coverage, not zero purchases.

## Sources and query notes

- [TON: Internal messages — Bounces](https://docs.ton.org/llms/foundations/messages/internal/content.md)
  explains failed-message bounce mechanics; it does not define Fragment business refunds.
- [Fragment: Buy Telegram Stars](https://fragment.com/stars/buy) lists 50 Stars
  at USD 0.75 and 1,000 at USD 15 in the observed page. Current GRAM quotes are
  not historical prices and should not be applied retroactively.
- [Fragment FAQ](https://fragment.com/about) discusses auction refunds and final
  purchases, but does not establish an automatic Stars-underpayment threshold.

Use indexed message lookups by `(source,created_lt)` or `(destination,created_lt)`.
To inspect a message within a known trace, use `(trace_id,tx_lt)`; bare
`(tx_hash,tx_lt)` lookups in this database selected an expensive plan and timed out.
Receiver transaction lookups by `(hash,lt,mc_block_seqno)` were bounded and fast.
All probes used read-only sessions; no source rows or published aggregates were modified.
