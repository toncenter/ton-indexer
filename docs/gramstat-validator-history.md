# Validator-count history

The `validators` aggregate reads indexed PostgreSQL cycle/member rows and checks
each new roster against config #34 at its activation block. The standard service
runs it hourly. Valid rounds are published even if another round fails; the
handler reports the failures after writing valid rows. Complete unchanged rounds
reuse the previously verified config hash. An explicit date range can backfill
already indexed history without replaying the indexer.

## Historical audit (November 2026)

The initial backfill published 3326 of 3333 source cycles. Published counts and
nominal end times matched the source exactly. Seven cycles needed investigation.

Four had valid activation blocks delayed by network pauses. Searching only five
minutes after `utime_since` incorrectly rejected them. The collector now searches
until the cycle's nominal end, still requiring the first subsequent MC to be a
key block, its predecessor to precede `utime_since`, and full config #34 equality.

| Nominal start (Unix) | Activation MC | Activation time (Unix) | Delay (seconds) |
| --- | --- | --- | --- |
| 1586074020 | 3081813 | 1586074470 | 450 |
| 1609268856 | 8536842 | 1609277049 | 8193 |
| 1629781624 | 13991799 | 1629820414 | 38790 |
| 1748782856 | 48400110 | 1748784140 | 1284 |

The other three expose limitations of a nominal-cycle-keyed model:

* **Bootstrap, since 1573821854:** MC 1 already contains five validators, but is
  not a key block and has no indexed predecessor. Config hash:
  `363633c3cb89ecb4be799141b6fb9ab07c18612841463db8f7f18b9dca79ca18`.
* **Since 1573921854:** config #34 still contains the five-validator set at MC
  94794. MC 94795 (time 1574095839) changes it to 17, after the new set's nominal
  end 1573987390. The 17-validator config hash is
  `832ad5420b0c658cac380acc35a26eb73b2c1e0e92d28bf52ae5315c27ae5b43`.
  Moving this observation back to its nominal start would invent history.
* **Since 1607798599:** MC 8525529 through 8536840 contains 21 validators, with
  nominal end 1607864135 and hash
  `6923de0292d1a4639982d72d321bbe1b9e7dafaa6e76cadfb1af74b5207a833d`.
  MC 8536841 changes the set to 16 and extends its end to 1609591189, retaining
  the same `utime_since`; hash
  `a781d671356c4577998fd5d2463b9deaf69fd97e6fb973e9de49826a18411147`.
  PostgreSQL has one cycle row with total 16 but 21 member rows. A key of
  `utime_since` cannot preserve both snapshots. Removing five member rows or
  replacing the count with 21 would not restore the historical sequence.

These three cases remain unpublished by the cycle-based handler. Correct support
requires bootstrap evidence and effective intervals keyed by config-change MC/hash,
separate from nominal election timestamps. The UI leaves missing intervals as gaps.

Config observations above were decoded from archival TON Center `getConfigParam`
responses; this is the same trusted RPC check as the handler, not a Merkle proof.
See [TON configuration parameters 32/34/36](https://docs.ton.org/foundations/config#32-and-34-and-36)
for previous/current/next validator sets. The historical observations demonstrate
why nominal validity times alone cannot establish actual activation boundaries.
