# DEX user volume

`go build ./cmd/gramstat-dex-volume`. A dry run is the default. For example,
`gramstat-dex-volume --from 2026-11-22 --to 2026-11-23` prints a reconciliable
UTC day and execution time. `--write` atomically replaces each day in
`gramstat.daily_metrics`; `--missing` resumes without re-reading partial but
already published days. Omit `--missing` for an explicit historical rebuild.

The worker uses `GRAMSTAT_PG_DSN` in a read-only repeatable-read transaction,
and the existing `GRAMSTAT_CH_URL`, `GRAMSTAT_CH_USER`, `GRAMSTAT_CH_PASSWORD`
for the result. Supply only these variables to the service. The public API
reads prepared rows; it has no route to either raw source.

Install `tools/gramstat-dex-prices.py` on the authorized price reader host
with `python3-clickhouse-driver`. Provision `/etc/gramstat-dex-prices.json`
with the native `host`, `port`, `user`, `password` for `dex_prices_hist`;
only the restricted reader and administrators should be able to read it.
Configure the `gramstat-prices` host alias in `/etc/gramstat-dex/ssh_config`
with a dedicated key and pinned host key. Restrict the authorized key to
the aggregator source and the forced command `/usr/bin/python3 -I
/opt/gramstat-prices/read.py` with `restrict`. Price reads are bounded by
asset and day; the reader checks the final publication watermark and fails
the day on a transport/query error. No shell command is supplied by the caller.

One closed-trace MC interval is read in six-hour slices. Only multi-action
or mixed-success traces need message-tree reads (64 at a time). An action's
`peer_swaps` are **not** independent user trades. Shared initiating tx hashes
and LT order alone are not proof of a funding edge. Serial steps must have
the same asset, a confirmed handoff on the funding path and a next input
(sum of children if split) between 80% and 120% of the previous output,
inclusive. Keep the common intermediate amount at the smaller observed amount.
A STON.fi v2 pTON handoff may name the two ends of the same directed transfer:
verify its opcode, paired message and action membership. Direct, separately
funded GRAM legs from one external root to different venues that return the
same token to that payer form one split, including when old wallet code is
unavailable. Check the attached value but sum only actual swap amounts.
Separate branches of a common instruction use successful
disjoint inputs only; separate commands from a recognized wallet may be
independent. Failed, ambiguous and unpriced observations are never promoted to
complete volume. Exceptional traces over 64 swap actions or 2,048 transactions
are left unresolved. Same-protocol versions share one category; confirmed
cross-venue routes contribute once to `multiple`.

The service processes missing dates newest first. The start includes the month
of the first positive `swaps_total`. Day selection uses the MC in which the
trace closed; the quote instant is the action/route end. Direct GRAM and both
STON.fi pTON masters are valued at their transferred nanogram amount. Otherwise
use the actual boundary USDT amount at the preceding minute's rate. When
neither boundary qualifies, use one confirmed intermediate GRAM or USDT
amount covering every successful funded path. Multi-hop action records must
match both endpoints exactly; the same 20% tolerance applies only between
adjacent hops in raw units. Only then fall back to
a jetton spot estimate, preferring the received token over the spent token
at equal freshness. Admit estimates only up to 20% of the historical
`liquidity_quote` in nanograms from the same observation; above that we cannot
reliably infer the execution value from a marginal rate. Prefer quotes no
more than 15 minutes old on either boundary. If
neither side can be valued this way, use the latest still-valid jetton quote
before execution, without an age cap or later GRAM/USDT adjustment. An invalid
latest quote invalidates the asset; it does not uncover an older valid quote.
The USDT anchor itself must be from the preceding minute. Look up pre-window
history only for assets on otherwise unpriced routes. `dex_volume_gram_v5`
records `carried` and `intermediate` as disjoint subsets of priced routes and
does not overwrite earlier metric versions.
