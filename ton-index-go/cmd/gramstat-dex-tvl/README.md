# Closing pool TVL

`publish.py --from YYYY-MM-DD --to YYYY-MM-DD` produces a dry run (end
exclusive). `--write` replaces the whole day's `dex_tvl_gram_v1` metric;
`--missing` skips existing observations even if some sides lack a quote.
Explicit recalculation omits `--missing`. Day-end **stock**, not turnover.

Install `tools/gramstat_tvl.py` and `python3-clickhouse-driver` on the approved
price reader host with the same read-only connection configuration as the DEX
price reader. Provision a separate restricted SSH key with a source address
restriction and forced command `/usr/bin/python3 -I /opt/gramstat-prices/tvl.py`.
Configure alias `gramstat-tvl` in `/etc/gramstat-dex/ssh_tvl_config` with its
own identity and pinned host key. The sink service only needs the existing
`GRAMSTAT_CH_URL`, `GRAMSTAT_CH_USER` and `GRAMSTAT_CH_PASSWORD` in `tvl.env`.
No public API or blockchain indexer credentials are required on the reader.

The first day of a batch reconciles each daily closing state against the same
`(pool,mc_seqno,math_version)` in the raw snapshot partition. Subsequent days
only replace changed pools, reconciling every reserve against `pool_daily`.
Unchanged cash stays, missing prices do not. `source_mc_seqno` is the greatest
observed pool MC through the day, not a fabricated midnight block number.
The live ingestion and finalized price watermarks must both cover the close.
