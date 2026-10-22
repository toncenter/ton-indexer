# Live Firehose

```
archive node host                          store host (Garage)
reader-node ── one-block files ──────────► firehose/one-blocks ──► merger ──► firehose/merged-blocks
     └──────── live blocks (gRPC :10010) ─────────────────────────► relayer ──► firehose, substreams
```

- **reader-node** runs ton-index-firehose on the node db: the tool prints the
  Firehose protocol, resumes from its cursor and is started with `--from` only
  the first time. Deployed with `deploy-firehose.yaml` in the repository root
  (service `{{ stack_name }}__firehose_reader`, config in `ansible/config.yaml`
  under `services.firehose_reader`). Its block stream (:10010) and metrics
  (:9102) listen on the private address in `firehose_reader_private_ip`,
  everything else on localhost.
- **merger**, **relayer**, **firehose** and **substreams** (tier1 and tier2 in
  one process) run on the store host as the `firecore` user, from the units
  here, with the Garage key in `/etc/firecore/garage.env`. Their gRPC listens on
  localhost, their metrics on the private address of the host (9103, 9104,
  9102, 9105). The merger goes on from the last bundle in merged-blocks, so the
  history must reach the block the reader starts from: the backfill
  (`../backfill`) fills the gap.

The unit files use example addresses: `192.0.2.10` for the archive node
and `192.0.2.20` for the store host. Replace them with your private
addresses before installing the units.

```bash
sudo install -m 644 firecore-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now firecore-merger firecore-relayer firecore-firehose firecore-substreams
```
