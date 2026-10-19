# Backfill of merged-blocks

The history is converted in bounded chunks of ton-index-firehose runs and
uploaded to the store that firecore reads (`firehose/merged-blocks` in Garage).

- `make_chunks.py` splits a masterchain range into chunks of about equal work
  by the archive package sizes (`du -s --block-size=1M arch*`).
- `runner.py` converts the queued chunks, checks and uploads their bundles and
  keeps the state of each chunk, so it can be stopped and started again.
- `rclone-garage.sh` is rclone with the remote `garage:`, configured from a
  Garage key file.
- `verify.py` checks the bundles of a range in the store: every block there,
  in order and linked by parent_num and parent_id.

## Queue

```bash
du -s --block-size=1M /nvmepool/ton-db/archive/packages/arch* > arch_sizes.txt
./make_chunks.py arch_sizes.txt 1 67880999 15000 2000000 > chunks-history.tsv
./runner.py init --state history --chunks chunks-history.tsv
```

## Run

As a transient service, so it survives the ssh session and stops on
`systemctl stop`. A runner that crashed is started again and goes on from its
state; a tool killed for memory is a failed attempt of its chunk, not the end
of the service (OOMPolicy=continue). On a stopped db dump:

```bash
sudo systemd-run --unit=fh-backfill-history -p Restart=on-failure -p RestartSec=60 -p OOMPolicy=continue \
  -p User=toncenter -p Group=toncenter \
  -p LimitNOFILE=1048576 -p MemoryMax=150G -p TimeoutStopSec=300 \
  -p StandardOutput=append:$HOME/fh-backfill/history.log -p StandardError=append:$HOME/fh-backfill/history.log \
  -E GARAGE_ENV=$HOME/.config/garage/backfill.env \
  python3 $HOME/fh-backfill/runner.py run --state $HOME/fh-backfill/history --work $HOME/fh-backfill/work \
  --bin $HOME/ton-indexer-substreams/build/ton-index-worker/ton-index-firehose/ton-index-firehose --db /nvmepool/ton-db \
  --tool-args "--threads 64 --max-active-tasks 64 --celldb-cache-size 8192 --stats-freq 60" \
  --env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 --workers 4 \
  --rclone $HOME/fh-backfill/rclone-garage.sh --dest garage:firehose/merged-blocks
```

On the db of a live node, as the node user and with a memory cap;
the node host lacks some libraries of the build, they are in `$H/lib`:

```bash
H=/var/tmp/fh-backfill
sudo systemd-run --unit=fh-backfill-tail -p Restart=on-failure -p RestartSec=60 -p OOMPolicy=continue \
  -p User=validator -p Group=validator \
  -p LimitNOFILE=1048576 -p MemoryMax=64G -p TimeoutStopSec=300 \
  -p StandardOutput=append:$H/tail.log -p StandardError=append:$H/tail.log \
  -E GARAGE_ENV=/etc/fh-backfill/backfill.env -E RCLONE=$H/rclone \
  python3 $H/runner.py run --state $H/tail --work $H/work --bin $H/ton-index-firehose --db /var/ton-work/db \
  --tool-args "--threads 96 --max-active-tasks 96 --celldb-cache-size 24576 --celldb-direct-io --stats-freq 60" \
  --env LD_LIBRARY_PATH=$H/lib --env OPENBLAS_NUM_THREADS=1 --env LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2 \
  --workers 1 --rclone $H/rclone-garage.sh --dest garage:firehose/merged-blocks
```

`--only` and `--limit` pick chunks for a trial. `runner.py status` shows the
progress, `runner.py reset` queues the failed chunks again; the log of every
chunk is kept in `<state>/logs`.

## Check

In minutes, from the runner states (copy the states of every machine to one
place): the done chunks cover the range once, the store lists every bundle with
the sizes the runner uploaded, and the blocks link up across every chunk
boundary. The inside of a chunk was checked by the runner and the upload.

```bash
GARAGE_ENV=... ./verify.py --rclone ./rclone-garage.sh --src garage:firehose/merged-blocks --from 1 --to 99999999 \
  --states history states-live/history states-live/tail --jobs 16
```

Without `--states` every bundle of the range is downloaded and read, at about
the speed of the network.
