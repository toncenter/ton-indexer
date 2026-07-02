# TON Index (C++ Worker)

This is a C++ worker for [TON indexer](https://github.com/toncenter/ton-indexer/). This worker reads data from TON node files, parses TL-B schemas, and inserts data into PostgreSQL database.

## 1. How to install

Before installing the worker, ensure the TON Index database is set up using the [instructions provided](https://github.com/toncenter/ton-indexer/tree/master).


### 1.1. Setup as systemd daemon
To install as a daemon use the script below: 

    ./scripts/add2systemd.sh --db /var/ton-work/db --host <ip> --port <port> \
                             --user <postgres user> --password <postgres password> --dbname <database name> \
                             --from 1 --max-active-tasks $(nproc) --threads $(nproc) \
                             --max-insert-actors <number of insert actors> [--force]

You may find the list of arguments in section [available arguments](#13-available-arguments). Use flag `--force` to force rebuild binary. 

**NOTE:** We strongly recommend to setup worker **separately** from the database server because of significant RAM and Disk IO usage.

### 1.2. Manual install

Do the following steps to build and run index worker from source.

1. Install required packages: 

        sudo apt-get update -y
        sudo apt-get install -y build-essential cmake clang openssl libssl-dev zlib1g-dev gperf wget git curl libreadline-dev ccache libmicrohttpd-dev pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev python3-dev libpq-dev ninja-build
2. Build TON index worker binary:

        mkdir -p build
        cd build
        cmake -DCMAKE_BUILD_TYPE=Release -GNinja ..
        ninja -j$(nproc) ton-index-postgres

3. Install binary to your system:

        sudo cmake --install 

4. Increase maximum opened files limit: 

        ulimit -n 1000000

5. Run TON index worker:

        ton-index-postgres <args>

### 1.3. Available arguments:
* `--db <path>` - path to TON node directory. Pass `/var/ton-work/db`, if you have TON node installed by mytonctrl. **Required**.
* `--host <ip>` - PostgreSQL host. Only IPv4 is acceptable. Default: `127.0.0.1`.
* `--port <port>` - PostgreSQL port. Default: `5432`.
* `--user <user>` - PostgreSQL user. Default: `postgres`.
* `--password <password>` - PostgreSQL password. Default: empty password.
* `--dbname <dbname>` - PostgreSQL database name. Default: `ton_index`.
* `--from <seqno>` - Masterchain seqno to start indexing from. Use value `1` to index the whole blockchain.
* `--max-active-tasks <count>` - maximum parallel disk reading tasks. Recommended value is number of CPU cores.
* `--max-queue-blocks <size>` - maximum blocks in queue (prefetched blocks from disk).
* `--max-queue-txs <size>` - maximum transactions in queue.
* `--max-queue-msgs <size>` - maximum messages in queue.
* `--max-insert-actors <actors>` - maximum concurrent INSERT queries.
* `--max-batch-blocks <size>` - maximum blocks in batch (size of insert batch).
* `--max-batch-txs <size>` - maximum transactions in batch.
* `--max-batch-msgs <size>` - maximum messages in batch.
* `--max-data-depth <depth>` - maximum depth of data boc to index (use 0 to index all accounts).
* `--pg-no-copy` - use `INSERT ... ON CONFLICT` instead of `COPY` for PostgreSQL historical table writes.
* `--db-event-fifo <path>` - path to the FIFO pipe created by `validator-engine --db-event-fifo`. New blocks are then indexed as soon as the node applies them instead of being discovered by polling; polling remains as a fallback if events stop. The FIFO supports a single reader, so use a dedicated pipe per consumer.
* `--threads <threads>` - number of CPU threads.
* `--stats-freq <seconds>` - frequency of printing a statistics.
* `--failover-check-interval <seconds>` - frequency of failover watchdog checks; `0` disables the watchdog. Default: `10`.
* `--failover-strikes <count>` - consecutive watchdog checks a probed inconsistency must persist before the worker restarts. Default: `3`.

## 2. Failover recovery

When PostgreSQL (e.g. Patroni) or Kvrocks run with asynchronous replication, a failover may promote a replica that misses a tail of acknowledged writes. The worker handles this in two parts:

The recovery state on the Kvrocks side is a monotonic watermark key `ton-index:v1:progress:finalized_mc_seqno`, advanced only through actually written data: right after the Kvrocks writes of a batch are acknowledged, the leader atomically claims the batch's seqno range (`ton-index:v1:progress:pending_mc_seqno_ranges` + Lua sweep) and the watermark moves forward exactly through contiguously claimed ranges. The claim follows the data in the Kvrocks replication stream, so on any replica watermark `W` implies all Kvrocks data for seqnos `<= W` is present, and a failover rolls the watermark back together with the data. The watermark never mirrors PostgreSQL progress: during post-failover re-indexing PostgreSQL runs ahead, and the watermark catches up only as batches are actually re-written to Kvrocks, so a second failover mid-recovery still rewinds correctly.

1. **Resume point.** On startup the worker resumes from `min(finalized_mc_seqno + 1, kvrocks watermark + 1)`, re-indexing the range Kvrocks may have lost. Re-applied blocks are idempotent: PostgreSQL uses `ON CONFLICT`, Kvrocks writes are guarded by `source_mc_seqno` compare-and-set. A completely missing watermark on a non-empty index is reported with an ERROR log line ("Kvrocks progress watermark is missing") instead of a rewind: it means either the first run with this feature or a Kvrocks node that lost all its data — the latter cannot be healed by re-indexing and requires rebuilding Kvrocks, so alert on this line.
2. **Failover watchdog.** A background actor periodically verifies direct facts about acknowledged data: `finalized_mc_seqno` must not decrease; the masterchain block right above it must exist in the `blocks` table whenever higher seqnos were already acknowledged; the Kvrocks watermark must not decrease, disappear, or lag the acknowledged head (every seqno is claimed to the watermark before its batch commits, so a lagging watermark means claimed data vanished — this also catches regressions that go up and back down between two samples, and writes acknowledged by a deposed Kvrocks master). Each signal is a fact check, not a timing heuristic, so slow catch-up batches (minutes-long transactions committing out of order, `finalized_mc_seqno` lagging behind) and post-failover re-indexing do not produce false restarts. On detection the worker exits with code `86` and the supervisor restart (systemd `Restart=always`, docker `restart: unless-stopped`) triggers the resume-point recovery above, which converges instead of looping.

The watchdog runs in every worker (leader and standbys) in normal indexing mode; it is disabled in bounded archive mode (`--from`/`--to`), and bounded archive backfills do not touch the watermark. When rebuilding Kvrocks from scratch on purpose, `DEL ton-index:v1:progress:finalized_mc_seqno` as well — the next worker start logs the expected "watermark is missing" ERROR once and continues from PostgreSQL progress.
