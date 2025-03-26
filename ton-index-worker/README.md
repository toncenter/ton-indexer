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
* `--threads <threads>` - number of CPU threads.
* `--stats-freq <seconds>` - frequency of printing a statistics.

