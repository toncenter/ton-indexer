# TON Index C++

This is a C++ worker for [TON indexer](https://github.com/kdimentionaltree/ton-indexer/tree/cpp-indexer). This worker reads data from TON node files, parse TL-B schemas and insert data to PostgreSQL database.

## 1. How to install

To install this worker you need first to setup TON Indexer database (see [instruction](https://github.com/kdimentionaltree/ton-indexer/tree/cpp-indexer)).


### 1.1. Setup as systemd daemon
To install a daemon just run a script: 

    ./scripts/add2systemd.sh <args> [--force]

You may find list of arguments in section [available arguments](#13-available-arguments). Use flag `--force` to force rebuild binary. 

**NOTE:** We strongly recommend to setup worker **separately** from database server because of significant RAM and Disk IO usage.

### 1.2. Manual install

Do the following steps to build and run index worker from source.

1. Install required packages: 

        sudo apt-get update -y
        sudo apt-get install -y build-essential cmake clang openssl libssl-dev zlib1g-dev gperf wget git curl libreadline-dev ccache libmicrohttpd-dev pkg-config libsecp256k1-dev libsodium-dev python3-dev libpq-dev ninja-build
2. Build TON index worker binary:

        mkdir -p build
        cd build
        cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=off -GNinja .
        ninja -j$(nproc) tondb-scanner

3. Install binary to your system:

        sudo cp ./tondb-scanner/tondb-scanner /usr/local/bin

4. Increase maximum opened files limit: 

        ulimit -n 1000000

5. Run TON index worker:

        tondb-scanner <args>

### 1.3. Available arguments:
* `--db <path>` - path to TON node directory. Pass `/var/ton-work/db`, if you have TON node installed by mytonctrl. **Required**.
* `--host <ip>` - PostgreSQL host. Only IPv4 acceptable. Default: `127.0.0.1`.
* `--port <port>` - PostgreSQL port. Default: `5432`.
* `--user <user>` - PostgreSQL user. Defaul: `postgres`.
* `--password <password>` - PostgreSQL password. Default: empty password.
* `--dbname <dbname>` - PostgreSQL database name. Default: `ton_index`.
* `--from <seqno>` - Masterchain seqno to start indexing from. Use value `1` to index whole blockchain.
* `--max-parallel-tasks <count>` - maximum parallel disk reading tasks. Default: `2048`.
* `--insert-batch-size <size>` - maximum masterchain seqnos in one INSERT query. Default: `512`.
* `--insert-parallel-actors <actors>` - maximum concurrent INSERT queries. Default: `3`.

