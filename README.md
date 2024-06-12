# TON Indexer

> [!NOTE]  
> This repository's master branch hosts the TON Indexer designed for direct reading from the TON Node database. If you are looking for an indexer that operates through `tonlib + liteserver`, please refer to the branch [legacy](https://github.com/toncenter/ton-indexer/tree/legacy).


TON Indexer stores blocks, transactions, messages, NFTs, Jettons and DNS domains in PostgreSQL database and provides convenient API.

TON node stores data in a key-value database RocksDB.  While RocksDB excels in specific use cases, it isn't versatile enough for diverse queries. An SQL database is perfectly suitable for the storage and retrieval of data. TON Indexer reads data from RocksDB and inserts it into PostgreSQL. Masterchain blocks are used as an atomic unit of insertion to guarantee data integrity. Indexes allow the efficient fetching of required data from a database.

TON Indexer stack consists of:
1. `postgres`: PostgreSQL server to store indexed data and perform queries.
2. `index-api`: FastAPI server with convenient endpoints to access the database.
3. `alembic`: alembic service to run database migrations.
4. `index-worker`: TON Index worker to read and parse data from TON node database. This service must be run on the machine with a working TON Node.

## How to run

Requirements:
* Docker and Docker compose (see [instruction](https://docs.docker.com/engine/install/)).
* Running TON full node (archival for full indexing).
* Recommended hardware: 
  * Database and API: 4 CPU, 32 GB RAM, 200GB disk, SSD recommended (more than 1TB required for archival indexing).
  * Worker: 4 CPU, 32 GB RAM, SSD recommended (for archival: 8 CPUs, 64 GB RAM, SSD recommended).

Do the following steps to setup TON Indexer:
* Clone repository: `git clone --recursive --branch v0.4.2 https://github.com/toncenter/ton-indexer/`.
* Create *.env* file with command `./configure.sh`.
  * `./configure.sh` will create `.env` file only with indexer and PostgreSQL configuration data. Use `--worker` flag to add TON Index worker configuration data too.
* Adjust parameters in *.env* file (see [list of available parameters](#available-parameters)).
* Set postgreSQL password by running `echo -n 'YOUR_PASSWORD' >> private/postgres_password`
* Build docker images: `docker compose build postgres alembic index-api`.
* Run stack: `docker compose up -d postgres alembic index-api`.
  * To start worker use command `docker compose up -d index-worker` after creating all services.

**NOTE:** we recommend to setup indexer stack and index worker on separate servers. To install index worker to **Systemd** check this [instruction](https://github.com/toncenter/ton-index-worker).

### Available parameters

* PostgreSQL parameters:
  * `POSTGRES_HOST`: PostgreSQL host. Can be IP or FQDN. Default: `postgres`.
  * `POSTGRES_PORT`: PostgreSQL port. Default: `5432`.
  * `POSTGRES_USER`: PostgreSQL user. Default: `postgres`.
  * Set password in the file `private/postgres_password`. Ensure there is no newline character (\n) at the end.
  * `POSTGRES_DBNAME`: PostgreSQL database name. You can change the database name to use one instance of PostgreSQL for different TON networks (but we strongly recommend using separate instances). Default: `ton_index`.
  * `POSTGRES_PUBLISH_PORT`: a port to publish. Change this port if you host multiple indexers on the same host. Default: `5432`.
* API parameters:
  * `TON_INDEXER_API_ROOT_PATH`: root path for reverse proxy setups. Keep it blank if you don't use reverse proxies. Default: `<blank>`.
  * `TON_INDEXER_API_PORT`: a port to expose. You need check if this port is busy. Use different ports in case of multiple indexer instances on the same host. Default: `8081`.
  * `TON_INDEXER_TON_HTTP_API_ENDPOINT`: an endpoint to [ton-http-api](https://github.com/toncenter/ton-http-api). Indexer API contains several proxy methods to ton-http-api service: `/message`, `/runGetMethod`, `/estimateFee`, `/account`, `/wallet`. If the variable is not set these methods will not be included. Default: `<blank>`
  * `TON_INDEXER_WORKERS`: number of API workers. Default: `4`.
* TON worker parameters:
  * `TON_WORKER_DBROOT`: path to TON full node database. Use default value if you've installed node with `mytonctrl`. Default: `/var/ton-work/db`.
  * `TON_WORKER_FROM`: masterchain seqno to start indexing. Set 1 to full index, set last masterchain seqno to collect only new blocks (use [/api/v2/getMasterchainInfo](https://toncenter.com/api/v2/getMasterchainInfo) to get last masterchain block seqno).
  * `TON_WORKER_MAX_PARALLEL_TASKS`: max parallel reading actors. Adjust this parameter to decrease RAM usage. Default: `1024`.
  * `TON_WORKER_INSERT_BATCH_SIZE`: max masterchain seqnos per INSERT query. Small value will decrease indexing performance. Great value will increase RAM usage. Default: `512`.
  * `TON_WORKER_INSERT_PARALLEL_ACTORS`: number of parallel INSERT transactions. Increasing this number will increase PostgreSQL server RAM usage. Default: `3`.

## How to use http API

As mentioned before, there is `index-API` coming with indexer. It provides several functions that API user can use to fetch data from postgresql db. Some of them:

* /masterchainInfo
* /blocks
* /masterchainBlockShardState
* /addressBook
* /masterchainBlockShards
* /transactions
* /transactionsByMasterchainBlock
* /transactionsByMessage
* /adjacentTransactions
* /traces
* /transactionTrace
* /messages
* /nft/collections
* /nft/items
* /nft/transfers
* /jetton/masters
* /jetton/wallets
* /jetton/transfers
* /jetton/burns
* /topAccountByBalance

All of that endpoints are `GET`. 

Example of http API usage:

```json
root@MyServerWithRunningIndexerAndApi:~# curl --request GET --url 0.0.0.0:8081/masterchainInfo
{
  "last": {
    "workchain": -1,
    "shard": "8000000000000000",
    "seqno": 3247951,
    "root_hash": "ysDp4rUqj17B24VkUKX/kLPmagp7GsySWNI30LlM6BQ=",
    "file_hash": "13NC8TTUG814CX3a60Oqzg0CDiC2cXCy/7jDvRFD9YI=",
    "global_id": -239,
    "version": 0,
    "after_merge": false,
    "before_split": false,
    "after_split": false,
    "want_merge": true,
    "want_split": false,
    "key_block": false,
    "vert_seqno_incr": false,
    "flags": 1,
    ... <- some other data
    "masterchain_block_ref": {
      "workchain": -1,
      "shard": "8000000000000000",
      "seqno": 3247951
    },
    "prev_blocks": [
      {
        "workchain": -1,
        "shard": "8000000000000000",
        "seqno": 3247950
      }
    ]
  },
  "first": {
    "workchain": -1,
    "shard": "8000000000000000",
    "seqno": 4,
    ... <- some other data
    "tx_count": 11,
    "masterchain_block_ref": {
      "workchain": -1,
      "shard": "8000000000000000",
      "seqno": 4
    },
    "prev_blocks": [
      {
        "workchain": -1,
        "shard": "8000000000000000",
        "seqno": 3
      }
    ]
  }
}
```

Indexer API also contains several proxy methods to ton-http-api service (url must be provided by `TON_INDEXER_TON_HTTP_API_ENDPOINT`):

1. POST `/message` - Send an external message to the TON network.
2. POST `/runGetMethod` - Execute a smart contract's get method.
3. POST `/estimateFee` - Estimate the fee for an external message.
4. GET `/account` - Get information about a smart contract.
5. GET `/wallet` - Get information about a smart contract wallet.

Mostly that API is very close to [ton center v3 API](https://toncenter.com/api/v3/), but not equal.

# FAQ

## How to point TON Index worker to existing PostgreSQL instance
* Remove PostgreSQL container: `docker compose rm postgres` (add flag `-v` to remove volumes).
* Setup PostgreSQL credentials in *.env* file.
* Run alembic migration: `docker compose up alembic`.
* Run index worker: `docker compose up -d index-worker`.

## How to update code
* Pull new commits: `git pull`.
* Update submodules: `git submodule update --recursive --init`.
