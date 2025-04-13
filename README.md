# TON Indexer

> [!NOTE]  
> This repository's master branch hosts the TON Indexer designed for direct reading from the TON Node database. If you are looking for an indexer that operates through `tonlib + liteserver`, please refer to the branch [legacy](https://github.com/toncenter/ton-indexer/tree/legacy).

> [!NOTE]
> Be careful upgrading the indexer. Versions with different major and minor have different schemas and they are incompatible with each other.


TON Indexer stores blocks, transactions, messages, NFTs, Jettons and DNS domains in PostgreSQL database and provides convenient API.

TON node stores data in a key-value database RocksDB.  While RocksDB excels in specific use cases, it isn't versatile enough for diverse queries. An SQL database is perfectly suitable for the storage and retrieval of data. TON Indexer reads data from RocksDB and inserts it into PostgreSQL. Masterchain blocks are used as an atomic unit of insertion to guarantee data integrity. Indexes allow the efficient fetching of required data from a database.

TON Indexer stack consists of:
1. `postgres`: PostgreSQL server to store indexed data and perform queries.
2. `index-api`: [Fiber](https://github.com/gofiber/fiber) server with convenient endpoints to access the database.
3. `event-classifier`: trace classification service.
4. `index-worker`: TON Index worker to read and parse data from TON node database. This service must be run on the machine with a working TON Node.

## How to run

Requirements:
* Docker and Docker compose v2 (see [instruction](https://docs.docker.com/engine/install/)).
* Running [TON full node](https://docs.ton.org/participate/run-nodes/full-node) ([archival](https://docs.ton.org/participate/run-nodes/archive-node) for full indexing).
  * **NOTE:** Archive Node requires capacity of more than 4 TB, SSD OR Provided 32+k IOPS storage. Please, pay attention that node is constantly growing and resource consumption my increase within time.
* Recommended hardware: 
  * Database and API: 4 CPU, 32 GB RAM, 200GB disk, SSD recommended (more than 1TB required for archival indexing).
  * Worker: 4 CPU, 32 GB RAM, SSD recommended (for archival: 8 CPUs, 64 GB RAM, SSD recommended).

Do the following steps to setup TON Indexer:
* Clone repository: `git clone --recursive --branch master https://github.com/toncenter/ton-indexer.git && cd ./ton-indexer`.
* Create *.env* file with command `./configure.sh`.
  * ./configure.sh will create .env file only with indexer and PostgreSQL configuration data. Use --worker flag to add TON Index worker configuration data too.
* Adjust parameters in *.env* file (see [list of available parameters](#available-parameters)).
* Set PostgreSQL password `echo -n "MY_PASSWORD" > private/postgres_password`
* Build docker images: `docker compose build postgres event-classifier index-api`.
* Run stack: `docker compose up -d postgres event-classifier event-cache index-api`.
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
  * `TON_WORKER_ADDITIONAL_ARGS`: additional args to pass into index worker.

## Swagger

To test API, built-in swagger can be used. It is available after running `docker compose` at `localhost:8081/api/v3`

# FAQ

## How to point TON Index worker to existing PostgreSQL instance
* Remove PostgreSQL container: `docker compose rm postgres` (add flag `-v` to remove volumes).
* Setup PostgreSQL credentials in *.env* file.
* Run index worker: `docker compose up -d index-worker index-api event-classifier event-cache`.

## How to update code
* Pull new commits: `git pull`.
* Update submodules: `git submodule update --recursive --init`.
* Build new image: `docker compose build postgres event-classifier event-cache index-api`.
  * Build new image of worker: `docker compose build index-worker`
* Run new version: `docker compose up -d postgres event-classifier event-cache index-api`
  * Run new version of worker: `docker compose up -d index-worker`

# LICENSE

TON Indexer is licensed under the **Server Side Public License v1 (SSPL)**.

You are free to use, modify, and redistribute this project under the SSPL terms. If you plan to offer it as a paid service, you must comply with SSPL’s requirement  
to open-source all infrastructure code used to host the service.

To use the software **without** open-sourcing your infrastructure, please [contact us](https://t.me/toncenter_support) for a commercial license.
