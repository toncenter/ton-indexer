# TON Indexer

> [!NOTE]
> Be careful upgrading the indexer. Only patch-level upgrades are supported. Major and minor version changes require a new database.

TON Indexer is a robust indexing system that extracts, transforms, and loads data from the TON blockchain into a PostgreSQL database. It enables efficient querying of blocks, transactions, messages, account states, NFTs, Jettons and Actions through a powerful API.

TON nodes store data in a RocksDB key-value store optimized for performance in specific use cases. However, RocksDB isn't well-suited for complex queries. TON Indexer bridges this gap by:
- Reading raw blockchain data from a local TON node's RocksDB.
- Parsing and transforming the data.
- Classifying transaction chains (traces) into Actions (such as DEX swaps, Multisig interactions and others).
- Persisting the data in a PostgreSQL database.

## Architecture Overview

TON Indexer stack consists of following services:
1. `postgres` - PostgreSQL server for primary storage for indexed blockchain data.
2. `index-api` - [Fiber](https://github.com/gofiber/fiber)-based server with convenient endpoints to access the database with REST API.
3. `event-classifier` - Actions classification service.
4. `index-worker` - TON Index worker to read and parse data from TON node database. Must run on the same machine as a functioning TON full node.
5. `run-migrations` -  Initializes the database schema and runs all required migrations.
6. `metadata-fetcher` - Indexes offchain metadata for Jettons and NFTs, optional service.
7. `imgproxy` - Proxies images from Jetton and NFT metadata, optional service.
8. `emulate-task-emulator` - emulates trace for given external message.
9. `emulate-event-classifier` - actions for emulated traces.
10. `emulate-api` - API for emulation services.

> [!IMPORTANT]
> Metadata fetcher and imgproxy services perform requests to external links, such requests may expose your IP, strongly recommended to run this services on a separate machine and set up `IMGPROXY_KEY` and `IMGPROXY_SALT` variables.

## Getting started

### Prerequisites

- **Docker & Docker Compose v2** - [Installation guide](https://docs.docker.com/engine/install/)
- **Running TON Full Node** - Follow the [official TON documentation](https://docs.ton.org/participate/run-nodes/full-node)
- **Recommended hardware:** 
  * Database: 8 cores CPU, 64 GB RAM, 4 TB NVME SSD
  * Worker: 16 cores CPU, 128 GB RAM, 1 TB NVME SSD

### Setup Instructions

```bash
git clone --recursive --branch master https://github.com/toncenter/ton-indexer.git
cd ton-indexer

# Copy and configure the environment file
cp .env.example .env
nano .env   # Set TON_WORKER_FROM and other variables as needed

# Set PostgreSQL password
mkdir private
echo -n "My53curePwD" > private/postgres_password

# Pull images and start the stack
docker compose pull
docker compose up -d

# To run ton-indexer with metadata services
docker compose --profile metadata pull
docker compose --profile metadata up -d

# To run emulate api
docker compose --profile emulate pull
docker compose --profile emulate up -d
```

Once the stack is running, the REST API and interactive Swagger are available at `localhost:8081/`.

> **Production Tip:** For performance and reliability, consider running the indexer stack and the index worker on separate machines.

# FAQ

## How to point TON Index worker to existing PostgreSQL instance

1. Stop and remove the bundled PostgreSQL container (add `-v` flag to remove the volume as well):

   ```bash
   docker compose rm postgres
   ```

2. Create new PostgreSQL database.

3. Set PostgreSQL credentials and host in the `.env` file.

4. Start relevant services: 

   ```bash
   docker compose up -d run-migrations index-worker index-api event-classifier event-cache
   ```

## How to run TON Index worker independently

When using a remote PostgreSQL instance, you can run the index worker independently without service dependencies:

```bash
docker compose up -d --no-deps index-worker
```

This is useful when you want to:
- Run index worker with a remote PostgreSQL database
- Deploy index worker separately from other services
- Avoid unnecessary service dependencies in your deployment

## How to update Indexer

1. Ensure your current version only differs from the latest by patch version. Major/minor upgrades are not supported and require clean DB.

2. Pull the latest source and container images:

    ```bash
    git pull
    git submodule update --init --recursive
    docker compose pull
    ```

3. Run new version: `docker compose up -d`

4. Check logs:

   - `run-migrations` — to confirm schema migrations succeeded.
   - `index-worker` — to ensure indexing resumed without issues.

# License

TON Indexer is licensed under the **MIT License**.
