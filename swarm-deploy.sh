#!/bin/bash
set -e

# toncenter env: testnet, mainnet, stage
export TONCENTER_ENV=${1:-mainnet}
STACK_NAME="${TONCENTER_ENV}-indexer"
echo "Deploying stack: ${STACK_NAME}"

if [ -f ".env.${TONCENTER_ENV}" ]; then
    echo "Found env for ${TONCENTER_ENV}"
    ENV_FILE=".env.${TONCENTER_ENV}"
elif [ -f ".env" ]; then
    echo "Found default .env"
    ENV_FILE=".env"
else
    echo "Please provide env file"
    exit 1
fi

# load environment variables
if [ ! -z "${ENV_FILE}" ]; then
    set -a; source ${ENV_FILE}; set +a
fi

# check global network
NETWORK_ID=$(docker network ls -f "name=toncenter-global" -q)

if [[ -z "$NETWORK_ID" ]]; then
    echo "Creating toncenter-global network"
    NETWORK_ID=$(docker network create --attachable --driver=overlay toncenter-global)
fi
echo "Network ID of toncenter-global: $NETWORK_ID"

# deploy with compose file
COMPOSE_FILE=docker-compose.swarm.yaml
if [[ $# -eq 2 ]]; then
COMPOSE_FILE=$2
echo "Deploying compose file ${COMPOSE_FILE}"
fi

# build image
docker compose -f ${COMPOSE_FILE} build
docker compose -f ${COMPOSE_FILE} push

# deploy stack
docker stack deploy --with-registry-auth -c ${COMPOSE_FILE} ${STACK_NAME}
