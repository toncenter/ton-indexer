#!/bin/bash
set -e

# parse args
DEPLOY_DATABASE=0
MIGRATE=0
BUILD=1
DEPLOY_API=0
DEPLOY_EVENTS=0
DEPLOY_IMGPROXY=0
UPDATE_POOLS=0

BUILD_ARGS=
POSITIONAL_ARGS=()

function usage() {
    echo 'Supported argumets:'
    echo ' -d --deploy-db           Deploy docker database'
    echo ' -m --migrate             Run database migration'
    echo '    --no-build            Do not build docker images'
    echo '    --no-build-cache      No build cache'
    echo '    --api                 Deploy API'
    echo '    --events              Deploy event classfier'
    echo ' -h --help                Show this message'
    exit
}


while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 1
            ;;
        -d|--db)
            DEPLOY_DATABASE=1
            shift
            ;;
        -m|--migrate)
            MIGRATE=1
            shift
            ;;
        --no-build)
            BUILD=0
            shift
            ;;
        --no-build-cache)
            BUILD_ARGS=--no-cache
            shift
            ;;
        --api)
            DEPLOY_API=1
            shift
            ;;
        --events)
            DEPLOY_EVENTS=1
            shift
            ;;
        --imgproxy)
            DEPLOY_IMGPROXY=1
            shift
            ;;
        --update-pools)
            UPDATE_POOLS=1
            shift
            ;;
        -*|--*)
            echo "Unknown option $1"
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1") # save positional arg
            shift # past argument
            ;;
    esac
done

set -- "${POSITIONAL_ARGS[@]}"

# toncenter env: testnet, mainnet, stage
export TONCENTER_ENV=${1:-mainnet}
STACK_NAME="${TONCENTER_ENV}-indexer-cpp"
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

# deploy database
if [[ $DEPLOY_DATABASE -eq "1" ]]; then
    echo "Deploying Docker database"
    docker stack deploy -c docker-compose.database.yaml ${STACK_NAME}
fi

# build image
if [[ $BUILD -eq "1" ]]; then
    docker compose -f docker-compose.api.yaml -f docker-compose.events.yaml -f docker-compose.imgproxy.yaml build $BUILD_ARGS
    docker compose -f docker-compose.api.yaml -f docker-compose.events.yaml -f docker-compose.imgproxy.yaml push
fi

if [[ $UPDATE_POOLS -eq "1" ]]; then
    echo "updating DeDust pools"
    curl -s https://api.dedust.io/v2/pools -o indexer/files/dedust_pools.json
fi

if [[ $MIGRATE -eq "1" ]]; then
    echo "Running migrations"
    docker stack deploy --with-registry-auth -c docker-compose.alembic.yaml ${STACK_NAME}
fi

if [[ $DEPLOY_API -eq "1" ]]; then
    echo "Deploying API"
    docker stack deploy --with-registry-auth -c docker-compose.api.yaml ${STACK_NAME}
fi

if [[ $DEPLOY_EVENTS -eq "1" ]]; then
    echo "Deploying event classifier"
    docker stack deploy --with-registry-auth -c docker-compose.events.yaml ${STACK_NAME}
fi

if [[ $DEPLOY_IMGPROXY -eq "1" ]]; then
    echo "Deploying imgproxy"
    docker stack deploy --with-registry-auth -c docker-compose.imgproxy.yaml ${STACK_NAME}
fi
