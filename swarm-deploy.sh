#!/bin/bash
set -e

# parse args
DEPLOY_DATABASE=0
MIGRATE=0
BUILD=1
DEPLOY_API=1

BUILD_ARGS=
POSITIONAL_ARGS=()

function usage() {
    echo 'Supported argumets:'
    echo ' -d --deploy-db           Deploy docker database'
    echo ' -m --migrate             Run database migration'
    echo '    --no-build            Do not build docker images'
    echo '    --no-build-cache      No build cache'
    echo '    --no-api              Do not deploy API'
    echo ' -h --help                Show this message'
    exit
}


while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 1
            ;;
        -d|--deploy-db)
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
        --no-api)
            DEPLOY_API=0
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
    docker compose -f docker-compose.api.yaml build $BUILD_ARGS
    docker compose -f docker-compose.api.yaml push
fi

if [[ $MIGRATE -eq "1" ]]; then
    echo "Running migrations"
    docker stack deploy --with-registry-auth -c docker-compose.alembic.yaml ${STACK_NAME}
fi

if [[ $DEPLOY_API -eq "1" ]]; then
    echo "Deploying API"
    docker stack deploy --with-registry-auth -c docker-compose.api.yaml ${STACK_NAME}
fi
