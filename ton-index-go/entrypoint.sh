#!/bin/bash
set -e

# postgres password
if [ ! -z "$POSTGRES_PASSWORD_FILE" ]; then
    echo "Postgres password file: ${POSTGRES_PASSWORD_FILE}"
    if [ ! -f "${POSTGRES_PASSWORD_FILE}" ]; then
        echo "Password file not found"
        exit 1
    fi
    POSTGRES_PASSWORD=$(cat ${POSTGRES_PASSWORD_FILE})
elif [ ! -z "$POSTGRES_PASSWORD" ]; then
    echo "Postgres password specified"
else
    echo "Postgres password file not specified!"
    exit 1
fi

if [ "$POSTGRES_PASSWORD" -eq 0 ]; then
    echo "Using postgres connection without password"
    export TON_INDEXER_PG_DSN="postgresql://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DBNAME}"
else
    echo "Using postgres connection with password"
    export TON_INDEXER_PG_DSN="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DBNAME}"
fi
printenv

INDEX_ARGS=""
case $TON_INDEXER_IS_TESTNET in 
    y|yes|t|true|on|1)
        echo "Using testnet"
        INDEX_ARGS="$INDEX_ARGS -testnet"
        ;;
    *) ;;
esac

if [ ! -z "$TON_INDEXER_TON_HTTP_API_ENDPOINT" ]; then
    INDEX_ARGS="$INDEX_ARGS -v2 ${TON_INDEXER_TON_HTTP_API_ENDPOINT}"
fi

echo "Args: $INDEX_ARGS"

ton-index-go -pg $TON_INDEXER_PG_DSN -bind ":8081" $INDEX_ARGS $@
