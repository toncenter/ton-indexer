#!/bin/bash
set -e

# prepare pgpass file
PW="$(tr -d '\r\n' < /run/secrets/postgres_password)"
tmp="$(mktemp)"
printf '*:*:*:*:%s\n' "$PW" > "$tmp"
chmod 0600 "$tmp"
export PGPASSFILE="$tmp"

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

ton-index-go -pg "postgresql://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DBNAME}" -bind ":8081" $INDEX_ARGS $@
