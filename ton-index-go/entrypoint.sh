#!/bin/bash
set -e

# prepare pgpass file
if [ -n "$POSTGRES_PASSWORD_FILE" ]; then
    echo "Using postgres password from POSTGRES_PASSWORD_FILE"
    if [ ! -f "$POSTGRES_PASSWORD_FILE" ]; then
        echo "ERROR: POSTGRES_PASSWORD_FILE does not exist: $POSTGRES_PASSWORD_FILE" >&2
        exit 1
    fi
    PW="$(tr -d '\r\n' < "$POSTGRES_PASSWORD_FILE")"
elif [ -n "$POSTGRES_PASSWORD" ]; then
    echo "Using postgres password from POSTGRES_PASSWORD env variable"
    PW="$POSTGRES_PASSWORD"
else
    echo "ERROR: Password not supplied. Set POSTGRES_PASSWORD or POSTGRES_PASSWORD_FILE" >&2
    exit 1
fi
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

ton-index-go -pg "postgresql://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}" -bind ":8081" $INDEX_ARGS $@
