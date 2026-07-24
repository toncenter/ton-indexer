#!/bin/bash
set -e

PG_ARGS=()
if [ -n "${POSTGRES_HOST:-}${POSTGRES_PORT:-}${POSTGRES_USER:-}${POSTGRES_DB:-}" ]; then
    if [ -z "${POSTGRES_HOST:-}" ] || [ -z "${POSTGRES_PORT:-}" ] || [ -z "${POSTGRES_USER:-}" ] || [ -z "${POSTGRES_DB:-}" ]; then
        echo "ERROR: PostgreSQL configuration is incomplete" >&2
        exit 1
    fi

    # prepare pgpass file only when the PostgreSQL backend is configured
    if [ -n "${POSTGRES_PASSWORD_FILE:-}" ]; then
        echo "Using postgres password from POSTGRES_PASSWORD_FILE"
        if [ ! -f "$POSTGRES_PASSWORD_FILE" ]; then
            echo "ERROR: POSTGRES_PASSWORD_FILE does not exist: $POSTGRES_PASSWORD_FILE" >&2
            exit 1
        fi
        PW="$(tr -d '\r\n' < "$POSTGRES_PASSWORD_FILE")"
    elif [ -n "${POSTGRES_PASSWORD:-}" ]; then
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
    PG_ARGS=(-pg "postgresql://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}")
else
    echo "PostgreSQL backend is not configured"
fi

INDEX_ARGS=()
case $TON_INDEXER_IS_TESTNET in 
    y|yes|t|true|on|1)
        echo "Using testnet"
        INDEX_ARGS+=(-testnet)
        ;;
    *) ;;
esac

if [ -n "${TON_INDEXER_IMGPROXY_BASEURL:-}" ]; then
    echo "imgproxy baseurl is specified"
    INDEX_ARGS+=(-imgproxy-baseurl "$TON_INDEXER_IMGPROXY_BASEURL")
fi

echo "Args: ${INDEX_ARGS[*]}"

exec ton-emulate-go "${PG_ARGS[@]}" -redis "${TON_INDEXER_EMULATE_REDIS_DSN:-redis://localhost:6379}" "${INDEX_ARGS[@]}" "$@"
