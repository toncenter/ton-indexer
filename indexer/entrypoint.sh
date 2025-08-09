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

export TON_INDEXER_PG_DSN="postgresql+asyncpg://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
export TQDM_NCOLS=0
export TQDM_POSITION=-1
printenv
ls -la

/app/event_classifier.py $@
