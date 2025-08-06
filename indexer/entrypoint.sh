#!/bin/bash
set -e

# prepare pgpass file
PW="$(tr -d '\r\n' < /run/secrets/postgres_password)"
tmp="$(mktemp)"
printf '*:*:*:*:%s\n' "$PW" > "$tmp"
chmod 0600 "$tmp"
export PGPASSFILE="$tmp"

export TON_INDEXER_PG_DSN="postgresql+asyncpg://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DBNAME}"
export TQDM_NCOLS=0
export TQDM_POSITION=-1
printenv
ls -la

/app/event_classifier.py $@
