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

export TQDM_NCOLS=0
export TQDM_POSITION=-1
if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "Using postgres connection without password"
    export TON_INDEXER_PG_DSN="${POSTGRES_DIALECT:-postgresql+asyncpg}://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DBNAME}"
else
    echo "Using postgres connection with password"
    export TON_INDEXER_PG_DSN="${POSTGRES_DIALECT:-postgresql+asyncpg}://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DBNAME}"
fi
printenv
ls -la

/app/event_classifier.py $@
