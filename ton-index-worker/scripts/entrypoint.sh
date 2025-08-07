#!/bin/bash
set -e

# postgres password
if [ ! -z "$POSTGRES_PASSWORD_FILE" ]; then
    echo "Postgres password file: ${POSTGRES_PASSWORD_FILE}"
    if [ ! -f "${POSTGRES_PASSWORD_FILE}" ]; then
        echo "Password file specified, but not found"
        exit 1
    fi
    POSTGRES_PASSWORD=$(cat ${POSTGRES_PASSWORD_FILE})
elif [ ! -z "$POSTGRES_PASSWORD" ]; then
    echo "Postgres password specified"
else
    echo "Warning: postgres password file not specified!"
fi

export PGPASSWORD=$POSTGRES_PASSWORD

ulimit -n 1000000
printenv
echo "Running binary ${TON_WORKER_BINARY}"
${TON_WORKER_BINARY} $@