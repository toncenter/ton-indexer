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
    echo "Postgres password file not specified!"
    exit 1
fi

echo "Postgres host: $POSTGRES_HOST"
POSTGRES_HOST_IP=$(dig +short ${POSTGRES_HOST})

ulimit -n 1000000
tondb-scanner -D $POSTGRES_DBROOT -H $POSTGRES_HOST_IP -U $POSTGRES_USER -P $POSTGRES_PASSWORD -B $POSTGRES_DBNAME $@
