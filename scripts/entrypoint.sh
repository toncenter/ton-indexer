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

POSTGRES_HOST_IP=$(dig +short ${POSTGRES_HOST})
if [[ -z "$POSTGRES_HOST_IP" ]]; then
    POSTGRES_HOST_IP=$POSTGRES_HOST
fi
echo "Postgres host: $POSTGRES_HOST (ip: $POSTGRES_HOST_IP)"

ulimit -n 1000000
tondb-scanner --host $POSTGRES_HOST_IP \
              --port $POSTGRES_PORT \
              --user $POSTGRES_USER \
              --password $POSTGRES_PASSWORD \
              --dbname $POSTGRES_DBNAME \
              --db /tondb $@
