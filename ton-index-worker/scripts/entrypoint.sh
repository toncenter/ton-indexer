#!/bin/bash
set -e

# prepare pgpass file
if [ ! -r /run/secrets/postgres_password ]; then
    echo "Error: /run/secrets/postgres_password does not exist or is not readable." >&2  
    exit 1          
fi
PW="$(tr -d '\r\n' < /run/secrets/postgres_password)"
tmp="$(mktemp)"
printf '*:*:*:*:%s\n' "$PW" > "$tmp"
chmod 0600 "$tmp"
export PGPASSFILE="$tmp"

ulimit -n 1000000
echo "Running command: $@"
exec "$@"