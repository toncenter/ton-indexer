#!/bin/bash
set -e

# prepare pgpass file
PW="$(tr -d '\r\n' < /run/secrets/postgres_password)"
tmp="$(mktemp)"
printf '*:*:*:*:%s\n' "$PW" > "$tmp"
chmod 0600 "$tmp"
export PGPASSFILE="$tmp"

ulimit -n 1000000
echo "Running command: $@"
exec "$@"