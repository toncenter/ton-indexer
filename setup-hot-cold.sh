set -a
source .env
set +a
export PGPASSWORD="$PATRONI_POSTGRES_PASSWORD"

create_db() {
  local port="$1"
  psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -v db="$POSTGRES_DB" <<'SQL'
SELECT format('CREATE DATABASE %I', :'db')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db')\gexec
SQL
}

create_db 5000   # hot primary
create_db 5010   # cold primary


./build/ton-index-worker/ton-index-postgres/ton-index-postgres-migrate \
  --pg "host=127.0.0.1 port=5000 dbname=${POSTGRES_DB} user=postgres password=${PATRONI_POSTGRES_PASSWORD}"

./build/ton-index-worker/ton-index-postgres/ton-index-postgres-migrate \
  --pg "host=127.0.0.1 port=5010 dbname=${POSTGRES_DB} user=postgres password=${PATRONI_POSTGRES_PASSWORD}"