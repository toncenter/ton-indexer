#!/bin/bash
set -e

# dir for secrets
mkdir -p private

# help
function usage() {
    echo 'Supported argumets:'
    echo ' -h --help                Show this message'
    echo '    --worker              Do configure TON Index worker'
    exit
}

# read arguments
WORKER=0
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage; exit 1;;
        --worker)
            WORKER=1; shift;;
        -*|--*)
            echo "Unknown option: '$1'"; exit 1 ;;
        *)
            POSITIONAL_ARGS+=("$1"); shift;;
    esac
done
set -- "${POSITIONAL_ARGS[@]}"

# interactive config
cat <<EOF > .env
# TON Indexer config
POSTGRES_HOST=${POSTGRES_HOST:-postgres}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD_FILE=private/postgres_password
POSTGRES_DBNAME=${POSTGRES_DBNAME:-ton_index}
POSTGRES_PUBLISH_PORT=${POSTGRES_PUBLISH_PORT:-5432}

TON_INDEXER_API_ROOT_PATH=
TON_INDEXER_API_PORT=8081
TON_INDEXER_API_TITLE=TON Indexer
TON_INDEXER_WORKERS=4

TON_INDEXER_TON_HTTP_API_ENDPOINT=${TON_INDEXER_TON_HTTP_API_ENDPOINT}

TON_WORKER_DBROOT=

EOF

if [[ "$WORKER" -eq 1 ]]; then
echo "Configure Worker"

cat <<EOF >> .env
TON_WORKER_DBROOT=${TON_WORKER_DBROOT:-/var/ton-work/db/}
TON_WORKER_FROM=${TON_WORKER_FROM:-1}
TON_WORKER_MAX_PARALLEL_TASKS=${TON_WORKER_MAX_PARALLEL_TASKS:-1024}
TON_WORKER_INSERT_BATCH_SIZE=${TON_WORKER_INSERT_BATCH_SIZE:-512}
TON_WORKER_INSERT_PARALLEL_ACTORS=${TON_WORKER_INSERT_PARALLEL_ACTORS:-3}
EOF
fi
