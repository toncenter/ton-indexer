#!/bin/bash
set -e

if [[ ! $# -eq "3" ]]; then
    echo "Exactly 3 arguments required: container name, PostgreSQL and Redis connection string."
    exit 1
fi

CONTAINER_NAME=${1}
PG_DSN=${2}
REDIS_DSN=${3}

docker build -t ton-index-event-classifier:devel -f indexer/classifier.Dockerfile indexer

docker rm --force ${CONTAINER_NAME}
docker run -e TON_INDEXER_PG_DSN="$PG_DSN" \
           -e TON_REDIS_MONGO_DSN="REDIS_DSN" \
           -e TQDM_NCOLS=0 \
           -e TQDM_POSITION=-1 \
           --restart unless-stopped \
           --name ${CONTAINER_NAME} \
           --network host \
           --entrypoint /usr/local/bin/python \
           -it ton-index-event-classifier:devel -- /app/event_classifier.py
