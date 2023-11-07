#!/bin/bash
set -e

if [[ ! $# -eq "2" ]]; then
    echo "Exactly 2 arguments required: container name and PostgreSQL connection string."
    exit 1
fi

CONTAINER_NAME=${1}
PG_DSN=${2}

docker build -t ton-index-event-detector:devel -f indexer/Dockerfile indexer

docker rm --force ${CONTAINER_NAME}
docker run -e TON_INDEXER_PG_DSN="$PG_DSN" \
           -e TQDM_NCOLS=0 \
           -e TQDM_POSITION=-1 \
           --restart unless-stopped \
           --name ${CONTAINER_NAME} \
           --entrypoint /bin/python3 \
           -itd ton-index-event-detector:devel -- /app/event_detector.py --verbose
