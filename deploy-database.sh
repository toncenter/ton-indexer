#!/bin/bash
set -e

docker compose up -d --wait postgres

rm -f indexer/alembic/versions/*.py
docker compose run --build --rm -v `pwd`/indexer/alembic/versions:/app/alembic/versions/ alembic alembic revision --autogenerate -m "First migration"
docker compose up --build alembic
