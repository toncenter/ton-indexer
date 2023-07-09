#!/bin/bash
set -e

docker compose run --build --rm -v `pwd`/indexer/alembic/versions:/app/alembic/versions/ alembic alembic revision --autogenerate -m ${1:?}
