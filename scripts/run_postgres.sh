#!/bin/bash
set -e

POSTGRES_PASSWORD=${1:?Please pass 1 argument: postgres password}

docker volume create ton-index-cpp-data
docker run -d -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" -e POSTGRES_USER=postgres -e POSTGRES_DB=ton_index -v ton-index-cpp-data:/var/lib/postgresql/data --restart=always --name=ton-index-cpp-postgres -p 5433:5432 postgres:latest
