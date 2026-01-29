# BUILD Stages
## build core functionality
FROM ubuntu:24.04 AS core-builder
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update -y && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt-get update -y \
    && apt-get install -y build-essential cmake clang-20 openssl libssl-dev zlib1g-dev \
                   gperf wget git curl ccache libmicrohttpd-dev liblz4-dev \
                   pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev python3-dev libpq-dev \
                   automake libjemalloc-dev lsb-release software-properties-common gnupg \
                   autoconf libtool \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/
COPY ton-index-worker/external/ /app/external/
COPY ton-index-worker/pgton/ /app/pgton/
COPY ton-index-worker/celldb-migrate/ /app/celldb-migrate/
COPY ton-index-worker/ton-index-clickhouse/ /app/ton-index-clickhouse/
COPY ton-index-worker/ton-index-postgres/ /app/ton-index-postgres/
COPY ton-index-worker/ton-integrity-checker/ /app/ton-integrity-checker/
COPY ton-index-worker/ton-smc-scanner/ /app/ton-smc-scanner/
COPY ton-index-worker/ton-trace-emulator/ /app/ton-trace-emulator/
COPY ton-index-worker/ton-trace-task-emulator/ /app/ton-trace-task-emulator/
COPY ton-index-worker/tondb-scanner/ /app/tondb-scanner/
COPY ton-index-worker/ton-marker/ /app/ton-marker/
COPY ton-index-worker/CMakeLists.txt /app/

WORKDIR /app/build
ENV CC=clang-20
ENV CXX=clang++-20
RUN touch /app/suppression_mappings.txt && cmake -DCMAKE_BUILD_TYPE=Release -DPORTABLE=1 .. && make -j$(nproc) ton-index-postgres ton-index-postgres-migrate ton-index-clickhouse ton-smc-scanner \
     ton-integrity-checker ton-trace-emulator ton-trace-task-emulator ton-marker-cli ton-marker-core ton-marker


## build index api service ton-index-go
FROM golang:trixie AS index-api-builder

RUN apt-get update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN go install github.com/swaggo/swag/cmd/swag@latest

ADD ton-index-go/index/ /go/app/index/
ADD ton-index-go/main.go /go/app/main.go
ADD ton-index-go/go.mod /go/app/go.mod
ADD ton-index-go/go.sum /go/app/go.sum
COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=core-builder /app/ton-marker/src/wrapper.h /usr/local/include/wrapper.h
RUN cd /go/app && swag init && go build -o ton-index-go ./main.go


## build emulate api service ton-emulate-go
FROM golang:trixie AS emulate-api-builder

RUN apt-get update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN go install github.com/swaggo/swag/cmd/swag@latest

ADD ton-index-go/ /go/ton-index-go/
ADD ton-emulate-go/models/ /go/app/models/
ADD ton-emulate-go/main.go /go/app/main.go
ADD ton-emulate-go/go.mod /go/app/go.mod
ADD ton-emulate-go/go.sum /go/app/go.sum
COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=core-builder /app/ton-marker/src/wrapper.h /usr/local/include/wrapper.h
RUN cd /go/app && swag init && go build -o ton-emulate-go ./main.go

## build metadata cache service ton-metadata-cache
FROM golang:trixie AS metadata-cache-builder

RUN apt-get update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN go install github.com/swaggo/swag/cmd/swag@latest

ADD ton-index-go/ /go/ton-index-go/
ADD ton-metadata-cache/cache/ /go/app/cache/
ADD ton-metadata-cache/repl/ /go/app/repl/
ADD ton-metadata-cache/models/ /go/app/models/
ADD ton-metadata-cache/loader/ /go/app/loader/
ADD ton-metadata-cache/main.go /go/app/main.go
ADD ton-metadata-cache/db.go /go/app/db.go
ADD ton-metadata-cache/handler.go /go/app/handler.go
ADD ton-metadata-cache/go.mod /go/app/go.mod
ADD ton-metadata-cache/go.sum /go/app/go.sum
COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=core-builder /app/ton-marker/src/wrapper.h /usr/local/include/wrapper.h
RUN cd /go/app && go build -o ton-metadata-cache .


## build metadata fetcher service ton-metadata-fetcher
FROM golang:trixie AS metadata-fetcher-builder

ADD ton-metadata-fetcher/images.go /go/app/images.go
ADD ton-metadata-fetcher/ipfs.go /go/app/ipfs.go
ADD ton-metadata-fetcher/main.go /go/app/main.go
ADD ton-metadata-fetcher/overrides.go /go/app/overrides.go
ADD ton-metadata-fetcher/go.mod /go/app/go.mod
ADD ton-metadata-fetcher/go.sum /go/app/go.sum

WORKDIR /go/app
RUN go build -o ton-metadata-fetcher ./*.go


# IMAGE stages
## index worker service image
FROM ubuntu:24.04 AS index-worker
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update -y && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt-get update -y \
    && apt install -y dnsutils libpq5 libsecp256k1-1 libsodium23 libhiredis1.1.0 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY ton-index-worker/scripts/entrypoint.sh /app/entrypoint.sh
COPY --from=core-builder /app/build/ton-index-postgres/ton-index-postgres /usr/bin/ton-index-postgres
COPY --from=core-builder /app/build/ton-index-postgres/ton-index-postgres-migrate /usr/bin/ton-index-postgres-migrate
COPY --from=core-builder /app/build/ton-index-clickhouse/ton-index-clickhouse /usr/bin/ton-index-clickhouse
COPY --from=core-builder /app/build/ton-smc-scanner/ton-smc-scanner /usr/bin/ton-smc-scanner
COPY --from=core-builder /app/build/ton-integrity-checker/ton-integrity-checker /usr/bin/ton-integrity-checker
COPY --from=core-builder /app/build/ton-trace-emulator/ton-trace-emulator /usr/bin/ton-trace-emulator
COPY --from=core-builder /app/build/ton-trace-task-emulator/ton-trace-task-emulator /usr/bin/ton-trace-task-emulator
COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=core-builder /app/build/ton-marker/ton-marker-cli /usr/bin/ton-marker-cli

ENTRYPOINT [ "/app/entrypoint.sh" ]


## index api service image
FROM ubuntu:24.04 AS index-api
RUN apt-get update \
    && apt install --yes dnsutils libpq5 libsecp256k1-1 libsodium23 libhiredis1.1.0 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=index-api-builder /go/app/ton-index-go /usr/local/bin/ton-index-go
COPY ton-index-go/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]


## emulate api service image
FROM ubuntu:24.04 AS emulate-api
RUN apt-get update \
    && apt install --yes curl dnsutils libpq5 libsecp256k1-1 libsodium23 libhiredis1.1.0 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=emulate-api-builder /go/app/ton-emulate-go /usr/local/bin/ton-emulate-go
COPY ton-emulate-go/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]

## metadata cache service image
FROM ubuntu:24.04 AS metadata-cache
RUN apt-get update \
    && apt install --yes curl dnsutils libpq5 libsecp256k1-1 libsodium23 libhiredis1.1.0 \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY --from=core-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=metadata-cache-builder /go/app/ton-metadata-cache /usr/local/bin/ton-metadata-cache
COPY ton-metadata-cache/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]


## metadata fetcher service image
FROM ubuntu:24.04 AS metadata-fetcher
RUN apt-get update && apt install --yes curl && rm -rf /var/lib/apt/lists/*

COPY --from=metadata-fetcher-builder /go/app/ton-metadata-fetcher /usr/local/bin/ton-metadata-fetcher
COPY ton-metadata-fetcher/entrypoint.sh /app/entrypoint.sh
COPY ton-metadata-fetcher/metadata_overrides.json /app/metadata_overrides.json

ENTRYPOINT [ "/app/entrypoint.sh" ]


## classifier service image
FROM python:3.12-trixie AS classifier
ADD indexer/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
COPY indexer/ /app/
COPY indexer/files/* /app

WORKDIR /app
ENV C_FORCE_ROOT=1
ENTRYPOINT [ "/app/entrypoint.sh" ]
