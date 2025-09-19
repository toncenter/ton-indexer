# build c++ library ton-marker
FROM ubuntu:22.04 AS cpp-builder
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update -y && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt-get update -y \
    && apt-get install -y build-essential cmake clang openssl libssl-dev zlib1g-dev \
                   gperf wget git curl ccache libmicrohttpd-dev liblz4-dev \
                   pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev python3-dev libpq-dev \
                   automake libjemalloc-dev lsb-release software-properties-common gnupg \
                   autoconf libtool \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

# building
COPY external/ /app/external/
COPY pgton/ /app/pgton/
COPY celldb-migrate/ /app/celldb-migrate/
COPY ton-index-clickhouse/ /app/ton-index-clickhouse/
COPY ton-index-postgres/ /app/ton-index-postgres/
COPY ton-integrity-checker/ /app/ton-integrity-checker/
COPY ton-smc-scanner/ /app/ton-smc-scanner/
COPY ton-trace-emulator/ /app/ton-trace-emulator/
COPY ton-trace-task-emulator/ /app/ton-trace-task-emulator/
COPY tondb-scanner/ /app/tondb-scanner/
COPY CMakeLists.txt /app/

WORKDIR /app/build
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN make -j$(nproc) ton-marker-core ton-marker ton-marker-cli


# build main application ton-index-go
FROM golang:bookworm AS go-builder

RUN go install github.com/swaggo/swag/cmd/swag@latest

ADD index/ /go/app/index/
ADD main.go /go/app/main.go
ADD go.mod /go/app/go.mod
ADD go.sum /go/app/go.sum
COPY --from=cpp-builder /app/build/ton-marker/libton-marker* /usr/lib/
RUN cd /go/app && swag init && go build -o ton-index-go ./main.go

FROM ubuntu:jammy
RUN apt-get update && apt install --yes bash curl && rm -rf /var/lib/apt/lists/*

COPY --from=go-builder /go/app/ton-index-go /usr/local/bin/ton-index-go
COPY entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]
