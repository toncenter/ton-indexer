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
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN make -j$(nproc) ton-marker-core ton-marker


# build main application ton-index-go
FROM golang:bookworm AS go-builder

RUN apt-get update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN go install github.com/swaggo/swag/cmd/swag@latest

ADD ton-index-go/ /go/ton-index-go/
ADD ton-emulate-go/models/ /go/app/models/
ADD ton-emulate-go/main.go /go/app/main.go
ADD ton-emulate-go/go.mod /go/app/go.mod
ADD ton-emulate-go/go.sum /go/app/go.sum
COPY --from=cpp-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=cpp-builder /app/ton-marker/src/wrapper.h /usr/local/include/wrapper.h
RUN cd /go/app && swag init && go build -o ton-emulate-go ./main.go

FROM ubuntu:jammy
RUN apt-get update \
    && apt install --yes bash curl dnsutils libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY --from=cpp-builder /app/build/ton-marker/libton-marker* /usr/lib/
COPY --from=go-builder /go/app/ton-emulate-go /usr/local/bin/ton-emulate-go
COPY ton-emulate-go/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]
