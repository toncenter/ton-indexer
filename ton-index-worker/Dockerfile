FROM ubuntu:22.04 as builder
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y build-essential cmake clang openssl libssl-dev zlib1g-dev \
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
RUN make -j$(nproc) ton-index-postgres ton-index-clickhouse ton-smc-scanner ton-integrity-checker ton-trace-emulator

FROM ubuntu:22.04
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev libhiredis-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY scripts/entrypoint.sh /entrypoint.sh
COPY --from=builder /app/build/external/libpqxx/src/libpqxx.so /usr/lib/libpqxx.so
COPY --from=builder /app/build/external/libpqxx/src/libpqxx-*.so /usr/lib/
COPY --from=builder /app/build/ton-index-postgres/ton-index-postgres /usr/bin/ton-index-postgres
COPY --from=builder /app/build/ton-index-clickhouse/ton-index-clickhouse /usr/bin/ton-index-clickhouse
COPY --from=builder /app/build/ton-smc-scanner/ton-smc-scanner /usr/bin/ton-smc-scanner
COPY --from=builder /app/build/ton-integrity-checker/ton-integrity-checker /usr/bin/ton-integrity-checker
COPY --from=builder /app/build/ton-trace-emulator/ton-trace-emulator /usr/bin/ton-trace-emulator

ENTRYPOINT [ "/entrypoint.sh" ]
