FROM ubuntu:22.04 as builder
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y build-essential cmake clang openssl libssl-dev zlib1g-dev \
                   gperf wget git curl ccache libmicrohttpd-dev \
                   pkg-config libsecp256k1-dev libsodium-dev python3-dev libpq-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

# building
COPY . /app/

WORKDIR /app/build
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN make -j$(nproc)

FROM ubuntu:22.04
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY scripts/entrypoint.sh /entrypoint.sh
COPY --from=builder /app/build/ton-index-postgres/ton-index-postgres /usr/bin/ton-index-postgres
COPY --from=builder /app/build/ton-index-postgres-v2/ton-index-postgres-v2 /usr/bin/ton-index-postgres-v2
COPY --from=builder /app/build/ton-index-clickhouse/ton-index-clickhouse /usr/bin/ton-index-clickhouse
COPY --from=builder /app/build/ton-smc-scanner/ton-smc-scanner /usr/bin/ton-smc-scanner
COPY --from=builder /app/build/ton-integrity-checker/ton-integrity-checker /usr/bin/ton-integrity-checker
COPY --from=builder /app/build/ton-trace-emulator/ton-trace-emulator /usr/bin/ton-trace-emulator

ENTRYPOINT [ "/entrypoint.sh" ]
