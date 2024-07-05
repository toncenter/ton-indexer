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
RUN cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=off ..
RUN make -j$(nproc) ton-index-postgres

FROM ubuntu:22.04
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y dnsutils libpq-dev libsecp256k1-dev libsodium-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY scripts/entrypoint.sh /entrypoint.sh
COPY --from=builder /app/build/ton-index-postgres/ton-index-postgres /usr/bin/ton-index-postgres

ENTRYPOINT [ "/entrypoint.sh" ]
