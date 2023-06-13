FROM ubuntu:22.04 as builder
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y build-essential cmake clang openssl libssl-dev zlib1g-dev \
                   gperf wget git curl libreadline-dev ccache libmicrohttpd-dev \
                   pkg-config libsecp256k1-dev libsodium-dev python3-dev libpqxx-dev ninja-build \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

# building
COPY external/ /app/external/
COPY tondb-scanner/ /app/tondb-scanner/
COPY CMakeLists.txt /app/


WORKDIR /app/build
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN cmake --build . -j$(nproc) --target tondb-scanner

FROM ubuntu:22.04
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt update -y \
    && apt install -y libpqxx-dev dnsutils \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY scripts/entrypoint.sh /entrypoint.sh
COPY --from=builder /app/build/tondb-scanner/tondb-scanner /usr/bin/tondb-scanner

ENTRYPOINT [ "/entrypoint.sh" ]
# ENTRYPOINT [ "tondb-scanner" ]
# ENTRYPOINT [ "/bin/bash" ]
