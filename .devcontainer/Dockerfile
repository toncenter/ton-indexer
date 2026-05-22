FROM ubuntu:24.04
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update -y && apt-get -y install tzdata && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN apt-get update -y \
    && apt-get install -y build-essential cmake clang-20 openssl libssl-dev zlib1g-dev \
                   gperf wget git curl ccache libmicrohttpd-dev liblz4-dev \
                   pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev python3-dev libpq-dev \
                   automake libjemalloc-dev lsb-release software-properties-common gnupg \
                   autoconf libtool sudo vim htop tmux golang-go \
                   libbrotli-dev libgsl-dev libblas-dev libzstd-dev libreadline-dev \
                   libnghttp2-dev libldap-dev libssh2-1-dev liburing-dev ccache nodejs npm \
    && rm -rf /var/lib/{apt,dpkg,cache,log}
RUN apt-get install -y ninja-build 
RUN npm install -g @openai/codex
RUN useradd -m -s /bin/bash dev \
    && echo "dev ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/dev \
    && chmod 0440 /etc/sudoers.d/dev
USER dev
WORKDIR /workspaces
