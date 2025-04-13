#!/bin/bash
set -e

function usage() {
    echo "Usage:"
    exit
}


POSITIONAL_ARGS=()
FORCE_BUILD=0

TASK_ARGS=

while [[ $# -gt 0 ]]; do
    case $1 in 
        -h|--help)
            usage
            ;;
        -f|--force)
            FORCE_BUILD=1
            shift;;
        -*|--*)
            echo "Adding argument '$1 $2' to daemon"
            TASK_ARGS="${TASK_ARGS} $1 $2"
            shift; shift;;
        *)
            POSITIONAL_ARGS+=($1)
            shift;;
    esac
done

# install libraries
sudo apt update
sudo apt install -y build-essential cmake clang openssl libssl-dev zlib1g-dev \
                    gperf wget git curl libreadline-dev ccache libmicrohttpd-dev liblz4-dev \
                    pkg-config libsecp256k1-dev libsodium-dev libhiredis-dev python3-dev libpq-dev ninja-build

# build
if [[ $FORCE_BUILD -eq "1" ]]; then
    echo "WARNING! Force building binary"
    rm -rf ./build
fi

if [[ -f "./build" ]]; then
    echo "Directory build exists"
else
    mkdir -p build
    cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=off -GNinja -S . -B ./build
    ninja -C ./build -j$(nproc) ton-index-postgres
    sudo cmake --install build/
fi

# setup daemon
echo "Task args: \'$TASK_ARGS\'"
cat <<EOF | sudo tee /etc/systemd/system/index-worker.service
[Unit]
Description = ton index worker service
After = network.target

[Service]
Type = simple
Restart = always
RestartSec = 20
ExecStart=/bin/sh -c '/usr/bin/ton-index-postgres $TASK_ARGS 2>&1'
ExecStopPost = /bin/echo "index-worker service down"
User = root
Group = root
LimitNOFILE = infinity
LimitNPROC = infinity
LimitMEMLOCK = infinity

[Install]
WantedBy = multi-user.target
EOF

# enable service
sudo systemctl daemon-reload
sudo systemctl enable index-worker.service
sudo systemctl restart index-worker.service
