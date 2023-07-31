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
        --host)
            TASK_ARGS="${TASK_ARGS} --host $2"
            shift; shift;;
        --port)
            TASK_ARGS="${TASK_ARGS} --port $2"
            shift; shift;;
        --user)
            TASK_ARGS="${TASK_ARGS} --user $2"
            shift; shift;;
        --password)
            TASK_ARGS="${TASK_ARGS} --password $2"
            shift; shift;;
        --db)
            TASK_ARGS="${TASK_ARGS} --db $2"
            shift; shift;;
        --dbname)
            TASK_ARGS="${TASK_ARGS} --dbname $2"
            shift; shift;;
        --from)
            TASK_ARGS="${TASK_ARGS} --from $2"
            shift; shift;;
        --max-parallel)
            TASK_ARGS="${TASK_ARGS} --max-parallel-tasks $2"
            shift; shift;;
        --batch-size)
            TASK_ARGS="${TASK_ARGS} --insert-batch-size $2"
            shift; shift;;
        --insert-workers)
            TASK_ARGS="${TASK_ARGS} --insert-parallel-actors $2"
            shift; shift;;
        -f|--force)
            FORCE_BUILD=1
            shift;;
        -*|--*)
            echo "Unknown argument $1"
            exit 1;;
        *)
            POSITIONAL_ARGS+=($1)
            shift;;
    esac
done

# install libraries
sudo apt update
sudo apt install -y build-essential cmake clang openssl libssl-dev zlib1g-dev \
                    gperf wget git curl libreadline-dev ccache libmicrohttpd-dev \
                    pkg-config libsecp256k1-dev libsodium-dev python3-dev libpq-dev ninja-build

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
    ninja -C ./build -j$(nproc) tondb-scanner
    sudo cp ./build/tondb-scanner/tondb-scanner /usr/local/bin
fi

# setup daemon
echo "Task args: \'$TASK_ARGS\'"
cat <<EOF | sudo tee /etc/systemd/system/ton-index-worker.service
[Unit]
Description = ton index worker service
After = network.target

[Service]
Type = simple
Restart = always
RestartSec = 20
ExecStart=/bin/sh -c '/usr/local/bin/tondb-scanner $TASK_ARGS 2>&1 | /usr/bin/cronolog /var/log/ton-index-cpp/%%Y-%%m-%%d.log'
ExecStopPost = /bin/echo "ton-index-worker service down"
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
sudo systemctl enable ton-index-worker.service
sudo systemctl start ton-index-worker.service
