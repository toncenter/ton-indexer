#!/usr/bin/env bash
# A test stand of fireton on the store host, next to the production
# units and apart from them: ton-tvm-router, and substreams tier1 and tier2 of
# fireton in two processes, all on localhost but tier1 at TIER1_ADDR, which
# can be a private address. tier2 has no router address of its own: it gets
# it from tier1. The router has the stub backend, or the ton-tvm-executor at
# EXECUTOR_ADDR.
#
#   sudo ./stand.sh start [stub-lag]   # e.g. 8s: blocks are reached 8 s after their gen_utime
#   sudo EXECUTOR_ADDR=192.0.2.10:10060 ./stand.sh start
#   sudo EXECUTOR_ADDR=192.0.2.10:10060 TIER1_ADDR=192.0.2.20:10041 ./stand.sh start
#   sudo TIER1_SUBREQUESTS=90 STAND_MEMORY_MAX=160G STAND_CPU_WEIGHT=30 ./stand.sh start
#   sudo ./stand.sh restart-router [stub-lag]
#   sudo ./stand.sh stop
#   sudo ./stand.sh status
#
# Binaries in $root/bin (fireton, ton-tvm-router), state in $root.
# tier1: TIER1_ADDR (127.0.0.1:10041), tier2: 10042, router: 10040; metrics on
# 9141, 9142, 9140. TIER1_SUBREQUESTS (4) is how many segments of a request
# tier2 runs at once, within the workers of tier1's session pool (100 by
# default, as many as TIER1_SUBREQUESTS above it); STAND_MEMORY_MAX (32G)
# bounds each process, and
# STAND_CPU_WEIGHT (100, as the production units) below 100 yields the CPU to
# them. STAND_WASM_RUNTIME sets SUBSTREAMS_WASM_RUNTIME (wasmtime, or wazero),
# STAND_GOGC sets GOGC of the processes.
set -euo pipefail

root=/srv/scratch/fireton-test
store="s3://localhost:3900/firehose"
units=(fh-ton-tvm-router-test fh-fireton-tier1-test fh-fireton-tier2-test)

common=(
  --config-file= --log-to-file=false --common-first-streamable-block=1
  "--common-merged-blocks-store-url=$store/merged-blocks?region=garage&insecure=true"
  "--common-one-block-store-url=$store/one-blocks?region=garage&insecure=true"
  "--common-forked-blocks-store-url=$store/forked-blocks?region=garage&insecure=true"
  --advertise-chain-name=ton --advertise-block-id-encoding=hex --ignore-advertise-validation
  --substreams-state-store-url=$root/states
)

run() {
  local unit=$1
  shift
  systemd-run --unit="$unit" --collect -p User=firecore -p Group=firecore \
    -p EnvironmentFile=/etc/firecore/garage.env -p WorkingDirectory=$root \
    -p MemoryMax="${STAND_MEMORY_MAX:-32G}" -p CPUWeight="${STAND_CPU_WEIGHT:-100}" -p LimitNOFILE=1048576 \
    ${STAND_WASM_RUNTIME:+-p Environment=SUBSTREAMS_WASM_RUNTIME=$STAND_WASM_RUNTIME} \
    ${STAND_GOGC:+-p Environment=GOGC=$STAND_GOGC} "$@"
}

start_router() {
  run fh-ton-tvm-router-test $root/bin/ton-tvm-router \
    --listen-addr=127.0.0.1:10040 --metrics-addr=127.0.0.1:9140 --stub-lag="${1:-0s}" \
    ${EXECUTOR_ADDR:+--executor-addr=$EXECUTOR_ADDR}
}

subrequests=${TIER1_SUBREQUESTS:-4}
workers=$((subrequests > 100 ? subrequests : 100))
sessions="local://?max_sessions_per_user=3&max_sessions=30&max_workers=$workers"
sessions+="&max_workers_per_user=$workers&max_workers_per_session=$workers"

case "${1:-}" in
  start)
    start_router "${2:-0s}"
    run fh-fireton-tier2-test $root/bin/fireton start substreams-tier2 "${common[@]}" \
      --data-dir=$root/tier2 --common-live-blocks-addr= \
      --substreams-tier2-grpc-listen-addr=127.0.0.1:10042 \
      --metrics-listen-addr=127.0.0.1:9142 --pprof-listen-addr=127.0.0.1:6072 \
      --log-level-switcher-listen-addr=127.0.0.1:1072
    run fh-fireton-tier1-test $root/bin/fireton start substreams-tier1 "${common[@]}" \
      --data-dir=$root/tier1 --common-live-blocks-addr=127.0.0.1:10014 \
      --substreams-tier1-block-type=sf.ton.type.v1.Block \
      --substreams-tier1-grpc-listen-addr="${TIER1_ADDR:-127.0.0.1:10041}" \
      --substreams-tier1-subrequests-endpoint=127.0.0.1:10042 \
      --substreams-tier1-max-subrequests="$subrequests" --common-session-plugin="$sessions" \
      --substreams-ton-router-addr=127.0.0.1:10040 \
      --metrics-listen-addr=127.0.0.1:9141 --pprof-listen-addr=127.0.0.1:6071 \
      --log-level-switcher-listen-addr=127.0.0.1:1071
    ;;
  restart-router)
    systemctl stop fh-ton-tvm-router-test || true
    start_router "${2:-0s}"
    ;;
  stop)
    systemctl stop "${units[@]}" || true
    ;;
  status)
    systemctl --no-pager status "${units[@]}" | grep -E "●|Active:"
    ;;
  *)
    echo "usage: $0 start [stub-lag] | restart-router [stub-lag] | stop | status" >&2
    exit 2
    ;;
esac
