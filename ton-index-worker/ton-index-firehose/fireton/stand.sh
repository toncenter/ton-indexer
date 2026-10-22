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
#   sudo ./stand.sh restart-router [stub-lag]
#   sudo ./stand.sh stop
#   sudo ./stand.sh status
#
# Binaries in $root/bin (fireton, ton-tvm-router), state in $root.
# tier1: TIER1_ADDR (127.0.0.1:10041), tier2: 10042, router: 10040; metrics on
# 9141, 9142, 9140.
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
    -p MemoryMax=32G -p LimitNOFILE=1048576 "$@"
}

start_router() {
  run fh-ton-tvm-router-test $root/bin/ton-tvm-router \
    --listen-addr=127.0.0.1:10040 --metrics-addr=127.0.0.1:9140 --stub-lag="${1:-0s}" \
    ${EXECUTOR_ADDR:+--executor-addr=$EXECUTOR_ADDR}
}

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
