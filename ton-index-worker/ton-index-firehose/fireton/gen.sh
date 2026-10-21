#!/usr/bin/env bash
# Generates the Go types of ../proto into pb/. Needs protoc, protoc-gen-go
# v1.36.12 and protoc-gen-go-grpc v1.5.1 on PATH:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
set -euo pipefail
cd "$(dirname "$0")"

module=github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton
# The protos have no go_package: their Go packages are given here.
mappings=(
  "sf/ton/type/v1/type.proto=$module/pb/sf/ton/type/v1;pbton"
  "ton/state/v1/state.proto=$module/pb/ton/state/v1;pbstate"
  "ton/tvm/v1/tvm.proto=$module/pb/ton/tvm/v1;pbtvm"
  "ton/tvm/v1/executor.proto=$module/pb/ton/tvm/v1;pbtvm"
)
go_opts=(--go_opt=module=$module)
grpc_opts=(--go-grpc_opt=module=$module)
for mapping in "${mappings[@]}"; do
  go_opts+=(--go_opt=M$mapping)
  grpc_opts+=(--go-grpc_opt=M$mapping)
done

rm -rf pb
protoc --proto_path=../proto \
  --go_out=. "${go_opts[@]}" \
  --go-grpc_out=. "${grpc_opts[@]}" \
  sf/ton/type/v1/type.proto ton/state/v1/state.proto ton/tvm/v1/tvm.proto ton/tvm/v1/executor.proto
