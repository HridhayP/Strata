#!/usr/bin/env bash
# Regenerates Go code from the protobuf definitions in proto/.
# Requires protoc, protoc-gen-go and protoc-gen-go-grpc on PATH.
set -euo pipefail
cd "$(dirname "$0")/.."
PATH="$PATH:$(go env GOPATH)/bin"
protoc --proto_path=proto \
  --go_out=. --go_opt=module=github.com/HridhayP/strata \
  --go-grpc_out=. --go-grpc_opt=module=github.com/HridhayP/strata \
  proto/*.proto
