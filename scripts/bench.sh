#!/usr/bin/env bash
# Runs the benchmark matrix recorded in RESULTS.md against a local 5-node
# cluster (one Raft group, all nodes on this machine). Every run starts a
# fresh cluster. Raw JSON reports go to $OUT (default bench-results/).
#
#   scripts/bench.sh [reps] [duration]
set -euo pipefail
cd "$(dirname "$0")/.."
REPS=${1:-3}
DUR=${2:-30s}
OUT=${OUT:-bench-results}
mkdir -p "$OUT"
BENCH=bin/strata-bench
[ -x "$BENCH" ] || BENCH=bin/strata-bench.exe
NODES=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003,4=127.0.0.1:7004,5=127.0.0.1:7005

run() { # name, cluster flags, bench flags
  local name=$1 cflags=$2 bflags=$3
  for rep in $(seq 1 "$REPS"); do
    # shellcheck disable=SC2086
    scripts/local-cluster.sh start unsharded $cflags >/dev/null
    sleep 1
    # shellcheck disable=SC2086
    "$BENCH" -nodes "$NODES" -servers 1,2,3,4,5 -duration "$DUR" -warmup 5s $bflags \
      -out "$OUT/$name-r$rep.json" >/dev/null 2>"$OUT/$name-r$rep.log"
    scripts/local-cluster.sh stop
    printf '%-28s rep %d: %s\n' "$name" "$rep" \
      "$(grep -E '"(ops_per_sec|p50_ms|p99_ms)"' "$OUT/$name-r$rep.json" | tr -d ' \n')"
  done
}

# 90% reads / 10% writes over 10k preloaded keys, 100-byte values.
for c in 64 128 256; do
  run "mixed-90-10-c$c" "" "-clients $c -read-ratio 0.9"
done
# Write-only, with and without WAL fsync batching (group commit).
for c in 64 256; do
  run "write-batched-c$c" "" "-clients $c -read-ratio 0 -preload=false"
  run "write-unbatched-c$c" "-no-fsync-batching" "-clients $c -read-ratio 0 -preload=false"
done
