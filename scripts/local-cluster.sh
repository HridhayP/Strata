#!/usr/bin/env bash
# Starts or stops a local Strata cluster of N processes on 127.0.0.1.
#
#   scripts/local-cluster.sh start [unsharded|sharded] [extra server flags...]
#   scripts/local-cluster.sh stop
#
# unsharded: 5 nodes forming one KV group (g1) that serves every shard.
# sharded:   6 nodes; controller on 1-3; group 1 = 1,2,3; group 2 = 4,5,6.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${RUN_DIR:-tmp/cluster}
BIN=bin/strata-server
[ -x "$BIN" ] || BIN=bin/strata-server.exe

cmd=${1:-start}
mode=${2:-unsharded}
shift $(( $# >= 2 ? 2 : $# )) || true

stop() {
  if [ -d "$RUN" ]; then
    for pidf in "$RUN"/*.pid; do
      [ -e "$pidf" ] || continue
      kill "$(cat "$pidf")" 2>/dev/null || true
      rm -f "$pidf"
    done
  fi
}

case "$cmd" in
stop)
  stop
  exit 0
  ;;
start)
  stop
  rm -rf "$RUN"
  mkdir -p "$RUN"
  if [ "$mode" = sharded ]; then
    n=6
    args=(-ctrl 1,2,3 -groups "1=1,2,3;2=4,5,6")
  else
    n=5
    args=(-groups "1=1,2,3,4,5")
  fi
  nodes=""
  for i in $(seq 1 "$n"); do nodes+="${nodes:+,}$i=127.0.0.1:$((7000 + i))"; done
  for i in $(seq 1 "$n"); do
    extra=()
    [ "$mode" = sharded ] && [ "$i" = 1 ] && extra=(-bootstrap)
    "$BIN" -id "$i" -nodes "$nodes" "${args[@]}" -data "$RUN/data/$i" \
      -metrics "127.0.0.1:$((9100 + i))" "${extra[@]}" "$@" >"$RUN/node$i.log" 2>&1 &
    echo $! >"$RUN/node$i.pid"
  done
  echo "nodes: $nodes"
  ;;
*)
  echo "usage: $0 start [unsharded|sharded] [flags...] | stop" >&2
  exit 2
  ;;
esac
