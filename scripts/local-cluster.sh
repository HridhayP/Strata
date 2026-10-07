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
  [ -d "$RUN" ] || return 0
  local pids=()
  for pidf in "$RUN"/*.pid; do
    [ -e "$pidf" ] || continue
    pids+=("$(cat "$pidf")")
    rm -f "$pidf"
  done
  [ ${#pids[@]} -gt 0 ] || return 0
  kill "${pids[@]}" 2>/dev/null || true
  # Wait for the processes to exit so their ports and files are free.
  for _ in $(seq 1 100); do
    kill -0 "${pids[@]}" 2>/dev/null || return 0
    sleep 0.1
  done
  kill -9 "${pids[@]}" 2>/dev/null || true
  sleep 0.5
}

# wait_ready blocks until every node answers /status and one is leader.
wait_ready() {
  local n=$1
  for _ in $(seq 1 100); do
    local up=0 leader=0
    for i in $(seq 1 "$n"); do
      if st=$(curl -sf "http://127.0.0.1:$((9100 + i))/status" 2>/dev/null); then
        up=$((up + 1))
        case $st in *'"leader"'*) leader=1 ;; esac
      fi
    done
    [ "$up" = "$n" ] && [ "$leader" = 1 ] && return 0
    sleep 0.2
  done
  echo "cluster did not become ready; see $RUN/node*.log" >&2
  return 1
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
  wait_ready "$n"
  echo "nodes: $nodes"
  ;;
*)
  echo "usage: $0 start [unsharded|sharded] [flags...] | stop" >&2
  exit 2
  ;;
esac
