#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date +%Y%m%d-%H%M%S)-$$"
if [[ -n "${ELIXIRDB_DEMO_ROOT:-}" ]]; then
  run_root="$ELIXIRDB_DEMO_ROOT/replication-harness-$run_id"
  keep_root=1
else
  run_root="$project_root/tmp/replication-harness-$run_id"
  keep_root=""
fi
main_root="$run_root/web-data"
cli_root="$run_root/native-data"
state_root="$run_root/state"
main_config="$state_root/main.json"
cli_config="$state_root/cli.json"
ready_config="$state_root/ready.json"
server_port="${ELIXIRDB_DEMO_DB_PORT:-4100}"
cli_port="${ELIXIRDB_DEMO_CLI_PORT:-4101}"
web_port="${ELIXIRDB_DEMO_WEB_PORT:-4180}"
main_log="$run_root/web-node.log"
cli_log="$run_root/native-cli.log"
web_log="$run_root/ui-server.log"
main_pid=""
cli_pid=""
web_pid=""

mkdir -p "$main_root" "$cli_root" "$state_root"
rm -f -- "$main_config" "$cli_config" "$ready_config"

cleanup() {
  trap - EXIT INT TERM
  for pid in "$web_pid" "$cli_pid" "$main_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done

  deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    alive=0
    for pid in "$web_pid" "$cli_pid" "$main_pid"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        alive=1
      fi
    done

    if (( alive == 0 )); then
      break
    fi

    sleep 0.1
  done

  for pid in "$web_pid" "$cli_pid" "$main_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done

  for pid in "$web_pid" "$cli_pid" "$main_pid"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -z "$keep_root" ]]; then
    rm -rf -- "$run_root"
  else
    echo "Harness files kept at $run_root"
  fi
}

wait_for_file() {
  local path="$1"
  local deadline=$((SECONDS + 120))
  while [[ ! -s "$path" ]]; do
    for pid in "$main_pid" "$cli_pid"; do
      if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
        echo "A database node stopped before writing $path" >&2
        return 1
      fi
    done

    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $path" >&2
      return 1
    fi
    sleep 0.1
  done
}

trap cleanup EXIT INT TERM

echo "Starting ElixirDB replication harness"
echo "  Database HTTP: http://127.0.0.1:$server_port"
echo "  Native CLI HTTP wire: http://127.0.0.1:$cli_port"
echo "  Web UI: http://127.0.0.1:$web_port"

(
  cd "$project_root"
  MIX_ENV=dev \
  ELIXIR_DB_ROOT="$main_root" \
  ELIXIR_DB_PORT="$server_port" \
  DEMO_MAIN_CONFIG="$main_config" \
  DEMO_C_CONFIG="$cli_config" \
  DEMO_READY_CONFIG="$ready_config" \
  mix run --no-start demo/replication_harness/node.exs web
) >"$main_log" 2>&1 &
main_pid=$!

wait_for_file "$main_config"

(
  cd "$project_root"
  MIX_ENV=dev \
  ELIXIR_DB_ROOT="$cli_root" \
  ELIXIR_DB_PORT="$cli_port" \
  DEMO_C_CONFIG="$cli_config" \
  DEMO_READY_CONFIG="$ready_config" \
  mix run --no-start demo/replication_harness/node.exs cli
) >"$cli_log" 2>&1 &
cli_pid=$!

wait_for_file "$ready_config"

(
  cd "$project_root"
  DEMO_READY_CONFIG="$ready_config" \
  DEMO_WEB_PORT="$web_port" \
  node demo/replication_harness/server.mjs
) >"$web_log" 2>&1 &
web_pid=$!

echo
echo "Harness is ready: http://127.0.0.1:$web_port"
echo "Write in Client A or Client B, then use Burst writes to stress the feed."
echo "Native CLI output: $cli_log"
echo "Press Ctrl-C to stop all three processes."
echo

while :; do
  for pid in "$main_pid" "$cli_pid" "$web_pid"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "A harness process stopped unexpectedly; inspect $run_root" >&2
      exit 1
    fi
  done
  sleep 1
done
