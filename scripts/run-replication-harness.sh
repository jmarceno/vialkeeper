#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date +%Y%m%d-%H%M%S)-$$"
if [[ -n "${VIALKEEPER_DEMO_ROOT:-}" ]]; then
  run_root="$VIALKEEPER_DEMO_ROOT/replication-harness-$run_id"
  keep_root=1
else
  run_root="$project_root/tmp/replication-harness-$run_id"
  keep_root=""
fi

main_root="$run_root/web-data"
cli_root="$run_root/native-data"
worker_a_root="$run_root/shadow-worker-a-data"
worker_b_root="$run_root/shadow-worker-b-data"
state_root="$run_root/state"
main_config="$state_root/main.json"
cli_config="$state_root/cli.json"
worker_a_config="$state_root/worker-a.json"
worker_b_config="$state_root/worker-b.json"
ready_config="$state_root/ready.json"
private_config="$state_root/private.json"
results_root="$state_root/results"

server_port="${VIALKEEPER_DEMO_DB_PORT:-4100}"
cli_port="${VIALKEEPER_DEMO_CLI_PORT:-4101}"
worker_a_port="${VIALKEEPER_DEMO_WORKER_A_PORT:-4102}"
worker_b_port="${VIALKEEPER_DEMO_WORKER_B_PORT:-4103}"
web_port="${VIALKEEPER_DEMO_WEB_PORT:-4180}"

main_log="$run_root/web-node.log"
cli_log="$run_root/native-cli.log"
worker_a_log="$run_root/shadow-worker-a.log"
worker_b_log="$run_root/shadow-worker-b.log"
web_log="$run_root/ui-server.log"

main_pid=""
cli_pid=""
worker_a_pid=""
worker_b_pid=""
web_pid=""

source_token="${VIALKEEPER_DEMO_SOURCE_TOKEN:-vialkeeper-demo-source-$run_id}"
worker_a_token="${VIALKEEPER_DEMO_WORKER_A_TOKEN:-vialkeeper-demo-worker-a-$run_id}"
worker_b_token="${VIALKEEPER_DEMO_WORKER_B_TOKEN:-vialkeeper-demo-worker-b-$run_id}"

mkdir -p "$main_root" "$cli_root" "$worker_a_root" "$worker_b_root" "$state_root" "$results_root"
rm -f -- "$main_config" "$cli_config" "$worker_a_config" "$worker_b_config" "$ready_config" "$private_config"

cleanup() {
  trap - EXIT INT TERM
  for pid in "$web_pid" "$cli_pid" "$main_pid" "$worker_b_pid" "$worker_a_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done

  deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    alive=0
    for pid in "$web_pid" "$cli_pid" "$main_pid" "$worker_b_pid" "$worker_a_pid"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        alive=1
      fi
    done

    if (( alive == 0 )); then
      break
    fi

    sleep 0.1
  done

  for pid in "$web_pid" "$cli_pid" "$main_pid" "$worker_b_pid" "$worker_a_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done

  for pid in "$web_pid" "$cli_pid" "$main_pid" "$worker_b_pid" "$worker_a_pid"; do
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
  shift
  local watched_pids=("$@")
  local deadline=$((SECONDS + 120))

  while [[ ! -s "$path" ]]; do
    for pid in "${watched_pids[@]}"; do
      if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
        echo "A harness process stopped before writing $path" >&2
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

echo "Starting VialKeeper replication harness"
echo "  Database HTTP: http://127.0.0.1:$server_port"
echo "  Native CLI HTTP wire: http://127.0.0.1:$cli_port"
echo "  Shadow workers: http://127.0.0.1:$worker_a_port and http://127.0.0.1:$worker_b_port"
echo "  Web UI: http://127.0.0.1:$web_port"

(
  cd "$project_root"
  MIX_ENV=dev \
  VIAL_KEEPER_ROOT="$worker_a_root" \
  VIAL_KEEPER_PORT="$worker_a_port" \
  DEMO_PROJECT_ROOT="$project_root" \
  DEMO_WORKER_CONFIG="$worker_a_config" \
  DEMO_WORKER_KEY="a" \
  DEMO_WORKER_CONTROL_TOKEN="$worker_a_token" \
  DEMO_SOURCE_ENDPOINT="http://127.0.0.1:$server_port" \
  DEMO_SOURCE_TOKEN="$source_token" \
  DEMO_ALLOWED_ATTACHMENT_ROOTS="$main_root" \
  mix run --no-start demo/replication_harness/node.exs worker
) >"$worker_a_log" 2>&1 &
worker_a_pid=$!

(
  cd "$project_root"
  MIX_ENV=dev \
  VIAL_KEEPER_ROOT="$worker_b_root" \
  VIAL_KEEPER_PORT="$worker_b_port" \
  DEMO_PROJECT_ROOT="$project_root" \
  DEMO_WORKER_CONFIG="$worker_b_config" \
  DEMO_WORKER_KEY="b" \
  DEMO_WORKER_CONTROL_TOKEN="$worker_b_token" \
  DEMO_SOURCE_ENDPOINT="http://127.0.0.1:$server_port" \
  DEMO_SOURCE_TOKEN="$source_token" \
  DEMO_ALLOWED_ATTACHMENT_ROOTS="$main_root" \
  mix run --no-start demo/replication_harness/node.exs worker
) >"$worker_b_log" 2>&1 &
worker_b_pid=$!

wait_for_file "$worker_a_config" "$worker_a_pid"
wait_for_file "$worker_b_config" "$worker_b_pid"

(
  cd "$project_root"
  MIX_ENV=dev \
  VIAL_KEEPER_ROOT="$main_root" \
  VIAL_KEEPER_PORT="$server_port" \
  DEMO_PROJECT_ROOT="$project_root" \
  DEMO_SOURCE_TOKEN="$source_token" \
  DEMO_ALLOWED_ATTACHMENT_ROOTS="$main_root" \
  DEMO_WORKER_A_ENDPOINT="http://127.0.0.1:$worker_a_port" \
  DEMO_WORKER_A_CONTROL_TOKEN="$worker_a_token" \
  DEMO_WORKER_B_ENDPOINT="http://127.0.0.1:$worker_b_port" \
  DEMO_WORKER_B_CONTROL_TOKEN="$worker_b_token" \
  DEMO_MAIN_CONFIG="$main_config" \
  DEMO_C_CONFIG="$cli_config" \
  DEMO_WORKER_A_CONFIG="$worker_a_config" \
  DEMO_WORKER_B_CONFIG="$worker_b_config" \
  DEMO_READY_CONFIG="$ready_config" \
  DEMO_PRIVATE_CONFIG="$private_config" \
  mix run --no-start demo/replication_harness/node.exs web
) >"$main_log" 2>&1 &
main_pid=$!

wait_for_file "$main_config" "$main_pid"

(
  cd "$project_root"
  MIX_ENV=dev \
  VIAL_KEEPER_ROOT="$cli_root" \
  VIAL_KEEPER_PORT="$cli_port" \
  DEMO_SOURCE_TOKEN="$source_token" \
  DEMO_C_CONFIG="$cli_config" \
  DEMO_READY_CONFIG="$ready_config" \
  mix run --no-start demo/replication_harness/node.exs cli
) >"$cli_log" 2>&1 &
cli_pid=$!

wait_for_file "$ready_config" "$main_pid" "$cli_pid" "$worker_a_pid" "$worker_b_pid"

(
  cd "$project_root"
  DEMO_READY_CONFIG="$ready_config" \
  DEMO_PRIVATE_CONFIG="$private_config" \
  DEMO_RESULTS_ROOT="$results_root" \
  DEMO_PROJECT_ROOT="$project_root" \
  DEMO_WEB_PORT="$web_port" \
  node demo/replication_harness/server.mjs
) >"$web_log" 2>&1 &
web_pid=$!

echo
echo "Harness is ready: http://127.0.0.1:$web_port"
echo "Write in Client A or Client B, then use Burst writes to stress the feed."
echo "Native CLI output: $cli_log"
echo "Worker logs: $worker_a_log and $worker_b_log"
echo "Scenario artifacts: $results_root"
echo "Press Ctrl-C to stop all harness processes."
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
