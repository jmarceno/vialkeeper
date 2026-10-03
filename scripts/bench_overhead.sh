#!/usr/bin/env bash
# Runs the SQLite overhead benchmark in the production Mix environment.
#
#   scripts/bench_overhead.sh --mode memory --scenario all
#
# Environment:
#   BENCH_CPUS          Optional CPU list for taskset (for example "2-5"). Pins
#                       the whole VM, including dirty schedulers, to those CPUs.
#   BENCH_ERL_OPTIONS   Optional extra VM flags, appended to ELIXIR_ERL_OPTIONS.
#
# The VM keeps production scheduler defaults on purpose. ExQLite runs every
# SQLite call on a dirty scheduler; disabling scheduler busy-waiting
# (+sbwt/+sbwtdcpu/+sbwtdio none) makes each of those hops several times more
# expensive and would charge the benchmark a cost production does not pay.
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

if [[ -n "${BENCH_ERL_OPTIONS:-}" ]]; then
  export ELIXIR_ERL_OPTIONS="${ELIXIR_ERL_OPTIONS:+$ELIXIR_ERL_OPTIONS }$BENCH_ERL_OPTIONS"
fi

export MIX_ENV=prod

command=(mix run --no-start bench/sqlite_exqlite_overhead_benchmark.exs -- "$@")

if [[ -n "${BENCH_CPUS:-}" ]]; then
  exec taskset --cpu-list "$BENCH_CPUS" "${command[@]}"
else
  exec "${command[@]}"
fi
