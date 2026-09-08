#!/usr/bin/env bash
set -euo pipefail
build=${BUILD_DIR:-build-bench}; out=${OUTPUT:-benchmark-results.csv}; count=${COUNT:-100000}
cmake -S . -B "$build" -DSHM_PUBSUB_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build "$build" -j
: > "$out"
for size in 64 1024 4072; do for bin in shm_benchmark uds_benchmark; do "$build/$bin" "$count" "$size" | tail -n 1 >> "$out"; done; done
printf 'results: %s\n' "$out"
