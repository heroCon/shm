#!/usr/bin/env bash
# Builds the benchmarks in Release and writes one CSV per benchmark plus the
# machine metadata needed to compare runs.
set -euo pipefail
build=${BUILD_DIR:-build-bench}
out=${OUTPUT_DIR:-benchmark-results}
count=${COUNT:-1000000}
latency_count=${LATENCY_COUNT:-50000}
pub_cpu=${PUB_CPU:--1}
sub_cpu=${SUB_CPU:--1}

cmake -S . -B "$build" -DSHM_PUBSUB_BUILD_BENCHMARKS=ON -DSHM_PUBSUB_BUILD_TESTS=OFF \
  -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$build" -j >/dev/null
mkdir -p "$out"

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "kernel: $(uname -srm)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //' || uname -p)"
  echo "cpus: $(nproc)"
  echo "compiler: $(grep -m1 CMAKE_CXX_COMPILER: "$build/CMakeCache.txt" | cut -d= -f2) $(
    "$(grep -m1 CMAKE_CXX_COMPILER: "$build/CMakeCache.txt" | cut -d= -f2)" --version | head -n1)"
  echo "pinning: pub_cpu=$pub_cpu sub_cpu=$sub_cpu"
} > "$out/metadata.txt"

latency="$out/latency.csv"
"$build/latency_benchmark" 1 64 futex | head -n1 > "$latency"
for mode in futex spin; do
  for size in 64 1024 4000; do
    "$build/latency_benchmark" "$latency_count" "$size" "$mode" "$pub_cpu" "$sub_cpu" | tail -n1 \
      >> "$latency"
  done
done

throughput="$out/throughput.csv"
"$build/throughput_benchmark" 1 1 64 | head -n1 > "$throughput"
for subs in 1 2 4; do
  for size in 64 1024 4000; do
    "$build/throughput_benchmark" "$subs" "$count" "$size" | tail -n1 >> "$throughput"
  done
done

uds="$out/uds.csv"
"$build/uds_benchmark" 1 64 | head -n1 > "$uds"
for size in 64 1024 4000; do "$build/uds_benchmark" "$count" "$size" | tail -n1 >> "$uds"; done

cat "$out/metadata.txt" "$latency" "$throughput" "$uds"
