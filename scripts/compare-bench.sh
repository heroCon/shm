#!/usr/bin/env bash
# Latency numbers for this tree, plus iceoryx2 and eCAL when they are installed.
# This machine's results are VM numbers; reproduce on bare metal before quoting them.
#   ICEORYX2_PUBSUB  path of iceoryx2's publish-subscribe benchmark (optional)
#   ECAL_LATENCY     path of eCAL's latency sample (optional)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
build=$root/build-rel
if [[ ! -x $build/latency_benchmark ]]; then
  cmake -S "$root" -B "$build" -DCMAKE_BUILD_TYPE=Release -DSHM_PUBSUB_BUILD_BENCHMARKS=ON
  cmake --build "$build" -j"$(nproc)" --target latency_benchmark
fi
out=${1:-$root/docs/comparison-vm.csv}
echo "transport,payload_bytes,samples,p50_ns,p99_ns,notes" > "$out"
# latency_benchmark prints its own table; keep a raw copy beside the CSV.
"$build/latency_benchmark" 2000 64 futex | tee "$out.raw"
awk -F, 'NR>1 && $1=="shm" {
  printf "shm_pubsub,%s,%s,%.0f,%.0f,futex; microseconds in the raw file, this VM\n", $3, $4, $5*1000, $6*1000
}' "$out.raw" >> "$out"
if [[ -n ${ICEORYX2_PUBSUB:-} && -x $ICEORYX2_PUBSUB ]]; then
  "$ICEORYX2_PUBSUB" | tee -a "$out.raw"
  echo "iceoryx2,64,,,,see raw (VM)" >> "$out"
else
  echo "iceoryx2 skipped: set ICEORYX2_PUBSUB to its publish-subscribe benchmark" | tee -a "$out.raw"
fi
if [[ -n ${ECAL_LATENCY:-} && -x $ECAL_LATENCY ]]; then
  "$ECAL_LATENCY" | tee -a "$out.raw"
  echo "ecal,64,,,,see raw (VM)" >> "$out"
else
  echo "eCAL skipped: set ECAL_LATENCY to its latency sample" | tee -a "$out.raw"
fi
echo "wrote $out"
