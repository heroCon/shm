# Reproducible benchmark

Run `./scripts/run-benchmarks.sh`. Override `COUNT`, `BUILD_DIR`, or `OUTPUT`; the default produces CSV rows for 64, 1024, and 4072-byte messages. Record alongside the CSV: CPU model, core affinity, kernel, compiler/version, build type, and whether the host was otherwise idle. Use `taskset` and `/usr/bin/time -v` externally when repeatable CPU accounting is required.

The shared-memory benchmark uses one process with two endpoints and includes allocation, queueing, receive, and release time. The Unix-domain-socket baseline uses `AF_UNIX/SOCK_DGRAM` and a receiver thread. Both report wall-clock throughput and drops; they do **not** establish equivalent delivery durability, scheduling, cross-process latency, or CPU cost. Run at least five repetitions and report raw rows rather than only the best value. This harness is a reproducible smoke benchmark, not a published performance claim.
