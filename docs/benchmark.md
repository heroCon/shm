# Benchmarks

`./scripts/run-benchmarks.sh` builds the benchmarks in Release and writes
`benchmark-results/{metadata.txt,latency.csv,throughput.csv,uds.csv}`. The metadata
records the date, commit, kernel, CPU model and count, compiler and pinning. Override
`COUNT`, `LATENCY_COUNT`, `BUILD_DIR`, `OUTPUT_DIR`, and `PUB_CPU`/`SUB_CPU` to pin the
latency benchmark's processes.

| Binary | What it measures |
|---|---|
| `latency_benchmark [count] [bytes] [futex\|spin] [pub_cpu] [sub_cpu] [interval_us]` | Two processes. The publisher stamps `CLOCK_MONOTONIC` into each loan every `interval_us` (default 100); the subscriber records receive minus send, either blocking in `wait_take()` (`futex`) or busy-polling `take()` (`spin`). Prints p50, p99, p99.9 and max in microseconds. |
| `throughput_benchmark [subscribers] [messages] [bytes] [--publishers N] [--batch N] [--no-timestamps] [--idle]` | One process, P publisher threads (default 1) and N busy-polling subscriber threads on private queues. `--batch` uses `publish(writes, n)` and `drain()`; `--no-timestamps` uses a `Config` with `kTimestamps = false`; `--idle` makes subscribers never read and use `kDropOldest`, which isolates publisher-side cost. Loans are written only in their first byte, so the result is independent of `bytes` by design: it measures the zero-copy hand-off, not `memcpy`. A publish counts once at least one subscriber queued it; `partial`, `delivered` and `received` are reported, and `delivered == received` checks that nothing was lost or duplicated. |
| `control_path_benchmark` | Region creation, endpoint construction plus destruction, `recover()`, and `collect_garbage()` after one killed subscriber, for 256, 4096, 16384 and 65536 blocks. |
| `uds_benchmark [messages] [bytes]` | `AF_UNIX`/`SOCK_DGRAM` between two threads, as a kernel-copy baseline. |

## 0.4 against 0.3

Measured on 2026-11-27 on the same kind of 4-vCPU cloud VM (Linux 6.12, g++ 13.3,
Release), without pinning, interleaving runs of `main` (0.3) and 0.4, 12 repetitions of
1 M messages each for throughput. Medians, with the range in parentheses.

| Measurement | 0.3 | 0.4 |
|---|---|---|
| Throughput, 1 subscriber, 64 B | 2.32 M msg/s (2.03–2.50) | 4.44 M msg/s (4.01–4.73) |
| Throughput, 2 subscribers, 64 B | 1.38 M msg/s (1.17–1.58) | 3.36 M msg/s (2.02–3.57) |
| Latency, futex, p50 / p99 | 3.9–4.0 / 10.5–11.9 µs | 3.9–4.0 / 11.3–12.9 µs |
| Endpoint construction + destruction, 256 / 4096 / 16384 / 65536 blocks | 1.6–2.5 / 22–25 / 85–98 / 304–336 µs | 2.6–3.0 µs at every size |
| `recover()`, 256 / 65536 blocks | 11.5–14.3 / 518–527 µs | 12.2–12.9 / 568–600 µs |

The throughput gain comes from three changes: the owner-written activity word moved
off the cache line that publishers read on every publish, publishers no longer CAS a
pin bit into each subscriber's lifecycle word, and an empty `take()` writes nothing.
Endpoint destruction reclaims through the slot's loan table instead of scanning every
block. `collect_garbage()` still ends with a global `recover()` and therefore still
grows with the pool.

Measured and **not** improved:

- `--batch 2` (4.20 / 3.14 M msg/s) and `--no-timestamps` (4.12 / 3.17 M msg/s) are
  within noise of the defaults with one publisher thread and busy-polling readers: the
  cost is in cross-core cache-line transfers, not in the clock read (about 24 ns of a
  103 ns single-thread round trip) or the per-message pin pass.
- Two publishers into one idle `kDropOldest` subscriber: 1 publisher 7.0–10.1 M
  publishes/s, 2 publishers 2.2–2.7 M in total, the same as 0.3. Every publish both
  enqueues into and evicts from the same 8-cell queue, so the queue positions are the
  contended words. A per-publisher block cache (keep blocks whose only reference is the
  publisher, adopt evicted blocks) was tried: about +10% with one publisher, noisy with
  two (1.4–3.5 M), and slower with more retries when subscribers read, so it was not
  kept.

## 0.3 against 0.2

Measured on 2026-11-26 on a 4-vCPU cloud VM (Intel Xeon, Linux 6.12, g++ 13.3,
Release), without pinning, interleaving 0.2 and 0.3 runs, 3 repetitions each. These
numbers come from a shared VM: compare runs on one machine, not across machines.

| Measurement | 0.2 (main) | 0.3 |
|---|---|---|
| Throughput, 1 subscriber, 64 B | 1.29–1.57 M msg/s | 2.26–2.31 M msg/s |
| Throughput, 2 subscribers, 64 B | 0.71–0.78 M msg/s | 1.26–1.48 M msg/s |
| Latency, futex, p50 / p99 | 4.0–4.2 / 10.3–10.8 µs | 3.9–4.1 / 10.7–11.0 µs |
| Latency, busy-poll, p50 / p99 | 0.40–0.49 / 0.66–0.82 µs | 0.28–0.31 / 0.49–1.12 µs |
| UDS datagram, 64 B (baseline) | – | 1.06–1.15 M msg/s |

0.2 counted a publish only when every subscriber queued it and retried otherwise, so
with 2 subscribers its rate is a lower bound; in 0.3, under 0.25% of those publishes
were partial. With 4 busy-polling subscribers on 4 vCPUs, 0.2 did not finish within
5 minutes. The p99.9 and max columns are dominated by VM scheduling noise on this host
(hundreds of microseconds for both versions) and are not compared.

Behavioural changes measured with the same experiments as the 0.2 analysis:

| Experiment | 0.2 | 0.3 |
|---|---|---|
| One stalled reader, 1000 publishes: fast reader receives | 32 (968 `QUEUE_FULL`) | all 1000 (`fairness_test`) |
| Writer killed holding loans, 40 times: free blocks after | 0 of 32 | all (`crash_leak_test`) |
| Wake-ups per message, competing readers | 8.0 with 8 readers | 1.0 with 6 group members (`wakeup_test`) |
| Unsubscribed endpoint receives broadcast | yes | no, `kNotSubscribed` (`semantics_test`) |
