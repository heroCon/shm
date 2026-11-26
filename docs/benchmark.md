# Benchmarks

`./scripts/run-benchmarks.sh` builds the benchmarks in Release and writes
`benchmark-results/{metadata.txt,latency.csv,throughput.csv,uds.csv}`. The metadata
records the date, commit, kernel, CPU model and count, compiler and pinning. Override
`COUNT`, `LATENCY_COUNT`, `BUILD_DIR`, `OUTPUT_DIR`, and `PUB_CPU`/`SUB_CPU` to pin the
latency benchmark's processes.

| Binary | What it measures |
|---|---|
| `latency_benchmark [count] [bytes] [futex\|spin] [pub_cpu] [sub_cpu] [interval_us]` | Two processes. The publisher stamps `CLOCK_MONOTONIC` into each loan every `interval_us` (default 100); the subscriber records receive minus send, either blocking in `wait_take()` (`futex`) or busy-polling `take()` (`spin`). Prints p50, p99, p99.9 and max in microseconds. |
| `throughput_benchmark [subscribers] [messages] [bytes]` | One process, one publisher thread and N busy-polling subscriber threads on private queues. Loans are written only in their first byte, so the result is independent of `bytes` by design: it measures the zero-copy hand-off, not `memcpy`. A publish counts once at least one subscriber queued it; `partial`, `delivered` and `received` are reported, and `delivered == received` checks that nothing was lost or duplicated. |
| `uds_benchmark [messages] [bytes]` | `AF_UNIX`/`SOCK_DGRAM` between two threads, as a kernel-copy baseline. |

## Reference results

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
