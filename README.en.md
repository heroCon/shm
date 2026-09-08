# shm_pubsub

A small **experimental Linux IPC library** providing fixed-size shared-memory messages in broadcast and competing-consumer modes. Version 0.2 makes lifecycle and failure boundaries explicit; it is not durable middleware.

## Quick start

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/subscriber /demo &
./build/publisher /demo
```

The header-only target is `shm_pubsub::shm_pubsub`. Consumers can use `add_subdirectory`, then `target_link_libraries(app PRIVATE shm_pubsub::shm_pubsub)`. The optional constructor `Options` configures the POSIX shared-memory name. Call `stop()` for normal shutdown and `ShmPubSub::destroy(name)` only when every participant has detached.

## Architecture

```text
publisher(s) -> fixed block pool -> per-reader MPMC queue -> every broadcast reader
                             `----> shared MPMC queue -----> one competing reader
```

`publish_result` reports no subscribers, full queues, partial broadcast and oversize input. `receive_result` reports empty queues and insufficient buffers. See [exact API semantics](docs/api-semantics.md), [lifecycle/recovery](docs/lifecycle.md), and [lock-free platform contract](docs/lock-free-design.md).

## Examples and performance

`broadcast_example` shows structured publish results; `competing_example` handles real payload bytes. Run `./scripts/run-benchmarks.sh` for shared-memory and Unix-domain-datagram raw CSV baselines; methodology and semantic differences are in [the benchmark guide](docs/benchmark.md).

## Support and limitations

Supported: Linux, a common ABI across participants, CMake 3.10+, and runtime lock-free required atomics. Messages are at-most-once and best-effort under queue pressure. No persistence, authentication, schema negotiation, publisher-crash allocation journal, or initializer takeover is provided. Heartbeat reclamation assumes processes are scheduled within five seconds. Review [the roadmap](ROADMAP.md), [changelog](CHANGELOG.md), and [contribution guide](CONTRIBUTING.md). Licensed under MIT, with the separately marked free-list under Apache-2.0 OR MIT.

[中文说明](README.md)
