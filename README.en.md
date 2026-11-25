# shm_pubsub

A two-header **C++11 Linux IPC library** for fixed-size shared-memory messages. Supports zero-copy broadcast, competing consumers, and blocking receive, with no daemon or library worker threads.

## Quick start

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/subscriber /demo &
./build/publisher /demo
```

The header-only target is `shm_pubsub::shm_pubsub`. Consumers can use `add_subdirectory`, then `target_link_libraries(app PRIVATE shm_pubsub::shm_pubsub)`. The optional constructor `Options` configures the POSIX shared-memory name. Use `stop()` to wake blocked receivers during shutdown. Call `ShmPubSub::destroy(name)` after every participant has detached.

## Zero copy and waiting

A publisher calls `loan()`, fills up to 4072 bytes in shared memory, then commits with `publish_loan()`. A subscriber reads in place with `take()` or waits with `wait_take()`. Loan handles release their references on destruction. The copy APIs remain available, including `receive_wait()` for a blocking copy.

```cpp
ShmPubSub::WriteLoan write;
if (pub.loan(1, write) == ShmPubSub::Status::OK) {
  write.data()[0] = 'x';
  pub.publish_loan(std::move(write));
}
ShmPubSub::ReadLoan read;
if (sub.wait_take(read, std::chrono::milliseconds(250)) == ShmPubSub::Status::OK) {
  std::cout << read.data()[0] << '\n';
}
```

## Architecture

```text
publisher(s) -> fixed block pool -> per-reader MPMC queue -> every broadcast reader
                             `----> shared MPMC queue -----> one competing reader
```

`publish_result` reports no subscribers, full queues, partial broadcast and oversize input. `receive_result` reports empty queues and insufficient buffers. See [exact API semantics](docs/api-semantics.md), [lifecycle/recovery](docs/lifecycle.md), and [lock-free platform contract](docs/lock-free-design.md).

## Examples and performance

`broadcast_example` shows structured publish results; `competing_example` handles real payload bytes. Run `./scripts/run-benchmarks.sh` for shared-memory and Unix-domain-datagram raw CSV baselines; methodology and semantic differences are in [the benchmark guide](docs/benchmark.md).

## Support and limitations

Requires Linux 3.15+, a common ABI, CMake 3.10+, and runtime lock-free atomics. Delivery is at most once. Subscriber queues are reclaimed on demand after exit. Death while holding a loan or updating a queue can still strand resources. Upgrading to shared-memory layout 4 requires a fresh name or recreation after all old endpoints have detached. Review [the roadmap](ROADMAP.md), [changelog](CHANGELOG.md), and [contribution guide](CONTRIBUTING.md). Licensed under MIT, with the separately marked free-list under Apache-2.0 OR MIT.

[中文说明](README.md)
