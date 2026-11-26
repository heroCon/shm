# shm_pubsub

A header-only **C++11** shared-memory publish/subscribe library for Linux that is
**crash-verified**: a process can be `SIGKILL`ed at any of 13 named points inside
the data path and the surviving processes get every block back and keep
exchanging messages. There is no daemon, no background thread and no
dependency beyond the C++ standard library and Linux.

- **Zero copy.** Publishers write into a loaned shared-memory block; subscribers
  read it in place.
- **Broadcast and consumer groups in one mechanism.** Every publish reaches each
  private subscriber and exactly one member of each consumer group.
- **Bounded by construction.** Capacities are compile-time parameters, and a
  `static_assert` guarantees that slow or stalled subscribers can never exhaust the
  block pool. Each queue picks `kDropNewest` or `kDropOldest`.
- **Targeted wake-ups.** Each queue has its own futex word, so a group publish wakes
  one member and a private queue wakes only its owner. `WaitSet` waits on several
  subscribers, even across topics, with `futex_waitv`.
- **Observable.** `shmtop` shows liveness, queue depths, loans and drop counters of a
  running region without attaching to it.

## Quick start

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/subscriber_example /demo &
./build/publisher_example /demo
./build/shmtop /demo --once
```

```cpp
#include "shm_pubsub/shm_pubsub.hpp"

shm_pubsub::Options options;
options.name = "/demo";
shm_pubsub::Topic<> topic(options);             // creates or attaches
shm_pubsub::Topic<>::Publisher pub(topic);
shm_pubsub::Topic<>::Subscriber sub(topic);     // private queue, subscribed

shm_pubsub::Topic<>::WriteLoan write;
if (pub.loan(5, write) == shm_pubsub::Status::kOk) {
  std::memcpy(write.data(), "hello", 5);
  pub.publish(std::move(write));                // PublishResult: targets, delivered, drops
}

shm_pubsub::Topic<>::ReadLoan read;
if (sub.wait_take(read, std::chrono::milliseconds(100)) == shm_pubsub::Status::kOk) {
  // read.data(), read.size(), read.sequence(), read.timestamp_ns(), read.publisher_id()
}
```

Competing consumers join a group: `SubscriberOptions o; o.group = 1;`. Capacities
come from a `Config` type (`Topic<MyConfig>`). Every process attached to a region
must use the same `Config`; the region header stores a hash and rejects mismatches.

## Using it from CMake

```cmake
find_package(shm_pubsub 0.3 REQUIRED)     # after cmake --install
# or: add_subdirectory(shm_pubsub)
target_link_libraries(app PRIVATE shm_pubsub::shm_pubsub)
```

When added as a subproject, only the library target is defined; examples, tests and
tools build only at top level (`SHM_PUBSUB_BUILD_*` options).

## How it differs

| | shm_pubsub | iceoryx2 | eCAL | cpp-ipc |
|---|---|---|---|---|
| Integration | 1 header directory, C++11 | Rust core with C/C++ bindings | Runtime, services, monitoring | Library to build |
| Central daemon | none | none | none (per-process registration threads) | none |
| Consumer groups (work queue) next to broadcast | yes, one mechanism | no | no | route vs channel types |
| Crash recovery verified per fault point | yes, in CI | dead-node cleanup | n/a | not documented |

See [docs/comparison.md](docs/comparison.md) for the details and where each
alternative is the better choice.

## Documents

- [API semantics](docs/api-semantics.md): statuses, ownership, ordering, overflow.
- [Lifecycle](docs/lifecycle.md): creation, endpoint liveness, reclamation, `fork()`.
- [Crash consistency](docs/crash-consistency.md): what is recovered, the fault
  matrix and the remaining windows.
- [Lock-free design](docs/lock-free-design.md) and platform requirements.
- [Benchmarks](docs/benchmark.md), [comparison](docs/comparison.md),
  [roadmap](ROADMAP.md), [changelog](CHANGELOG.md) including 0.2 → 0.3 migration,
  and [contributing](CONTRIBUTING.md).

## Requirements and limits

Linux 3.15+ (OFD locks, process-shared futexes); `WaitSet` needs 5.16+ for
`futex_waitv` and reports `kNotSupported` otherwise. All processes must share the
architecture, ABI and `Config`. Delivery is in memory and at most once; there is no
persistence or history replay. Endpoints are not thread-safe, so use one per thread.
At most 48 endpoints per topic.

MIT licensed. `detail/lock_free_list.hpp` is derived from Eclipse iceoryx
(Apache-2.0 OR MIT); see [NOTICE](NOTICE).

[中文说明](README.md)
