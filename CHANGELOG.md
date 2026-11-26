# Changelog

## 0.3.0 - Unreleased

This release breaks source and shared-memory compatibility with 0.2. See the migration
notes below. It also includes the changes merged after 0.2.0 and never released on
their own: zero-copy loans, futex-backed blocking receives, and OFD slot ownership
replacing the heartbeat and recycle threads (layout 4).

### Added

- `shm_pubsub::Topic<Config>` with compile-time capacities; the region header stores a
  hash of the `Config` and rejects mismatched attachments.
- Consumer groups (`SubscriberOptions::group`): each publish reaches every private
  subscriber and exactly one member of each group.
- Per-queue overflow policy, `Overflow::kDropNewest` or `kDropOldest`, and a
  compile-time pool budget that stalled subscribers cannot exhaust.
- Per-queue futex words: group publishes wake one member, private publishes wake only
  the owner. `WaitSet` waits on several subscribers or topics with `futex_waitv`.
- Block headers carry the publisher slot, a per-publisher sequence number and a
  `CLOCK_MONOTONIC` timestamp, exposed on `ReadLoan`.
- Crash consistency: holder bitmaps, per-slot activity words, `Topic::recover()` with a
  `RecoveryReport`, and a fault-injection crash matrix in CI
  ([docs](docs/crash-consistency.md)).
- `Topic::snapshot()`, `Topic::inspect()` and the `shmtop` tool.
- `Options::mode`; per-slot statistics (published, taken, drops, held loans).
- Installable CMake package (`find_package(shm_pubsub 0.3)`), CI on gcc/clang,
  sanitizers, arm64, clang-format, and two-process latency benchmarks.

### Changed

- Headers moved to `include/shm_pubsub/`; everything is in namespace `shm_pubsub`.
- Shared-memory layout 5.
- `take()` on an unsubscribed endpoint returns `kNotSubscribed` instead of delivering.
- The free-list header is attributed in `NOTICE`; `LICENSE` is plain MIT.

### Removed

- `ShmPubSub`, its `Role` enum, `DeliveryMode`, `WRONG_ROLE` and the Boolean wrappers.
- `delay_time.h`, and the runtime lock-free checks, which are now compile-time.

### Migrating from 0.2

| 0.2 | 0.3 |
|---|---|
| `#include "shm_pubsub.h"` | `#include "shm_pubsub/shm_pubsub.hpp"` |
| `ShmPubSub pub(ShmPubSub::PUBLISHER, o)` | `Topic<> topic(o); Topic<>::Publisher pub(topic);` |
| `ShmPubSub sub(ShmPubSub::SUBSCRIBER, o); sub.subscribe();` | `Topic<>::Subscriber sub(topic);` (subscribed on construction) |
| `publish_loan(std::move(w), BROADCAST)` | `publish(std::move(w))` |
| `publish_loan(std::move(w), COMPETING)` | create the readers with `SubscriberOptions{group = 1}`, then `publish(std::move(w))` |
| `publish_result(data, len)` | `publish_copy(data, len)` returning `PublishResult` |
| `receive_result(...)` / `receive_wait(...)` | `receive(buf, size)` / `wait_receive(buf, size, timeout)` returning `ReceiveResult` |
| `Status::OK`, `QUEUE_FULL`, ... | `Status::kOk`, `kQueueFull`, ...; `to_string(status)` |
| `ShmPubSub::destroy(name)` | `Topic<>::destroy(name)` |
| `get_valid_subscriber_count()` | `topic.subscriber_count()` |
| Fixed capacities, 4072-byte payload | `Config`; default payload `Topic<>::max_payload_size()` = 4032 bytes |
| `add_subdirectory` + `shm_pubsub::shm_pubsub` | unchanged, or `find_package(shm_pubsub 0.3)` |

Stop every 0.2 process before starting 0.3 on the same name, then call
`Topic<>::destroy(name)` or use a new name; a 0.3 process refuses to attach to a region created by
0.2. Readers that relied on receiving without `subscribe()` must stay subscribed. A
reader that read both its broadcast queue and the competing queue now needs two
subscribers: one private, one in a group.

## 0.2.0 - 2026-09-08

- Defined structured delivery and receive semantics.
- Added creator/attacher initialization, layout validation, explicit destruction, safe subscriber slot reclamation, and lock-free platform checks.
- Added multi-process tests, CI, examples, an exported CMake target, and SHM/UDS benchmarks.
- Added lifecycle, platform, benchmark, contribution, roadmap, license, and release documentation.
