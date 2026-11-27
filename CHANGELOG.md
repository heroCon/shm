# Changelog

## 0.4.0 - Unreleased

This release changes the shared-memory layout (layout 6); 0.3 and 0.4 processes cannot
share a region. Source changes are small: see the migration notes below.

### Fixed

- Group membership could drift permanently when a member was killed between the two
  steps of joining or leaving (measured: in 1500 kill rounds the count ended wrong in
  730), silently misrouting publishes to a memberless group or skipping a group with a
  live member. Membership is now a per-group bitmap updated in one atomic step, and
  `recover()` clears bits of dead slots.
- Endpoint constructors threw "endpoint slot limit reached" while another process ran
  `recover()` (13.8% of constructions with `recover()` in a tight loop). They now wait
  for the maintenance lock and rescan.
- An idle reader busy-polling `take()` kept `recover()` from ever succeeding, because
  each empty poll wrote the activity journal. Empty polls now write nothing.
- A group's overflow policy was whatever the most recently created member asked for.

### Changed

- A group's policy is fixed by its first member; a member with different options is
  rejected with `std::invalid_argument`.
- When the last member leaves a group (or is reclaimed), the group's backlog is
  discarded unless the group is durable.
- Up to 56 endpoints per topic (was 48) and no separate limit on publishers (was 24).
- Publishers announce their targets in a per-publisher pin mask instead of CASing a
  pin into each subscriber's lifecycle word; owner-written slot words moved to their
  own cache line. Throughput with busy-polling readers roughly doubled (see
  [benchmarks](docs/benchmark.md)).
- Reclaiming a slot clears holder bits through a per-slot loan table, so endpoint
  destruction no longer scans the whole pool.
- `GroupInfo` gains `member_mask`, `policy_set` and `overflow`; `RecoveryReport` gains
  `members_cleared`; `SlotInfo::pins` now reports a publisher's pin mask.

### Added

- `Config::kTimestamps` (default `true`) to skip the per-publish clock read.
- `collect_garbage()` repairs holes, claimed cells and in-flight blocks of dead
  endpoints without a quiescent topic, and falls back to `recover()` only when a leak
  cannot be attributed. `Options::self_heal` (default on) runs that collection from the
  data path, at most once per `self_heal_interval`.
- Work-queue groups: `durable` backlogs, `Delivery::kAtLeastOnce` with
  `ReadLoan::delivery_count()`, `requeue()` and `max_deliveries` / `dead_lettered`, and
  `Overflow::kBlock` with `publish(loan, timeout)`.
- Optional `Config::kLargeBlockCount` / `kLargeBlockSize`, a second free list.
  `loan()` picks the smallest class that fits.
- `Publisher::publish(WriteLoan*, n)`, `Subscriber::take(ReadLoan*, n)` and
  `Subscriber::drain(fn, max)`.
- `membership_test`, `slot_contention_test`, `control_path_benchmark`, and
  `--publishers`, `--batch`, `--no-timestamps`, `--idle` for `throughput_benchmark`.

### Migrating from 0.3

| 0.3 | 0.4 |
|---|---|
| Group members with different `overflow` | Use one policy per group; a conflicting member throws `std::invalid_argument` |
| Group backlog kept after the last member left | Discarded |
| `find_package(shm_pubsub 0.3)` | `find_package(shm_pubsub 0.4)` |
| Custom `Config` deriving from `DefaultConfig` | Unchanged; it inherits `kTimestamps = true` |

Stop every 0.3 process before starting 0.4 on the same name, then call
`Topic<>::destroy(name)` or use a new name; a 0.4 process refuses to attach to a layout-5
region.

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
