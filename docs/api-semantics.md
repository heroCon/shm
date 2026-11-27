# API semantics

Everything lives in namespace `shm_pubsub`. A `Topic<Config>` owns one mapping of a
named region; `Topic::Publisher` and `Topic::Subscriber` are endpoints created from it.
Each endpoint occupies one slot in the region for its lifetime.

## Configuration

`Config` supplies `kBlockCount`, `kBlockSize` (including a 64-byte block header),
`kQueueCapacity`, `kMaxPublishers`, `kMaxSubscribers`, `kMaxGroups`,
`kMaxLoansPerPublisher`, `kMaxLoansPerSubscriber`, `kTimestamps` (default `true`;
`false` skips the per-publish clock read and leaves `timestamp_ns()` at 0) and an
optional second pool `kLargeBlockCount` / `kLargeBlockSize` (default 0). `loan()` picks
the smallest class that fits. Ids at or above `kBlockCount` index the large class, and
`WriteLoan::capacity()` reports that block's payload. `max_payload_size()` is the
larger of the two classes. The pool budget below covers the small class only: the large
class returns `kPoolExhausted` when it runs out, unless `kLargeBlockCount` alone meets
the same budget. Derive from `DefaultConfig` and redeclare the members you change. Compile-time checks
enforce:

- `kQueueCapacity` is a power of two and smaller than `kBlockCount`;
- at most 56 endpoints (`kMaxPublishers + kMaxSubscribers`), and at most 255 queues
  (`kMaxSubscribers + kMaxGroups`);
- the **pool budget** `kBlockCount >= (kMaxSubscribers + kMaxGroups) * kQueueCapacity +
  kMaxSubscribers * kMaxLoansPerSubscriber + kMaxPublishers * kMaxLoansPerPublisher`.

Because every reference to a block is either a queue entry or a loan, and both are
bounded, the budget means the pool cannot run dry no matter how slowly subscribers read.
`kPoolExhausted` therefore indicates leaked blocks, which `loan()` tries to recover
before it reports the status.

The region header stores a hash of the configuration and the layout version (6). An
attach with a different `Config` or layout throws `std::runtime_error`.

## Publishing

| Call | Result |
|---|---|
| `loan(size, write)` | `kOk` and a block of `size` bytes (1..`max_payload_size()`); `kLoanLimit` when the endpoint already holds `kMaxLoansPerPublisher` loans; `kMessageTooLarge`; `kInvalidArgument` for size 0 or a non-empty `write`; `kPoolExhausted` only after leaks that `collect_garbage()` and `recover()` could not undo. |
| `write.resize(n)` | Shrinks or grows the payload within `capacity()`. |
| `publish(std::move(write))` | Consumes the loan whenever it belongs to this publisher, whatever the status. Returns a `PublishResult`. Waits up to `Options::block_timeout` (default 1 s) for room in a full `kBlock` group. |
| `publish(std::move(write), timeout)` | Same, with an explicit wait. `kTimedOut` if a `kBlock` group stayed full for the whole timeout. |
| `publish(writes, n)` | Publishes up to `kMaxLoansPerPublisher` loans in order with one pass over the subscriber table and at most one wake-up per queue. Counts in the result are summed over the messages. Returns `kInvalidArgument` and consumes nothing unless every loan belongs to this publisher. |
| `publish_copy(data, size)` | `loan` + `memcpy` + `publish`; returns `kNoSubscribers` without allocating when nobody is eligible. |

`PublishResult` has `targets` (subscribed private subscribers plus groups with at least
one member), `delivered`, `dropped_oldest` and `status`:

| Status | Meaning |
|---|---|
| `kOk` | Every target queued the message. |
| `kNoSubscribers` | No targets; the message was dropped on purpose. |
| `kPartial` | Some targets queued it; the others were full under `kDropNewest`. |
| `kQueueFull` | Every target was full under `kDropNewest`. |
| `kTimedOut` | A `kBlock` group stayed full until the timeout. The loan is consumed. |

`ok()` is true for `kOk`, `kNoSubscribers` and `kPartial`. Every block header records
the publisher slot, a per-publisher `sequence` that increments on every publish, and a
`CLOCK_MONOTONIC` timestamp. A reader that sees a gap in `sequence` for one publisher
missed messages, through a full queue, `kDropOldest`, or a group sibling taking them.

## Subscribing

`Subscriber(topic, SubscriberOptions{group, overflow, durable, delivery, max_deliveries})`:

- `group == 0` gives the subscriber a **private queue**; every publish is offered to it.
  `kBlock`, `durable`, `delivery` and `max_deliveries` are rejected on a private queue.
- `group` in `1..kMaxGroups` joins a **consumer group**. Each group has one queue; every
  publish is offered once per group, and exactly one member takes it.
- `overflow` selects `kDropNewest` (a full queue rejects the new message),
  `kDropOldest` (the publisher discards the oldest queued message to make room) or,
  for groups only, `kBlock` (the publisher waits for a free cell, woken through the
  group's `space` word).
- `durable` keeps the group's backlog when the last member leaves, so a later member
  receives it. The default discards that backlog.
- `delivery` is `kAtMostOnce` (the default; a message a member held when it died is
  dropped) or `kAtLeastOnce` (it goes back on the group queue).
- `max_deliveries` (0 means unlimited, at most 254) drops a message on `requeue()` or
  on redelivery and counts it in `GroupInfo::dead_lettered`. `ReadLoan::delivery_count()`
  is 1 the first time and higher after a redelivery. `ReadLoan::requeue()` hands a
  group message back; `kQueueFull` keeps the loan, `kDeadLettered` consumes it.

A group's policy is fixed by its first member for the lifetime of the region. A later
member whose options differ is rejected with `std::invalid_argument`, instead of
silently changing the policy for everyone.

A subscriber is subscribed on construction. `unsubscribe()` makes it ineligible for new
messages; its private queue keeps what it already holds, and `take()` returns
`kNotSubscribed` until `subscribe()`. A group member that unsubscribes leaves the group.
Messages queued for the group stay for the remaining members. When the **last** member
leaves (or dies and is reclaimed), a group that is not `durable` discards its backlog,
so a later member does not receive stale work. Membership is a bitmap in shared memory,
so `GroupInfo::members` is always the number of member slots. `GroupInfo` also reports
`durable`, `delivery`, `max_deliveries`, `redelivered` and `dead_lettered`.

| Call | Result |
|---|---|
| `take(read)` | `kOk` with a read-only view; `kWouldBlock` if the queue is empty; `kNotSubscribed`; `kLoanLimit` at `kMaxLoansPerSubscriber` held loans; `kInvalidArgument` for a non-empty `read`; `kCorrupted` if a queued block id or size fails validation (the entry is skipped). |
| `wait_take(read, timeout)` | Blocks on the queue's futex word. `kTimedOut` at the deadline, `kStopped` after `stop()`. A zero timeout makes one attempt; negative timeouts are invalid. |
| `receive(buf, size)` / `wait_receive(...)` | Copy variants returning `ReceiveResult{status, size}`. `kBufferTooSmall` reports the size; the message is consumed and not copied. |
| `take(reads, n)` | Takes up to `n` messages into empty loans (stops at the loan limit or an empty queue) and returns how many. |
| `drain(fn, max)` | Takes up to `max` messages one at a time, calls `fn(const ReadLoan&)` and releases each before the next; returns how many. |
| `stop()` | Any thread may call it. Current and future blocking calls return `kStopped`. A stopping group member passes a pending wake-up on to another member. |

`ReadLoan` exposes `data()`, `size()`, `sequence()`, `timestamp_ns()` and
`publisher_id()`. Both loan types are move-only and release their reference on
`reset()` or destruction; the endpoint must outlive its loans.

## Ordering and delivery

- Each queue is FIFO. Messages of one publisher reach one queue in publish order.
  Concurrent publishers have no global order.
- Delivery is at most once per private subscriber. A group is at most once unless its
  policy says `kAtLeastOnce`, in which case a message a member was holding or had
  claimed when it died is delivered again, and `requeue()` asks for that explicitly.
  Reconnecting creates a new slot generation and does not replay a non-durable group's
  history.
- `Options::self_heal` (default on) lets an endpoint that observes a stalled queue, a
  full queue or an empty pool run `collect_garbage()` itself. See
  [crash consistency](crash-consistency.md).

## WaitSet

`WaitSet` waits on up to 128 subscribers, which may belong to topics with different
`Config`s, with one `futex_waitv` call (Linux 5.16+). `WaitSet::supported()` probes the
kernel once; `wait()` returns `kNotSupported` without it. `wait(timeout, &index)`
returns `kOk` with the index (in insertion order) of a subscriber whose queue head is
ready, `kStopped` if one was stopped, or `kTimedOut`. Readiness is a hint: for groups
another member may take the message first, so call `take()` and handle `kWouldBlock`.
A process woken for a group message should take it, otherwise the wake-up is not
passed on.

## Errors

Constructors throw `std::system_error` for system call failures and
`std::runtime_error` for layout or configuration mismatches, initialization timeouts
and full slot tables (a constructor that races `recover()` waits up to 100 ms and
rescans before reporting a full table); `Subscriber` throws `std::invalid_argument` for
a group outside `0..kMaxGroups` or options that conflict with the group's policy, and `Topic` throws it for a name that does not begin with `/`.
