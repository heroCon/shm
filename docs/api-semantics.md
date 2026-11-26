# API semantics

Everything lives in namespace `shm_pubsub`. A `Topic<Config>` owns one mapping of a
named region; `Topic::Publisher` and `Topic::Subscriber` are endpoints created from it.
Each endpoint occupies one slot in the region for its lifetime.

## Configuration

`Config` supplies `kBlockCount`, `kBlockSize` (including a 64-byte block header, so the
payload limit is `Topic::max_payload_size()`), `kQueueCapacity`, `kMaxPublishers`,
`kMaxSubscribers`, `kMaxGroups`, `kMaxLoansPerPublisher` and `kMaxLoansPerSubscriber`.
Derive from `DefaultConfig` and redeclare the members you change. Compile-time checks
enforce:

- `kQueueCapacity` is a power of two and smaller than `kBlockCount`;
- at most 48 endpoints (`kMaxPublishers + kMaxSubscribers`) and 24 publishers;
- the **pool budget** `kBlockCount >= (kMaxSubscribers + kMaxGroups) * kQueueCapacity +
  kMaxSubscribers * kMaxLoansPerSubscriber + kMaxPublishers * kMaxLoansPerPublisher`.

Because every reference to a block is either a queue entry or a loan, and both are
bounded, the budget means the pool cannot run dry no matter how slowly subscribers read.
`kPoolExhausted` therefore indicates leaked blocks, which `loan()` tries to recover
before it reports the status.

The region header stores a hash of the configuration and the layout version (5). An
attach with a different `Config` or layout throws `std::runtime_error`.

## Publishing

| Call | Result |
|---|---|
| `loan(size, write)` | `kOk` and a block of `size` bytes (1..`max_payload_size()`); `kLoanLimit` when the endpoint already holds `kMaxLoansPerPublisher` loans; `kMessageTooLarge`; `kInvalidArgument` for size 0 or a non-empty `write`; `kPoolExhausted` only after leaks that `collect_garbage()` and `recover()` could not undo. |
| `write.resize(n)` | Shrinks or grows the payload within `capacity()`. |
| `publish(std::move(write))` | Consumes the loan whenever it belongs to this publisher, whatever the status. Returns a `PublishResult`. |
| `publish_copy(data, size)` | `loan` + `memcpy` + `publish`; returns `kNoSubscribers` without allocating when nobody is eligible. |

`PublishResult` has `targets` (subscribed private subscribers plus groups with at least
one member), `delivered`, `dropped_oldest` and `status`:

| Status | Meaning |
|---|---|
| `kOk` | Every target queued the message. |
| `kNoSubscribers` | No targets; the message was dropped on purpose. |
| `kPartial` | Some targets queued it; the others were full under `kDropNewest`. |
| `kQueueFull` | Every target was full under `kDropNewest`. |

`ok()` is true for `kOk`, `kNoSubscribers` and `kPartial`. Every block header records
the publisher slot, a per-publisher `sequence` that increments on every publish, and a
`CLOCK_MONOTONIC` timestamp. A reader that sees a gap in `sequence` for one publisher
missed messages, through a full queue, `kDropOldest`, or a group sibling taking them.

## Subscribing

`Subscriber(topic, SubscriberOptions{group, overflow})`:

- `group == 0` gives the subscriber a **private queue**; every publish is offered to it.
- `group` in `1..kMaxGroups` joins a **consumer group**. Each group has one queue; every
  publish is offered once per group, and exactly one member takes it.
- `overflow` selects `kDropNewest` (a full queue rejects the new message) or
  `kDropOldest` (the publisher discards the oldest queued message to make room). For a
  group the policy of the most recently registered member applies to the whole group.

A subscriber is subscribed on construction. `unsubscribe()` makes it ineligible for new
messages; its private queue keeps what it already holds, and `take()` returns
`kNotSubscribed` until `subscribe()`. A group member that unsubscribes leaves the group;
queued group messages stay for the remaining members.

| Call | Result |
|---|---|
| `take(read)` | `kOk` with a read-only view; `kWouldBlock` if the queue is empty; `kNotSubscribed`; `kLoanLimit` at `kMaxLoansPerSubscriber` held loans; `kInvalidArgument` for a non-empty `read`; `kCorrupted` if a queued block id or size fails validation (the entry is skipped). |
| `wait_take(read, timeout)` | Blocks on the queue's futex word. `kTimedOut` at the deadline, `kStopped` after `stop()`. A zero timeout makes one attempt; negative timeouts are invalid. |
| `receive(buf, size)` / `wait_receive(...)` | Copy variants returning `ReceiveResult{status, size}`. `kBufferTooSmall` reports the size; the message is consumed and not copied. |
| `stop()` | Any thread may call it. Current and future blocking calls return `kStopped`. A stopping group member passes a pending wake-up on to another member. |

`ReadLoan` exposes `data()`, `size()`, `sequence()`, `timestamp_ns()` and
`publisher_id()`. Both loan types are move-only and release their reference on
`reset()` or destruction; the endpoint must outlive its loans.

## Ordering and delivery

- Each queue is FIFO. Messages of one publisher reach one queue in publish order.
  Concurrent publishers have no global order.
- Delivery is at most once per private subscriber and at most once per group.
  Reconnecting creates a new slot generation and does not replay history.
- A message taken by a group member that then crashes is lost, not redelivered.

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
and full slot tables; `Subscriber` throws `std::invalid_argument` for a group outside
`0..kMaxGroups` and `Topic` throws it for a name that does not begin with `/`.
