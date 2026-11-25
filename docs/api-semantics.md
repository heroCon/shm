# API semantics

`publish_result` and `receive_result` are the normative API; the legacy Boolean wrappers report only success.

| Situation | Result and ownership |
|---|---|
| No active subscribed reader | `NO_SUBSCRIBERS`; no block is allocated. This is a successful, intentional drop. |
| Pool or competing queue full | `QUEUE_FULL`; the message is not delivered. |
| Some broadcast queues full | `PARTIAL` and `delivered` is the exact successful enqueue count. Full readers miss that message. |
| Message exceeds 4072 bytes | `MESSAGE_TOO_LARGE`; nothing is truncated or delivered; `required_size` gives capacity. |
| Receive buffer too small | `BUFFER_TOO_SMALL`; `actual_len`/`required_size` give the message size. The dequeued message is consumed and released, and no partial bytes are copied. |
| No queued message | `WOULD_BLOCK`. |

Each queue is FIFO for its successfully enqueued messages. Concurrent publishers have no global ordering guarantee. Broadcast is best effort and has at-most-once delivery per subscriber; competing delivery is at-most-once to one active subscriber. Queue pressure drops new messages, never old messages. Reconnecting creates a new slot generation and does not replay history. A subscriber reads its broadcast queue before the competing queue, so the two modes are not mutually ordered.

Constructors throw on system errors, incompatible layouts, timeouts, unsupported atomics, and exhausted registration slots. Shared-memory names must be POSIX names beginning with `/`.

## Zero-copy and blocking operations

| Operation | Ownership and result |
|---|---|
| `loan(size, write)` | Requires an empty `WriteLoan` and 1–4072 bytes. On success fill all requested bytes directly through `write.data()`. |
| `publish_loan(std::move(write), mode)` | Requires a loan from this publisher. A valid call consumes it on success, partial delivery, queue pressure, or no subscribers. The write pointer must no longer be used once the call starts. An invalid call leaves ownership with its original publisher. |
| `take(read)` | Requires an empty `ReadLoan`. Returns `WOULD_BLOCK` if both queues are empty. A successful loan exposes a read-only view. The block stays reserved until the last reference in queues or read loans is released. |
| `wait_take(read, timeout)` / `receive_wait(buf, len, actual, timeout)` | Wait on a process-shared Linux futex, recheck the queues after wakeup, and return `TIMED_OUT` if still empty at the deadline. A zero timeout performs an immediate attempt. Negative timeouts are invalid. `stop()` causes current and future blocking calls to return `STOPPED` unless a call has already dequeued a message. |

Both loan types are movable and release their references on reset or destruction. The endpoint must outlive its loans, and each loan object is owned by one caller at a time. Existing copy APIs interoperate with loan APIs. `subscribe()` enables delivery eligibility as before. Broadcast and competing traffic share one futex per region, so a wake can reach readers whose queues remain empty. A publisher that dies after enqueue but before notification can delay a wakeup until the next notification or the deadline.
