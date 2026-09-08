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
