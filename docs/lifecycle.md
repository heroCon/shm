# Lifecycle and recovery

The process that wins `O_CREAT|O_EXCL` is the creator. It sizes the object, constructs every atomic object and queue, writes the magic/version/layout fields, then release-stores `READY`. Attachments never truncate; they verify the file size, wait with a bounded timeout, acquire-load `READY`, and validate the layout.

Process registration uses `FREE → CLAIMED → ACTIVE`. A subscriber reaper uses `ACTIVE → RECLAIMING`, prevents new deliveries, waits for in-flight publishers, drains queued references exactly once, and returns the slot to `FREE`. A generation is incremented on reuse, limiting PID-reuse ambiguity. Graceful shutdown uses the same ownership path. Heartbeat timeout means suspected failure, while destruction is explicit through `ShmPubSub::destroy`; detaching never destroys a region by default.

## Recovery boundary

Subscriber death after enqueue is recovered by draining its queue. A publisher killed while it owns a block between allocation and completed enqueue can leak that block: robust recovery of this narrow window requires a persistent allocation journal and is intentionally not claimed in version 2. The initializer dying before `READY` causes attachments to time out; an operator must unlink the object. `SIGSTOP` longer than the heartbeat can cause slot reclamation, so applications must choose timeout policy accordingly. The library is not crash-consistent across machine reset and has no durable delivery.
