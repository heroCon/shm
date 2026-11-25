# Lifecycle and recovery

The process that wins `O_CREAT|O_EXCL` sizes the object, constructs the shared state, writes the magic/version/layout fields and publishes `READY` with a release store. Attachments verify the file size, wait for `READY` with a bounded timeout, then validate the layout. Layout 4 requires a fresh shared-memory name or recreation of the old name after every old participant has exited.

Each endpoint owns a Linux open-file-description (OFD) byte-range lock on its slot. Independent endpoints in one process have separate file descriptions. The kernel releases ownership when the last descriptor for that description closes. Create endpoints after `fork()`. A child retaining an inherited endpoint descriptor also retains the lock and must not use the inherited endpoint. Pausing a process with `SIGSTOP` preserves its slot.

The library creates zero background threads. A new subscriber can reuse an abandoned slot. `collect_garbage()` scans abandoned reader slots on demand and returns the count reclaimed by that call. Allocation also scans once under pool pressure before retrying. `get_valid_subscriber_count()` reflects registered, subscribed slots and can include exited readers until a scan or reuse. The pool-exhaustion path may perform system calls and serialize maintenance within one endpoint.

Reclamation holds the slot lock, changes `ACTIVE` to `RECLAIMING`, and waits for a future scan if publishers still hold pins. Once the pin count reaches zero, reclamation drains queued broadcast references and marks the slot `FREE`. Graceful destruction uses the same path. Concurrent public calls and loans must finish before endpoint destruction. `stop()` wakes blocked receivers on that endpoint, allowing their caller to join before destruction. Detaching keeps the region by default.

## Crash recovery boundary

Queued broadcast references are reclaimable after a subscriber exits, provided its queue and outstanding publisher pins are intact. A process killed while it holds a write loan or a dequeued read loan can leak a block. Death during a queue operation, pin or reference update, or free-list operation can strand a queue or slot or reduce pool capacity. These transitions have no crash journal. If the initializer dies before `READY`, attachments time out. To reset a damaged region, detach all participants and recreate it. Delivery is in-memory and at most once.
