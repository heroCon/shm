# Lifecycle

## Creating and attaching

The process that wins `shm_open(O_CREAT | O_EXCL)` sizes the object, applies
`Options::mode` with `fchmod` (the umask would otherwise narrow it), constructs the
region and publishes `READY` with a release store. Other processes wait up to
`Options::initialization_timeout` for the expected size and for `READY`, then compare
the magic, layout version (5), size and `Config` hash. A mismatch throws; nothing in the
region is modified.

If the creator dies before `READY`, attachers time out. Remove the name with
`Topic<>::destroy(name)` and create it again. `Options::unlink_on_destroy` unlinks the
name when that `Topic` object is destroyed; by default the region outlives every
process, which is what lets a restarted process rejoin.

## Endpoints and liveness

Every endpoint opens its own file descriptor for the region and holds a Linux
open-file-description (OFD) write lock on one byte, its slot index. The kernel drops the
lock when the last descriptor of that description closes, including on `SIGKILL`, so a
slot whose byte can be locked by someone else belongs to a dead owner. No heartbeat or
PID check is involved, so PID reuse cannot confuse it.

- Create endpoints after `fork()`. A child that inherits an endpoint's descriptor also
  keeps the lock alive and must not use the inherited endpoint.
- `SIGSTOP` keeps a slot alive. A stopped process in the middle of an operation also
  makes `recover()` report `kBusy` until it continues or dies.
- Several endpoints in one process are independent; each has its own description.

Slots carry a generation that changes on every reuse, and a per-slot activity word (see
[crash consistency](crash-consistency.md)) whose counter never restarts.

## Reclamation

The library has no threads, so reclamation happens on demand:

| Trigger | What runs |
|---|---|
| A new endpoint lands on a slot whose owner died | `reclaim_slot` for that slot. |
| `Topic::collect_garbage()` | `reclaim_slot` for every dead slot; if any was reclaimed, one `recover()` attempt. Returns the number of slots reclaimed. |
| `loan()` finds the pool empty | `collect_garbage()`, then `recover()`, then `kPoolExhausted`. |
| `Topic::recover(&report)` | Full repair, see [crash consistency](crash-consistency.md). |

Reclaiming a slot, while holding its lock byte:

1. moves the lifecycle word from `ACTIVE` to `RECLAIMING`, so publishers stop pinning
   it;
2. for a subscriber slot, waits up to 100 ms until no **live** publisher's pin mask
   names it (masks of dead publishers are ignored). If they do not, the slot stays
   `RECLAIMING` and a later call finishes it;
3. drains the private queue, releasing every queued reference, including cells that
   were claimed but not yet released, and leaves the consumer group (discarding the
   group's backlog if it was the last member of a non-durable group);
4. for a publisher slot, clears its pin mask;
5. clears the slot's holder bit on the blocks listed in its loan table, releasing
   blocks held as loans, and marks the slot `FREE`. The next owner bumps the
   generation. Cost is proportional to the loans, not the pool size.

Every step is idempotent, so a reclaimer that is itself killed is finished by the next
one. Graceful endpoint destruction uses the same path; destroy loans before their
endpoint and endpoints before their `Topic`.

A supervisor that restarts crashed workers should call `collect_garbage()` after a child
exits (or periodically). `shmtop /name` shows dead slots as `live=no` before they are
reclaimed.

## Upgrading a region

Layout 6 (0.4) is incompatible with layout 5 (0.3) and layout 4 (0.2). Stop every
older process, then `Topic<>::destroy(name)` or choose a new name. A `Config` change is also a layout change
for this purpose: the hash check rejects it.
