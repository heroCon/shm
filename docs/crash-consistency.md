# Crash consistency

The guarantee: if any process attached to a topic is killed at any instruction of the
data path, the remaining processes can return the topic to a state where every block
is either free or legitimately referenced, and publishing and taking work again. The
data path stays lock-free; recovery runs on demand in whichever process calls it.

What is **not** guaranteed: the message being published or taken by the dead process
may be lost (delivery is at most once), and a reader of a group may lose the message it
was taking.

## Mechanisms

**Block state word.** Each block header has a 64-bit atomic. Bits 0–55 are a holder
bitmap, one bit per endpoint slot holding the block as a loan. Bits 56–63 count queue
entries that reference the block (a block sits in a given queue at most once).

**Loan tables.** Each slot lists the blocks it may hold a bit on. An id is recorded
before the bit is set and forgotten after it is cleared, so the table is a superset of
the slot's bits and reclaiming a slot touches only its own entries. A block returns to the free list when the word
reaches zero. `take()` turns a queue reference into the taker's holder bit with one
`fetch_add`, so a crash leaves exactly one of the two. Holder bits name their owner,
which is what lets a dead slot's loans be released precisely; a plain reference count
cannot tell whose references remain.

**Liveness.** Each endpoint holds an OFD lock on its slot byte. The kernel drops it at
process death, so locking the byte proves the owner is gone (see
[lifecycle](lifecycle.md)).

**Group membership.** Each group has a member bitmap. Joining and leaving are a single
`fetch_or` / `fetch_and`, so a member killed at any point leaves the bitmap either with
or without its bit, never half updated; reclamation clears the bit and `recover()`
clears bits of dead slots. (0.3 used a member flag plus a counter; a crash between the
two left the counter permanently wrong.)

**Activity word.** Each slot has a word `{counter:32, op:8, block:24}` that the owner
writes before and after every multi-step operation (loan, publish, take, release,
leaving a group, reclaim). A `take()` that finds the queue empty writes nothing. It doubles as a seqlock for `recover()`: if every live slot shows an idle op
with the same counter before and after a scan, the scan saw a quiescent snapshot.
Counters never restart across slot reuse.

**Ordered steps.** Publishers add the queue reference before enqueueing, and drop it
again if the enqueue fails. A publisher that discards the oldest entry under
`kDropOldest` releases the queue cell before dropping that entry's reference. Readers
claim a cell, convert its reference, then release the cell. Every intermediate state is
therefore either over-counted (fixed by recomputation) or visible in a queue cell (fixed
by cell repair); none hides a reference.

## Repair

`collect_garbage()` reclaims dead slots (see [lifecycle](lifecycle.md)) and repairs the
queue cells they left behind. It does not need a quiescent topic. It waits, bounded by
100 ms, until every live endpoint that was inside an operation has moved on, then:

- a cell that is still reserved but not committed becomes a tombstone (`holes_filled`);
- a cell that was claimed and not released is released (`cells_released`), and its queue
  reference is dropped unless a dead slot's holder bit shows the claimer already
  converted it;
- the block a dead publisher was delivering is recounted from the queues and its word
  is installed by CAS (`blocks_recounted`);
- a dead member of an at-least-once group has each held block put back on the group
  queue before its holder bit is cleared.

If a wait times out the slot stays `RECLAIMING` and the next pass resumes it. Where a
dead owner may have leaked a block that this pass cannot attribute, it sets a shared
`repair_pending` flag. The next `collect_garbage()`, or the next `loan()` that finds
the pool empty, then runs `recover()` and clears the flag. `collect_garbage()` takes
an optional `RecoveryReport`; `full_repair` says that fallback ran.

`recover()` is unchanged in shape:

1. takes the maintenance lock byte, then locks every slot byte it can; those slots are
   dead and are reclaimed;
2. reads the activity words of the live slots, scans the free list, every block word and
   every queue, then reads the activity words again; any non-idle or changed word
   returns `kBusy`;
3. recomputes each block's word as (observed live holder bits) | (queue entries << 56)
   and installs it by CAS; a block whose word becomes zero is pushed to the free list
   (`orphans_freed`), and clears member bits of dead slots (`members_cleared`);
4. fills cells a dead producer reserved but never committed with a tombstone that
   readers skip (`holes_filled`), and releases cells a dead consumer claimed but never
   released (`cells_released`);
5. clears pin masks left by dead publishers (`pins_cleared`).

The repairs in steps 3–5 are made by CAS against the snapshot, so they are safe even if
live endpoints resume immediately after the scan.

## Fault matrix

`tests/crash_matrix_test.cpp` is built with `SHM_PUBSUB_FAULT_INJECTION`, which turns
each `SHM_PUBSUB_FAULT_POINT(name)` into a check that sends `SIGKILL` to the process on
the N-th hit named by `SHM_PUBSUB_CRASH_AT=name:N`. In normal builds the macro expands
to nothing.

The victim process runs a publisher, a never-reading `kDropOldest` subscriber, a reader
and a group member, then repeatedly forks a subscriber, kills it and collects it. The
test process keeps a private subscriber and a group-1 member alive throughout. For each
point and hit count 1, 2, 3 and 5 it kills the victim, recovers, and requires all
64 blocks free, 8 publish/take rounds succeeding for both survivors, and zero
inconsistencies. A second pass per point uses only `collect_garbage()` and requires a
later `recover()` to find nothing left to repair.

| Fault point | State left by the crash | Repaired by | Hits passed |
|---|---|---|---|
| `loan.after_pop` | Block popped from the free list, word still 0 | orphan sweep | 1 2 3 5 + gc |
| `loan.after_claim` | Block held by the dead publisher's bit | slot reclaim | 1 2 3 5 + gc |
| `publish.after_ref` | Extra queue reference, no queue entry; stale pin mask | recompute | 1 2 3 5 + gc |
| `queue.after_reserve` | Reserved, uncommitted cell (hole) blocking the queue | tombstone fill, recompute | 1 2 3 5 + gc |
| `publish.after_enqueue` | Message in some queues, not yet in others | slot reclaim | 1 2 3 5 + gc |
| `publish.before_release` | Publisher bit still set after fan-out | slot reclaim | 1 2 3 5 + gc |
| `drop_oldest.after_claim` | Oldest cell claimed by the publisher, not released | recompute | 1 2 3 5 + gc |
| `drop_oldest.after_release_cell` | Cell released, its reference not yet dropped | recompute | 1 2 3 5 + gc |
| `take.after_claim` | Cell claimed, reference not converted | queue drain or cell release | 1 2 3 5 + gc |
| `take.after_convert` | Holder bit set, cell not released | slot reclaim, cell release | 1 2 3 5 + gc |
| `release.before_push` | Word reached zero, block not yet on the free list | orphan sweep | 1 2 3 5 + gc |
| `reclaim.after_pin_wait` | A reclaimer died mid-reclaim | next reclaim (idempotent) | 1 2 3 5 + gc |
| `reclaim.after_mark` | Queue cell marked released, reference not dropped | recompute | 1 2 3 5 + gc |
| `reclaim.after_resolve` | A reclaimer died after resolving other queues | next reclaim | 1 2 3 5 + gc |
| `endpoint.after_construct` | Slot activated, constructor not finished | slot reclaim | 1 2 3 5 + gc |
| `endpoint.before_destroy` | Destructor entered, slot not yet reclaimed | slot reclaim | 1 2 3 5 + gc |
| `subscribe.after_join` | Joined a group, subscribed flag not yet stored | slot reclaim | 1 2 3 5 + gc |
| `unsubscribe.before_leave` | Unsubscribed flag stored, membership bit still set | slot reclaim | 1 2 3 5 + gc |
| `repair.before_tombstone` | A hole was observed, the tombstone not yet stored | next local repair | 1 2 3 5 + gc |
| `repair.before_cell_release` | A claimed cell was observed, not yet released | next local repair | 1 2 3 5 + gc |
| `redeliver.after_enqueue` | A redelivered reference is queued, the holder bit remains | next reclaim (duplicates, then converges) | 1 2 3 5 + gc |
| `redeliver.after_convert` | Same window, immediately before the holder bit is dropped | next reclaim | 1 2 3 5 + gc |
| `recount.before_cas` | A recounted word was not yet installed | next recount or `recover()` | 1 2 3 5 + gc |
| `group.drain_after_release` | A drained cell was released, its reference not dropped | recompute | 1 2 3 5 + gc |

"gc" is the `collect_garbage()`-only pass. Run `build/crash_matrix_test` to print the counters
behind each row. `local_repair_test` kills an endpoint while other endpoints keep
running and checks that `collect_garbage()` alone unblocks the readers.
`chaos_test` kills publishers for several seconds under busy-polling readers with no
supervisor, and under blocking readers with `collect_garbage()`, and bounds every
reader's stall. Separately, `crash_leak_test` kills a writer holding two loans, a
reader holding a loan, and a reader holding a loan with a backlog, 40 times each, and
checks that `collect_garbage()` returns the pool to full after every kill.

## Remaining windows

These are known and deliberate; none silently loses capacity.

- **No quiescent window.** `recover()` needs a moment where every live endpoint is
  between operations. A process stopped (`SIGSTOP`, debugger) inside an operation, or
  sustained traffic, makes it return `kBusy`; slot reclamation still works. In 0.3 an
  idle reader that busy-polled `take()` also blocked it, because every empty poll wrote
  the activity word; measured success with such a reader was 0% even without traffic.
  Since 0.4 empty polls write nothing (measured: 100% idle, 99.6% at 1k msg/s, 81% at
  50k msg/s on a 4-vCPU VM).
  A hole left by a dead producer stops readers of that queue only until
  `collect_garbage()` runs. With `Options::self_heal` (the default) a reader that finds
  a non-empty queue whose head is not ready, or a publisher that finds a full queue or
  an empty pool, runs it itself, at most once per `self_heal_interval` (default 10 ms)
  and only if no other thread in the process is already collecting. `loan()` still
  reports `kPoolExhausted` only when that repair cannot find a block.
- **Free-list structure.** A crash cannot corrupt the free list: pop and push each
  commit with a single CAS, and the crash points around them are covered above. If the
  list is found structurally damaged anyway (for example by a stray write into shared
  memory), `recover()` reports `kCorrupted` and does not try to rebuild it; recreate the
  region.
- **Lost wake-ups.** A publisher killed after enqueueing but before waking a reader
  delays that reader until the next publish, `stop()`, or its timeout.
- **Lost messages.** Private subscribers and at-most-once groups do not get the message
  a dead endpoint was publishing or taking. An at-least-once group puts a message a
  dead member had claimed or was holding back on the group queue; a crash in the middle
  of that step can deliver it twice, which that policy allows. Past `max_deliveries`
  the message is counted in `dead_lettered` and dropped.
- **Counter wrap.** The free-list ABA tag and activity counters are 32 bits. A false
  match needs exactly 2^32 operations on one slot during a single `recover()` scan.
- **Creator death before `READY`.** Attachers time out; see [lifecycle](lifecycle.md).
