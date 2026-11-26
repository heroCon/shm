# Crash consistency

The guarantee: if any process attached to a topic is killed at any instruction of the
data path, the remaining processes can return the topic to a state where every block
is either free or legitimately referenced, and publishing and taking work again. The
data path stays lock-free; recovery runs on demand in whichever process calls it.

What is **not** guaranteed: the message being published or taken by the dead process
may be lost (delivery is at most once), and a reader of a group may lose the message it
was taking.

## Mechanisms

**Block state word.** Each block header has a 64-bit atomic. Bits 0–47 are a holder
bitmap, one bit per endpoint slot holding the block as a loan. Bits 48–63 count queue
entries that reference the block. A block returns to the free list when the word
reaches zero. `take()` turns a queue reference into the taker's holder bit with one
`fetch_add`, so a crash leaves exactly one of the two. Holder bits name their owner,
which is what lets a dead slot's loans be released precisely; a plain reference count
cannot tell whose references remain.

**Liveness.** Each endpoint holds an OFD lock on its slot byte. The kernel drops it at
process death, so locking the byte proves the owner is gone (see
[lifecycle](lifecycle.md)).

**Activity word.** Each slot has a word `{counter:32, op:8, block:24}` that the owner
writes before and after every multi-step operation (loan, publish, take, release,
reclaim). It doubles as a seqlock for `recover()`: if every live slot shows an idle op
with the same counter before and after a scan, the scan saw a quiescent snapshot.
Counters never restart across slot reuse.

**Ordered steps.** Publishers add the queue reference before enqueueing, and drop it
again if the enqueue fails. A publisher that discards the oldest entry under
`kDropOldest` releases the queue cell before dropping that entry's reference. Readers
claim a cell, convert its reference, then release the cell. Every intermediate state is
therefore either over-counted (fixed by recomputation) or visible in a queue cell (fixed
by cell repair); none hides a reference.

## Repair

`collect_garbage()` reclaims dead slots (see [lifecycle](lifecycle.md)) and, if it
reclaimed any, attempts `recover()` once. `recover()`:

1. takes the maintenance lock byte, then locks every slot byte it can; those slots are
   dead and are reclaimed;
2. reads the activity words of the live slots, scans the free list, every block word and
   every queue, then reads the activity words again; any non-idle or changed word
   returns `kBusy`;
3. recomputes each block's word as (observed live holder bits) | (queue entries << 48)
   and installs it by CAS; a block whose word becomes zero is pushed to the free list
   (`orphans_freed`);
4. fills cells a dead producer reserved but never committed with a tombstone that
   readers skip (`holes_filled`), and releases cells a dead consumer claimed but never
   released (`cells_released`);
5. clears publisher pins left by dead publishers (`pins_cleared`).

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
| `publish.after_ref` | Extra queue reference, no queue entry; dead pin | recompute, pin clear | 1 2 3 5 + gc |
| `queue.after_reserve` | Reserved, uncommitted cell (hole) blocking the queue | tombstone fill, recompute | 1 2 3 5 + gc |
| `publish.after_enqueue` | Message in some queues, not yet in others | slot reclaim, pin clear | 1 2 3 5 + gc |
| `publish.before_release` | Publisher bit still set after fan-out | slot reclaim | 1 2 3 5 + gc |
| `drop_oldest.after_claim` | Oldest cell claimed by the publisher, not released | recompute | 1 2 3 5 + gc |
| `drop_oldest.after_release_cell` | Cell released, its reference not yet dropped | recompute | 1 2 3 5 + gc |
| `take.after_claim` | Cell claimed, reference not converted | queue drain or cell release | 1 2 3 5 + gc |
| `take.after_convert` | Holder bit set, cell not released | slot reclaim, cell release | 1 2 3 5 + gc |
| `release.before_push` | Word reached zero, block not yet on the free list | orphan sweep | 1 2 3 5 + gc |
| `reclaim.after_pin_wait` | A reclaimer died mid-reclaim | next reclaim (idempotent) | 1 2 3 5 + gc |
| `reclaim.after_mark` | Queue cell marked released, reference not dropped | recompute | 1 2 3 5 + gc |

"gc" is the `collect_garbage()`-only pass. Run `build/crash_matrix_test` to print the counters
behind each row. Separately, `crash_leak_test` kills a writer holding two loans, a
reader holding a loan, and a reader holding a loan with a backlog, 40 times each, and
checks that `collect_garbage()` returns the pool to full after every kill.

## Remaining windows

These are known and deliberate; none silently loses capacity.

- **No quiescent window.** `recover()` needs a moment where every live endpoint is
  between operations. A process stopped (`SIGSTOP`, debugger) inside an operation, or a
  workload that never pauses, makes it return `kBusy`; slot reclamation still works.
  Until a repair runs, a hole left by a dead producer stops readers of that queue at the
  hole. Because of the pool budget, only leaks can exhaust the pool, so `loan()` reaching
  `kPoolExhausted` means repair could not run.
- **Free-list structure.** A crash cannot corrupt the free list: pop and push each
  commit with a single CAS, and the crash points around them are covered above. If the
  list is found structurally damaged anyway (for example by a stray write into shared
  memory), `recover()` reports `kCorrupted` and does not try to rebuild it; recreate the
  region.
- **Lost wake-ups.** A publisher killed after enqueueing but before waking a reader
  delays that reader until the next publish, `stop()`, or its timeout.
- **Lost messages.** The message in flight in the dead process is not redelivered. A
  group member killed between claiming and converting a message loses it for the group.
- **Counter wrap.** The free-list ABA tag and activity counters are 32 bits. A false
  match needs exactly 2^32 operations on one slot during a single `recover()` scan.
- **Creator death before `READY`.** Attachers time out; see [lifecycle](lifecycle.md).
