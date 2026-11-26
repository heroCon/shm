# Lock-free design and platform contract

## Data structures

- **Free list** (`detail/lock_free_list.hpp`): the iceoryx `MpmcLoFFLi` index list. The
  head packs a 32-bit index and a 32-bit ABA tag into one 64-bit atomic; pop and push
  each commit with one CAS. `force_push` (used only by `recover()` on blocks proven
  unreferenced) and `for_each_free` (a bounded, cycle-detecting walk) were added for
  repair.
- **Queues** (`detail/mpmc_queue.hpp`): Vyukov's bounded MPMC queue with 64-bit
  positions, so positions never wrap. Dequeue is split into `try_claim` and `release`,
  which lets a reader convert the cell's reference into its holder bit before the cell
  becomes reusable. Enqueue and dequeue positions sit on separate cache lines. Every
  private subscriber and every consumer group has one queue.
- **Block words** and **slot words** are described in
  [crash consistency](crash-consistency.md).

## Publish path

One pass over the subscriber slots: for each `ACTIVE`, subscribed, private slot the
publisher pins the slot in its lifecycle word (a CAS that fails once reclamation has
started), adds a queue reference, enqueues, and unpins. Then one enqueue per non-empty
group. Under `kDropOldest` a full queue is made room for by claiming and releasing its
oldest cell (at most two attempts). A successful publish involves no system call unless
a reader is waiting.

## Wake-ups

Each queue has a `WakeWord {generation, waiters}` on its own cache line. A waiter
increments `waiters`, issues a `seq_cst` fence, reads `generation`, re-checks the queue,
then calls `FUTEX_WAIT` on `generation`. The publisher issues a `seq_cst` fence after
its enqueues and touches only queues whose `waiters` is non-zero: it bumps `generation`
and wakes all waiters of a private queue or one waiter of a group queue. The two fences
ensure that either the waiter sees the message or the publisher sees the waiter.

`WaitSet` registers on several `WakeWord`s at once and sleeps in `futex_waitv`
(Linux 5.16+) with an absolute `CLOCK_MONOTONIC` deadline.

## Progress

The data path is lock-free in the usual sense, not wait-free: a reader can be blocked by
a producer that reserved a cell and has not yet committed it. If that producer dies,
`recover()` fills the cell. Reclamation and recovery take OFD locks and are not on the
data path.

## Platform requirements

- Linux 3.15+ for OFD locks; 5.16+ for `WaitSet`.
- 32- and 64-bit atomics that are always lock-free (`ATOMIC_INT_LOCK_FREE == 2` and
  `ATOMIC_LLONG_LOCK_FREE == 2`), checked at compile time, and coherent `MAP_SHARED`
  mappings. Tested on x86-64 and AArch64.
- All participants use the same architecture, ABI, `Config` and layout version; the
  region header checks the last two.
