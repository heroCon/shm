# Lock-free and platform contract

The bounded queues use Vyukov's MPMC sequence-number algorithm. Acquire/release sequence publication orders cell data, and enqueue/dequeue position accesses are relaxed. The free-list packs a 32-bit index and 32-bit ABA counter into one atomic word. Speculative next-link reads use atomic accesses. The caller owns each index until its last reference is released.

Construction checks the required atomics at runtime. Linux participants must share an architecture, ABI, constants, and layout version. The implementation relies on Linux process-shared lock-free atomics in coherent shared mappings, a native 32-bit futex word, and OFD locks (Linux 3.15+).

The normal non-blocking data path uses atomics, with a futex wake syscall when a waiter has armed notification. Queue reservations can stall progress if a participant stops before committing a cell. The implementation is neither wait-free nor crash-consistent. OFD locks are used for registration and reader-slot maintenance outside the successful data path. The 32-bit free-list ABA counter and 31-bit notification generation can theoretically wrap.
