# Lock-free and platform contract

The bounded queue is Vyukov's MPMC sequence-number algorithm. Its acquire/release sequence publication protects cell data; enqueue/dequeue positions are relaxed because cell sequence numbers provide synchronization. The free-list packs a 32-bit index and 32-bit monotonically increasing ABA counter into one atomic word and requires unique ownership before `push`.

Construction checks that 32-bit, 64-bit, `size_t`, and the composite free-list head atomics report `is_lock_free()`. Otherwise it throws before registration. The supported target is Linux with a coherent, process-shared mapping and a C++ implementation whose lock-free atomics operate across processes. C++ does not itself standardize process-shared atomics, so this is an explicit platform ABI requirement rather than a portable C++ guarantee. All participants must use the same architecture, compiler ABI, constants, and layout version.

Operations are lock-free when the runtime check passes: a stalled participant does not block queue/free-list progress. They are not wait-free and may starve under contention. The 32-bit ABA counter can theoretically wrap; this is outside the supported operational lifetime.
