# Roadmap

Done in 0.4: layout 6, group membership bitmaps and a fixed group policy, local
repair in `collect_garbage()` with self-heal, durable and at-least-once groups,
`Overflow::kBlock`, an optional large block class, a TLA+ model, a C API, a Python
ctypes package, Conan and vcpkg recipes, and an amalgamation.

Done in 0.3: compile-time capacities, consumer groups, overflow policies, per-queue
wake-ups, `WaitSet`, crash recovery with a fault matrix, sanitizer and arm64 CI,
two-process latency benchmarks, `shmtop`.

Next:

- **Service discovery**: a directory region listing topics, their `Config` hashes and
  endpoints, readable by `shmtop`.
- **Late-joiner history**: an opt-in ring of the last N messages that a new subscriber
  can replay.
- **Request/response** on top of two topics with correlation ids.
- **A tagged 0.4.0 release** once the stacked 0.4 pull requests are merged.
