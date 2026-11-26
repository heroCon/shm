# Roadmap

Done in 0.3: compile-time capacities, consumer groups, overflow policies, per-queue
wake-ups, `WaitSet`, crash recovery with a fault matrix, sanitizer and arm64 CI,
two-process latency benchmarks, `shmtop`.

Next:

- **Variable block sizes**: several size classes per region, chosen at loan time, with
  the pool budget computed per class.
- **Service discovery**: a directory region listing topics, their `Config` hashes and
  endpoints, readable by `shmtop`.
- **Late-joiner history**: an opt-in ring of the last N messages that a new subscriber
  can replay.
- **Request/response** on top of two topics with correlation ids.
- **Model checking** of the publish/take/reclaim protocol (for example with a TLA+ or
  GenMC model) to complement the fault matrix.
- **Bindings**: a C API and a Python binding over the same layout.
- **Packaging**: Conan and vcpkg recipes, and a tagged 0.3.0 release.
