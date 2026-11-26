# Contributing

Build Debug and Release with `-DCMAKE_CXX_FLAGS=-Werror` and run
`ctest --test-dir build --output-on-failure`. CI also runs TSAN, ASAN+UBSAN, an arm64
build and `clang-format-18 --dry-run --Werror` over every `*.cpp`, `*.hpp` and `*.h`
file, so run `clang-format-18 -i` before committing.

- Every concurrency fix needs a time-bounded regression test that uses a unique region
  name (`unique_name()` / `ScopedName` in `tests/test_util.hpp`) and cleans up.
- A change to a multi-step shared-memory operation needs a
  `SHM_PUBSUB_FAULT_POINT("area.step")` between the steps. Add the name to
  `detail::kFaultPoints`, make sure `crash_matrix_test` reaches it, and update the matrix
  in `docs/crash-consistency.md`.
- A change to the shared layout or to `Config` hashing bumps `detail::kLayoutVersion`
  and needs a CHANGELOG entry.
- Document changes to delivery or recovery semantics, and avoid claims stronger than the
  tests show.

Submit focused commits with reproduction steps for bugs.
