# Changelog

## Unreleased

- Added move-only zero-copy write and read loans, and futex-backed blocking receives.
- Replaced heartbeat and recycle threads with OFD slot ownership and on-demand reclamation.
- Bumped the shared-memory layout to 4. Use a fresh name or recreate the region after old clients detach.

## 0.2.0 - 2026-09-08

- Defined structured delivery and receive semantics.
- Added creator/attacher initialization, layout validation, explicit destruction, safe subscriber slot reclamation, and lock-free platform checks.
- Added multi-process tests, CI, examples, an exported CMake target, and SHM/UDS benchmarks.
- Added lifecycle, platform, benchmark, contribution, roadmap, license, and release documentation.
