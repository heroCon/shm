# Comparison with similar projects

This compares design choices, not benchmark numbers; the projects target different
scopes and their features change between releases. Statements about other projects
reflect their public documentation at the time of writing. Check their current docs
before deciding.

## Where shm_pubsub sits

shm_pubsub is deliberately small: one topic per region, fixed-size blocks, Linux only.
It aims to be the option you can drop into an existing C++11 code base without new
toolchains, daemons or threads (eCAL, for example, runs registration threads in every
process), and the one whose behaviour under `SIGKILL` is specified and tested point by
point.

| | shm_pubsub 0.3 | iceoryx2 | iceoryx (classic) | eCAL | Flow-IPC | cpp-ipc | Boost.Interprocess |
|---|---|---|---|---|---|---|---|
| Language / integration | C++11 headers | Rust core, C and C++ bindings | C++ libraries | C++ libraries and tools | C++17 libraries | C++ library | C++ headers |
| Central daemon | no | no | yes (RouDi) | no | no | no | no |
| Transport scope | one Linux host | one host, several OSes | one host | shared memory, UDP, TCP across hosts | one host | one host | one host |
| Payload model | fixed-size blocks, zero copy | zero copy, typed samples, dynamic sizes | zero copy, typed | serialized messages (protobuf etc.) | zero copy of structured data | byte buffers | build your own |
| Broadcast + competing consumers in one topic | yes (consumer groups) | publish-subscribe only, plus other patterns | no | no | n/a | route (1:N) and channel (N:M) types | build your own |
| Backpressure policy per reader | `kDropNewest` / `kDropOldest`, pool cannot be exhausted | configurable history and buffer, safe overflow | discard oldest or block publisher | transport-dependent | see project docs | see project docs | build your own |
| Dead-process cleanup | per-slot reclaim, local repair in `collect_garbage()`, `recover()`; verified at every fault point | dead-node detection and cleanup | RouDi monitoring | n/a (no shared ownership) | see project docs | see project docs | robust mutexes only |
| Monitoring | `shmtop`, `Topic::inspect` | CLI tooling | introspection | eCAL Monitor, recorder | – | – | – |

## When to use something else

- **You need several operating systems, request/response, events, or a typed service
  model:** iceoryx2 is the more complete zero-copy framework and is actively developed.
- **You need communication across hosts, recording and replay, or serialization:** eCAL
  provides the whole tool chain.
- **You need to pass complex structures with pointers (not flat blocks) between
  processes:** Flow-IPC targets this case.
- **You need a portable library that also works on Windows:** cpp-ipc or Boost-based
  designs.
- **You only need primitives (shared allocators, robust mutexes) to build your own
  protocol:** Boost.Interprocess.

## Where shm_pubsub is the better fit

- C++11 code bases that cannot pull in a Rust toolchain or a newer language standard.
- Deployments that forbid daemons or background threads, such as embedded Linux,
  real-time processes, and sandboxes.
- Work-queue fan-out next to broadcast on the same stream, without a broker.
- Systems where workers are killed routinely (watchdogs, OOM killer, `kill -9`
  deployments) and the remaining processes must keep running without restarting the
  region.
