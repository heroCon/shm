# lockfree_shm

This repository is a lightweight Linux shared-memory publish/subscribe example. It demonstrates broadcast and competing-consumer delivery modes, lock-free MPMC message queues, lock-free free-list block management, fixed-size block allocation, heartbeat tracking, and offline resource recycling for multi-process communication.

## Contents

- `publisher`: Publisher process example that continuously sends `TestTopic` messages.
- `subscriber`: Subscriber process example that polls and receives messages in a non-blocking loop.
- `shm_pubsub.h`: Core pub/sub implementation (shared memory layout, registration, publish/receive, recycling).
- `lockfree_list.hpp`: Lock-free free-list used for block allocation/release.
- `TestTopic.h`: Sample message structure.
- `DelayTime.h`: Optional delay measurement helper.

## Requirements

- Linux (POSIX shared memory APIs such as `shm_open` and `mmap`)
- CMake >= 3.10
- C++11-compatible compiler or newer

## Build

```bash
mkdir -p build
cd build
cmake ..
make
```

Build outputs:

- `build/publisher`
- `build/subscriber`

## Run

Use two terminals:

1. Start subscriber

```bash
./build/subscriber
```

2. Start publisher

```bash
./build/publisher
```

The subscriber should print continuously increasing timestamps/counters.

## Design Summary

- A fixed-size data block pool is stored in shared memory.
- Publishing allocates one block and writes payload; broadcast mode enqueues the block ID to each subscriber queue, while competing-consumer mode enqueues it to the shared queue.
- Publishers can choose `BROADCAST` or `COMPETING`: the former lets every subscriber receive the same data, while the latter lets subscribers compete for messages from one shared queue.
- Heartbeat checks detect offline publishers/subscribers and trigger recycling.


### Delivery Modes

`ShmPubSub::publish` uses broadcast mode by default, or callers can explicitly request competing-consumer mode:

```cpp
pub.publish(msg, sizeof(TestTopic), ShmPubSub::BROADCAST);   // every subscriber receives a copy
pub.publish(msg, sizeof(TestTopic), ShmPubSub::COMPETING);   // one subscriber consumes the message
```

## Notes

- This is an experimental/demo implementation focused on mechanism clarity.
- Shared-memory cleanup is currently simple (includes `shm_unlink` in teardown), which may need refinement for complex multi-process shutdown sequences.
- Validate behavior under stress and abnormal exits before production use.

## 中文版本

See `README.md`.
