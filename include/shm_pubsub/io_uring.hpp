#ifndef SHM_PUBSUB_IO_URING_HPP_
#define SHM_PUBSUB_IO_URING_HPP_

#include <linux/futex.h>
#include <linux/io_uring.h>

#include <atomic>
#include <cstdint>

#include "shm_pubsub/detail/platform.hpp"
#include "shm_pubsub/wait_set.hpp"

namespace shm_pubsub {

// What an IORING_OP_FUTEX_WAIT submission needs for a subscriber's wake word.
// Linux 6.7+ and a kernel that accepts the opcode; older kernels return
// EINVAL or ENOSYS from io_uring_enter and the caller should fall back to
// EventfdBridge.
//
// `waiters` stays incremented from arm_futex_wait until disarm_futex_wait.
// Publishers skip the generation increment when that count is zero, so a wait
// submitted without arming never completes on publish.
struct FutexWait {
  uint32_t* address;
  uint64_t expected;
  uint64_t mask;
  uint32_t futex_flags;
  std::atomic<uint32_t>* waiters;
  bool armed;
};

// Increments the subscriber's waiter count, then samples the generation.
// *should_wait is false when the subscriber is already ready or stopped:
// submitting the futex wait is unnecessary, but disarm_futex_wait is still
// required. When *should_wait is true, submit `wait` and disarm only after
// the completion (or after cancelling the SQE).
template <class Subscriber>
FutexWait arm_futex_wait(Subscriber& subscriber, bool* should_wait) {
  const detail::WaitHandle handle = subscriber.wait_handle();
  handle.waiters->fetch_add(1, std::memory_order_seq_cst);
  // Pairs with the relaxed load in Publisher::wake. A publish that lands
  // after the increment and before the sample either bumps the generation
  // this sample observes, or is visible to the readiness check below.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  FutexWait out;
  out.address = detail::futex_address(handle.generation);
  out.expected = handle.generation->load(std::memory_order_acquire);
  out.mask = FUTEX_BITSET_MATCH_ANY;
  out.futex_flags = FUTEX2_SIZE_U32;
  out.waiters = handle.waiters;
  out.armed = true;
  const bool running = handle.running->load(std::memory_order_acquire);
  const bool ready = handle.ready(handle.source);
  if (should_wait) *should_wait = running && !ready;
  return out;
}

inline void disarm_futex_wait(FutexWait* wait) {
  if (!wait || !wait->armed || !wait->waiters) return;
  wait->waiters->fetch_sub(1, std::memory_order_release);
  wait->armed = false;
}

// Fills a kernel SQE the way liburing's io_uring_prep_futex_wait does.
// The futex2 size flags go in fd; sqe->futex_flags carries io_uring SQE flags
// (none here) and addr3 carries the bitset mask.
inline void prepare_futex_wait(io_uring_sqe* sqe, const FutexWait& wait) {
  sqe->opcode = IORING_OP_FUTEX_WAIT;
  sqe->fd = static_cast<int>(wait.futex_flags);
  sqe->addr = reinterpret_cast<uint64_t>(wait.address);
  sqe->len = 0;
  sqe->off = wait.expected;
  sqe->addr3 = wait.mask;
  sqe->futex_flags = 0;
}

}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_IO_URING_HPP_
