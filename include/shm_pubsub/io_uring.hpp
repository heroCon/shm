#ifndef SHM_PUBSUB_IO_URING_HPP_
#define SHM_PUBSUB_IO_URING_HPP_

#include <linux/futex.h>
#include <linux/io_uring.h>

#include <cstdint>

#include "shm_pubsub/detail/platform.hpp"
#include "shm_pubsub/wait_set.hpp"

namespace shm_pubsub {

// What an IORING_OP_FUTEX_WAIT submission needs for a subscriber's wake word.
// Linux 6.7+ and a kernel that accepts the opcode; older kernels return
// EINVAL or ENOSYS from io_uring_enter and the caller should fall back to
// EventfdBridge.
struct FutexWait {
  uint32_t* address;
  uint64_t expected;
  uint64_t mask;
  uint32_t futex_flags;
};

template <class Subscriber>
FutexWait futex_wait_for(Subscriber& subscriber) {
  const detail::WaitHandle handle = subscriber.wait_handle();
  FutexWait out;
  out.address = detail::futex_address(handle.generation);
  out.expected = handle.generation->load(std::memory_order_acquire);
  out.mask = FUTEX_BITSET_MATCH_ANY;
  out.futex_flags = FUTEX2_SIZE_U32;
  return out;
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
