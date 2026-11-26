#ifndef SHM_PUBSUB_DETAIL_MPMC_QUEUE_HPP_
#define SHM_PUBSUB_DETAIL_MPMC_QUEUE_HPP_

#include <atomic>
#include <cstdint>

#include "shm_pubsub/detail/fault_injection.hpp"

namespace shm_pubsub {
namespace detail {

constexpr uint32_t kTombstone = 0xffffffffu;

// Futex generation plus the number of armed waiters. Publishers touch the
// generation only when waiters is nonzero.
struct alignas(64) WakeWord {
  std::atomic<uint32_t> generation;
  std::atomic<uint32_t> waiters;
  void init() {
    generation.store(0, std::memory_order_relaxed);
    waiters.store(0, std::memory_order_relaxed);
  }
};

// Vyukov's bounded MPMC queue with 64-bit positions. Dequeue is split into
// claim and release so that a consumer can record ownership of the claimed
// value before the cell becomes reusable; recovery relies on that order.
template <uint32_t kCapacity>
struct MpmcQueue {
  static_assert(kCapacity >= 2 && (kCapacity & (kCapacity - 1)) == 0,
                "queue capacity must be a power of two");
  static constexpr uint64_t kMask = kCapacity - 1;

  struct Cell {
    std::atomic<uint64_t> sequence;
    std::atomic<uint32_t> value;
    uint32_t padding;
  };

  alignas(64) std::atomic<uint64_t> enqueue_pos;
  alignas(64) std::atomic<uint64_t> dequeue_pos;
  alignas(64) Cell cells[kCapacity];
  WakeWord wake;

  void init() {
    enqueue_pos.store(0, std::memory_order_relaxed);
    dequeue_pos.store(0, std::memory_order_relaxed);
    for (uint64_t i = 0; i < kCapacity; ++i) {
      cells[i].value.store(kTombstone, std::memory_order_relaxed);
      cells[i].sequence.store(i, std::memory_order_relaxed);
    }
    wake.init();
    std::atomic_thread_fence(std::memory_order_release);
  }

  Cell& cell(uint64_t pos) { return cells[pos & kMask]; }
  const Cell& cell(uint64_t pos) const { return cells[pos & kMask]; }

  bool try_enqueue(uint32_t value) {
    uint64_t pos = enqueue_pos.load(std::memory_order_relaxed);
    Cell* target;
    for (;;) {
      target = &cell(pos);
      const uint64_t seq = target->sequence.load(std::memory_order_acquire);
      const int64_t diff = static_cast<int64_t>(seq - pos);
      if (diff == 0) {
        if (enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (diff < 0) {
        return false;
      } else {
        pos = enqueue_pos.load(std::memory_order_relaxed);
      }
    }
    SHM_PUBSUB_FAULT_POINT("queue.after_reserve");
    target->value.store(value, std::memory_order_relaxed);
    target->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  // On success the caller owns position pos and must call release(pos).
  bool try_claim(uint64_t& pos_out, uint32_t& value) {
    uint64_t pos = dequeue_pos.load(std::memory_order_relaxed);
    for (;;) {
      Cell& c = cell(pos);
      const uint64_t seq = c.sequence.load(std::memory_order_acquire);
      const int64_t diff = static_cast<int64_t>(seq - (pos + 1));
      if (diff == 0) {
        if (dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          value = c.value.load(std::memory_order_relaxed);
          pos_out = pos;
          return true;
        }
      } else if (diff < 0) {
        return false;
      } else {
        pos = dequeue_pos.load(std::memory_order_relaxed);
      }
    }
  }

  void release(uint64_t pos) {
    cell(pos).sequence.store(pos + kCapacity, std::memory_order_release);
  }

  bool try_dequeue(uint32_t& value) {
    uint64_t pos;
    if (!try_claim(pos, value)) return false;
    release(pos);
    return true;
  }

  bool head_ready() const {
    const uint64_t pos = dequeue_pos.load(std::memory_order_acquire);
    return cell(pos).sequence.load(std::memory_order_acquire) == pos + 1;
  }

  uint64_t size_approx() const {
    const uint64_t d = dequeue_pos.load(std::memory_order_acquire);
    const uint64_t e = enqueue_pos.load(std::memory_order_acquire);
    return e > d ? e - d : 0;
  }
};

template <uint32_t kCapacity>
constexpr uint64_t MpmcQueue<kCapacity>::kMask;

}  // namespace detail
}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_DETAIL_MPMC_QUEUE_HPP_
