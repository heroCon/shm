#ifndef SHM_PUBSUB_DETAIL_LAYOUT_HPP_
#define SHM_PUBSUB_DETAIL_LAYOUT_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "shm_pubsub/detail/lock_free_list.hpp"
#include "shm_pubsub/detail/mpmc_queue.hpp"

namespace shm_pubsub {
namespace detail {

// Atomics live in shared memory, so a lock-based fallback would deadlock or
// be invisible across processes.
static_assert(ATOMIC_INT_LOCK_FREE == 2 && ATOMIC_LLONG_LOCK_FREE == 2,
              "shm_pubsub requires always-lock-free 32- and 64-bit atomics");

constexpr uint64_t kMagic = 0x53484d5055425355ULL;
constexpr uint32_t kLayoutVersion = 5;
constexpr uint32_t kReady = 2;

// Slot lifecycle word: state (bits 0-1), publisher pins (bits 8-31, one per
// publisher slot) and generation (bits 32-63). Publishers pin an ACTIVE
// subscriber slot before touching its queue; reclamation waits for live pins.
constexpr uint64_t kStateMask = 3;
constexpr uint64_t kSlotFree = 0;
constexpr uint64_t kSlotActive = 1;
constexpr uint64_t kSlotReclaiming = 2;
constexpr unsigned kPinShift = 8;
constexpr unsigned kMaxPinBits = 24;

inline uint64_t lifecycle_state(uint64_t v) { return v & kStateMask; }
inline uint32_t lifecycle_generation(uint64_t v) { return static_cast<uint32_t>(v >> 32); }
inline uint64_t lifecycle_pins(uint64_t v) {
  return (v >> kPinShift) & ((1ULL << kMaxPinBits) - 1);
}
inline uint64_t make_lifecycle(uint32_t generation, uint64_t state) {
  return (static_cast<uint64_t>(generation) << 32) | state;
}

// Block state word: one holder bit per endpoint slot (bits 0-47) plus the
// number of queue cells that reference the block (bits 48-63). The block
// returns to the free list when the word drops to zero.
constexpr unsigned kMaxHolderBits = 48;
constexpr uint64_t kHolderMask = (1ULL << kMaxHolderBits) - 1;
constexpr uint64_t kQueueUnit = 1ULL << kMaxHolderBits;
inline uint64_t queue_refs(uint64_t state) { return state >> kMaxHolderBits; }

// Slot activity word, used both as a journal for diagnostics and as a seqlock
// for recovery: counter (bits 32-63), operation (bits 24-31), block (0-23).
enum Op : uint32_t { kOpIdle = 0, kOpLoan, kOpPublish, kOpTake, kOpRelease, kOpReclaim };
constexpr uint32_t kNoBlock = 0xffffff;
inline uint32_t activity_op(uint64_t v) { return static_cast<uint32_t>(v >> 24) & 0xff; }
inline uint32_t activity_block(uint64_t v) { return static_cast<uint32_t>(v) & 0xffffff; }

// Byte offsets of OFD locks: one byte per slot plus a maintenance byte.
constexpr uint32_t kMaintenanceByte = 4095;

struct alignas(64) BlockHeader {
  std::atomic<uint64_t> state;
  uint32_t size;
  uint32_t publisher;
  uint64_t sequence;
  uint64_t timestamp_ns;
};
static_assert(sizeof(BlockHeader) == 64, "unsupported block header ABI");

struct alignas(64) SlotStats {
  std::atomic<uint64_t> published;
  std::atomic<uint64_t> taken;
  std::atomic<uint64_t> dropped_newest;
  std::atomic<uint64_t> dropped_oldest;
  std::atomic<uint32_t> held_loans;
  void init() {
    published.store(0, std::memory_order_relaxed);
    taken.store(0, std::memory_order_relaxed);
    dropped_newest.store(0, std::memory_order_relaxed);
    dropped_oldest.store(0, std::memory_order_relaxed);
    held_loans.store(0, std::memory_order_relaxed);
  }
};

template <uint32_t kQueueCapacity>
struct alignas(64) Slot {
  std::atomic<uint64_t> lifecycle;
  std::atomic<uint64_t> activity;
  std::atomic<uint64_t> next_sequence;
  std::atomic<uint32_t> pid;
  std::atomic<uint32_t> group;
  std::atomic<uint32_t> overflow;
  std::atomic<uint32_t> subscribed;
  std::atomic<uint32_t> member;
  SlotStats stats;
  MpmcQueue<kQueueCapacity> queue;

  void init() {
    lifecycle.store(make_lifecycle(0, kSlotFree), std::memory_order_relaxed);
    activity.store(0, std::memory_order_relaxed);
    next_sequence.store(0, std::memory_order_relaxed);
    pid.store(0, std::memory_order_relaxed);
    group.store(0, std::memory_order_relaxed);
    overflow.store(0, std::memory_order_relaxed);
    subscribed.store(0, std::memory_order_relaxed);
    member.store(0, std::memory_order_relaxed);
    stats.init();
    queue.init();
  }
};

template <uint32_t kQueueCapacity>
struct alignas(64) GroupRecord {
  std::atomic<uint32_t> members;
  std::atomic<uint32_t> overflow;
  std::atomic<uint64_t> dropped_newest;
  std::atomic<uint64_t> dropped_oldest;
  MpmcQueue<kQueueCapacity> queue;

  void init() {
    members.store(0, std::memory_order_relaxed);
    overflow.store(0, std::memory_order_relaxed);
    dropped_newest.store(0, std::memory_order_relaxed);
    dropped_oldest.store(0, std::memory_order_relaxed);
    queue.init();
  }
};

// Region header followed by the slot table and group queues. Data blocks
// follow at blocks_offset(). Group index 0 is unused so that group ids map
// directly to array indices.
template <class Config>
struct Region {
  alignas(64) std::atomic<uint32_t> init_state;
  uint32_t version;
  uint64_t magic;
  uint64_t config_hash;
  uint64_t region_size;
  uint32_t block_count;
  uint32_t block_size;
  uint32_t max_publishers;
  uint32_t max_subscribers;
  uint32_t max_groups;
  uint32_t queue_capacity;
  alignas(64) LockFreeFreeList<uint32_t, Config::kBlockCount> free_list;
  Slot<Config::kQueueCapacity> slots[Config::kMaxPublishers + Config::kMaxSubscribers];
  GroupRecord<Config::kQueueCapacity> groups[Config::kMaxGroups + 1];
};

inline uint64_t fnv_mix(uint64_t hash, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    hash ^= (value >> (8 * i)) & 0xff;
    hash *= 1099511628211ULL;
  }
  return hash;
}

template <class Config>
uint64_t config_hash() {
  uint64_t h = 1469598103934665603ULL;
  h = fnv_mix(h, kLayoutVersion);
  h = fnv_mix(h, Config::kBlockCount);
  h = fnv_mix(h, Config::kBlockSize);
  h = fnv_mix(h, Config::kQueueCapacity);
  h = fnv_mix(h, Config::kMaxPublishers);
  h = fnv_mix(h, Config::kMaxSubscribers);
  h = fnv_mix(h, Config::kMaxGroups);
  h = fnv_mix(h, Config::kMaxLoansPerPublisher);
  h = fnv_mix(h, Config::kMaxLoansPerSubscriber);
  h = fnv_mix(h, sizeof(Region<Config>));
  h = fnv_mix(h, sizeof(Slot<Config::kQueueCapacity>));
  h = fnv_mix(h, alignof(Region<Config>));
  h = fnv_mix(h, sizeof(void*));
  return h;
}

template <class Config>
constexpr size_t blocks_offset() {
  return (sizeof(Region<Config>) + 4095) / 4096 * 4096;
}

template <class Config>
constexpr size_t region_size() {
  return blocks_offset<Config>() + static_cast<size_t>(Config::kBlockCount) * Config::kBlockSize;
}

}  // namespace detail
}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_DETAIL_LAYOUT_HPP_
