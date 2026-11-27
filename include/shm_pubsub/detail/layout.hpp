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
constexpr uint32_t kLayoutVersion = 6;
constexpr uint32_t kReady = 2;

// Slot lifecycle word: state (bits 0-1) and generation (bits 32-63). Before
// touching subscriber queues a publisher announces them in its own `pinned`
// mask (one bit per subscriber slot) and then rechecks that each is ACTIVE;
// reclamation marks a slot RECLAIMING and then waits for live publishers'
// masks to drop its bit. Both sides fence in between (Dekker), so publishers
// never write to a word that other publishers write.
constexpr uint64_t kStateMask = 3;
constexpr uint64_t kSlotFree = 0;
constexpr uint64_t kSlotActive = 1;
constexpr uint64_t kSlotReclaiming = 2;

inline uint64_t lifecycle_state(uint64_t v) { return v & kStateMask; }
inline uint32_t lifecycle_generation(uint64_t v) { return static_cast<uint32_t>(v >> 32); }
inline uint64_t make_lifecycle(uint32_t generation, uint64_t state) {
  return (static_cast<uint64_t>(generation) << 32) | state;
}

// Block state word: one holder bit per endpoint slot (bits 0-55) plus the
// number of queue cells that reference the block (bits 56-63). A block sits
// in a given queue at most once, so 8 bits cover every subscriber and group.
// The block returns to the free list when the word drops to zero.
constexpr unsigned kMaxHolderBits = 56;
constexpr uint32_t kMaxQueueRefs = 255;
constexpr uint64_t kHolderMask = (1ULL << kMaxHolderBits) - 1;
constexpr uint64_t kQueueUnit = 1ULL << kMaxHolderBits;
inline uint64_t queue_refs(uint64_t state) { return state >> kMaxHolderBits; }

// Slot activity word, used both as a journal for diagnostics and as a seqlock
// for recovery: counter (bits 32-63), operation (bits 24-31), block (0-23).
enum Op : uint32_t {
  kOpIdle = 0,
  kOpLoan,
  kOpPublish,
  kOpTake,
  kOpRelease,
  kOpReclaim,
  kOpLeave,
  kOpRequeue,
};
constexpr uint32_t kNoBlock = 0xffffff;
constexpr uint32_t kNoEntry = 0xffffffffu;

// Queue cells and loan-table entries hold a block id in the low 24 bits and,
// in group queues, how often the message was delivered before in the top 8.
constexpr uint32_t kCellBlockMask = 0xffffff;
constexpr uint32_t kMaxDeliveryCount = 254;
inline uint32_t cell_block(uint32_t v) { return v & kCellBlockMask; }
inline uint32_t cell_attempt(uint32_t v) { return v >> 24; }
inline uint32_t make_cell(uint32_t id, uint32_t attempt) {
  return id | ((attempt < kMaxDeliveryCount ? attempt : kMaxDeliveryCount) << 24);
}
// Set on a publish that started dropping the oldest entries of a full
// queue: a death in that phase can leak a block that is not the one being
// published.
constexpr uint64_t kActivityDropping = 1ULL << 31;
inline uint32_t activity_op(uint64_t v) { return static_cast<uint32_t>(v >> 24) & 0x7f; }
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

// The first cache line is read by every publisher on every publish and
// written only on activation and reclamation. The second holds words the
// owner writes on every operation.
// Each slot lists the blocks it may hold a bit on. An id is recorded before
// the holder bit is set and forgotten after it is cleared, so the table is a
// superset of the slot's bits and reclamation never scans the whole pool.
template <uint32_t kQueueCapacity, uint32_t kLoanEntries>
struct alignas(64) Slot {
  std::atomic<uint64_t> lifecycle;
  std::atomic<uint32_t> pid;
  std::atomic<uint32_t> group;
  std::atomic<uint32_t> overflow;
  std::atomic<uint32_t> subscribed;
  alignas(64) std::atomic<uint64_t> activity;
  std::atomic<uint64_t> next_sequence;
  std::atomic<uint64_t> pinned;  // publisher slots: subscriber slots being delivered to
  // The dead owner's activity word, saved before reclamation overwrites it.
  std::atomic<uint64_t> last_activity;
  std::atomic<uint32_t> held[kLoanEntries];
  SlotStats stats;
  MpmcQueue<kQueueCapacity> queue;

  void init() {
    lifecycle.store(make_lifecycle(0, kSlotFree), std::memory_order_relaxed);
    activity.store(0, std::memory_order_relaxed);
    next_sequence.store(0, std::memory_order_relaxed);
    pinned.store(0, std::memory_order_relaxed);
    last_activity.store(0, std::memory_order_relaxed);
    for (uint32_t k = 0; k < kLoanEntries; ++k) held[k].store(kNoEntry, std::memory_order_relaxed);
    pid.store(0, std::memory_order_relaxed);
    group.store(0, std::memory_order_relaxed);
    overflow.store(0, std::memory_order_relaxed);
    subscribed.store(0, std::memory_order_relaxed);
    stats.init();
    queue.init();
  }
};

// Group policy word, fixed by the first member for the lifetime of the
// region: set flag (bit 31), overflow (bits 0-3), durable (bit 4), delivery
// (bits 5-6), max deliveries (bits 8-15, 0 = unlimited).
constexpr uint32_t kPolicySet = 1u << 31;
inline uint32_t policy_overflow(uint32_t p) { return p & 0xf; }
inline bool policy_durable(uint32_t p) { return (p >> 4) & 1; }
inline uint32_t policy_delivery(uint32_t p) { return (p >> 5) & 3; }
inline uint32_t policy_max_deliveries(uint32_t p) { return (p >> 8) & 0xff; }
inline uint32_t make_policy(uint32_t overflow, bool durable, uint32_t delivery,
                            uint32_t max_deliveries) {
  return kPolicySet | (overflow & 0xf) | (durable ? 1u << 4 : 0) | ((delivery & 3) << 5) |
         ((max_deliveries & 0xff) << 8);
}

// Membership is a bitmap (one bit per subscriber slot) so that joining and
// leaving are single idempotent steps that a crash cannot leave half done.
template <uint32_t kQueueCapacity>
struct alignas(64) GroupRecord {
  std::atomic<uint64_t> member_mask;
  std::atomic<uint32_t> policy;
  std::atomic<uint64_t> dropped_newest;
  std::atomic<uint64_t> dropped_oldest;
  std::atomic<uint64_t> redelivered;
  std::atomic<uint64_t> dead_lettered;
  WakeWord space;  // kBlock publishers waiting for a free cell
  MpmcQueue<kQueueCapacity> queue;

  void init() {
    member_mask.store(0, std::memory_order_relaxed);
    policy.store(0, std::memory_order_relaxed);
    dropped_newest.store(0, std::memory_order_relaxed);
    dropped_oldest.store(0, std::memory_order_relaxed);
    redelivered.store(0, std::memory_order_relaxed);
    dead_lettered.store(0, std::memory_order_relaxed);
    space.init();
    queue.init();
  }
};

template <class Config>
constexpr uint32_t loan_entries() {
  return Config::kMaxLoansPerPublisher > Config::kMaxLoansPerSubscriber
             ? Config::kMaxLoansPerPublisher
             : Config::kMaxLoansPerSubscriber;
}

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
  // Set when a reclamation could not rule out a leaked block; the next
  // collect_garbage() or exhausted loan runs recover() and clears it.
  std::atomic<uint32_t> repair_pending;
  alignas(64) LockFreeFreeList<uint32_t, Config::kBlockCount> free_list;
  Slot<Config::kQueueCapacity, loan_entries<Config>()>
      slots[Config::kMaxPublishers + Config::kMaxSubscribers];
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
  h = fnv_mix(h, Config::kTimestamps ? 1 : 0);
  h = fnv_mix(h, sizeof(Region<Config>));
  h = fnv_mix(h, sizeof(Slot<Config::kQueueCapacity, loan_entries<Config>()>));
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
