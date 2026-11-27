#ifndef SHM_PUBSUB_SHM_PUBSUB_HPP_
#define SHM_PUBSUB_SHM_PUBSUB_HPP_

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "shm_pubsub/detail/fault_injection.hpp"
#include "shm_pubsub/detail/layout.hpp"
#include "shm_pubsub/detail/platform.hpp"
#include "shm_pubsub/status.hpp"
#include "shm_pubsub/wait_set.hpp"

#define SHM_PUBSUB_VERSION_MAJOR 0
#define SHM_PUBSUB_VERSION_MINOR 4
#define SHM_PUBSUB_VERSION_PATCH 0

namespace shm_pubsub {

// Compile-time capacities. Derive from DefaultConfig and hide the members to
// change them; every process attached to a region must use the same values,
// which the region header verifies through a hash.
struct DefaultConfig {
  static constexpr uint32_t kBlockCount = 256;
  static constexpr uint32_t kBlockSize = 4096;  // includes a 64-byte header
  static constexpr uint32_t kQueueCapacity = 8;
  static constexpr uint32_t kMaxPublishers = 8;
  static constexpr uint32_t kMaxSubscribers = 16;
  static constexpr uint32_t kMaxGroups = 4;
  static constexpr uint32_t kMaxLoansPerPublisher = 2;
  static constexpr uint32_t kMaxLoansPerSubscriber = 2;
  static constexpr bool kTimestamps = true;  // stamp CLOCK_MONOTONIC on every publish
};

struct Options {
  std::string name = "/shm_pubsub";
  mode_t mode = 0660;
  bool unlink_on_destroy = false;
  std::chrono::milliseconds initialization_timeout{5000};
};

struct SubscriberOptions {
  uint32_t group = 0;  // 0: private queue (broadcast); 1..kMaxGroups: competing group
  Overflow overflow = Overflow::kDropNewest;
};

struct RecoveryReport {
  uint32_t slots_reclaimed = 0;
  uint32_t orphans_freed = 0;
  uint32_t states_repaired = 0;
  uint32_t holes_filled = 0;
  uint32_t cells_released = 0;
  uint32_t pins_cleared = 0;
  uint32_t members_cleared = 0;
  uint32_t inconsistencies = 0;
};

template <class Config = DefaultConfig>
class Topic {
 public:
  static constexpr uint32_t kBlockCount = Config::kBlockCount;
  static constexpr uint32_t kBlockSize = Config::kBlockSize;
  static constexpr uint32_t kQueueCapacity = Config::kQueueCapacity;
  static constexpr uint32_t kMaxPublishers = Config::kMaxPublishers;
  static constexpr uint32_t kMaxSubscribers = Config::kMaxSubscribers;
  static constexpr uint32_t kMaxGroups = Config::kMaxGroups;
  static constexpr uint32_t kMaxLoansPerPublisher = Config::kMaxLoansPerPublisher;
  static constexpr uint32_t kMaxLoansPerSubscriber = Config::kMaxLoansPerSubscriber;
  static constexpr uint32_t kLoanEntries = detail::loan_entries<Config>();
  static constexpr uint32_t kSlots = kMaxPublishers + kMaxSubscribers;
  static constexpr size_t kMaxPayload = kBlockSize - sizeof(detail::BlockHeader);
  // Every reference a block can have is bounded, so a pool of this size can
  // never be exhausted by slow or stalled subscribers.
  static constexpr uint64_t kRequiredBlocks =
      static_cast<uint64_t>(kMaxSubscribers + kMaxGroups) * kQueueCapacity +
      static_cast<uint64_t>(kMaxSubscribers) * kMaxLoansPerSubscriber +
      static_cast<uint64_t>(kMaxPublishers) * kMaxLoansPerPublisher;

  static_assert(kBlockSize % 64 == 0 && kBlockSize > sizeof(detail::BlockHeader),
                "kBlockSize must be a multiple of 64 larger than the block header");
  static_assert(kBlockCount >= 1 && kBlockCount < detail::kNoBlock, "kBlockCount out of range");
  static_assert(kMaxPublishers >= 1, "kMaxPublishers must be at least 1");
  static_assert(kMaxSubscribers >= 1, "kMaxSubscribers must be at least 1");
  static_assert(kSlots <= detail::kMaxHolderBits, "at most 56 endpoints per topic");
  static_assert(kMaxSubscribers + kMaxGroups <= detail::kMaxQueueRefs,
                "too many queues for the 8-bit queue reference count");
  static_assert(kSlots < detail::kMaintenanceByte, "lock bytes overlap");
  static_assert(kMaxGroups <= 64, "kMaxGroups must be at most 64");
  static_assert(kQueueCapacity < kBlockCount, "queue capacity must be smaller than the pool");
  static_assert(kMaxLoansPerPublisher >= 1 && kMaxLoansPerSubscriber >= 1,
                "endpoints need at least one loan");
  static_assert(kBlockCount >= kRequiredBlocks,
                "kBlockCount must cover (subscribers + groups) * queue capacity plus all loans");

  class Publisher;
  class Subscriber;

  class WriteLoan {
   public:
    WriteLoan() {}
    ~WriteLoan() { reset(); }
    WriteLoan(WriteLoan&& other) noexcept : owner_(other.owner_), id_(other.id_) {
      other.owner_ = nullptr;
    }
    WriteLoan& operator=(WriteLoan&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = other.owner_;
        id_ = other.id_;
        other.owner_ = nullptr;
      }
      return *this;
    }
    WriteLoan(const WriteLoan&) = delete;
    WriteLoan& operator=(const WriteLoan&) = delete;

    explicit operator bool() const { return owner_ != nullptr; }
    char* data() { return owner_ ? owner_->topic_->payload(id_) : nullptr; }
    size_t size() const { return owner_ ? owner_->topic_->header(id_).size : 0; }
    static constexpr size_t capacity() { return kMaxPayload; }
    bool resize(size_t size) {
      if (!owner_ || size == 0 || size > kMaxPayload) return false;
      owner_->topic_->header(id_).size = static_cast<uint32_t>(size);
      return true;
    }
    void reset() {
      if (owner_) owner_->release_loan(id_);
      owner_ = nullptr;
    }

   private:
    friend class Topic;
    Publisher* owner_ = nullptr;
    uint32_t id_ = 0;
  };

  class ReadLoan {
   public:
    ReadLoan() {}
    ~ReadLoan() { reset(); }
    ReadLoan(ReadLoan&& other) noexcept : owner_(other.owner_), id_(other.id_) {
      other.owner_ = nullptr;
    }
    ReadLoan& operator=(ReadLoan&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = other.owner_;
        id_ = other.id_;
        other.owner_ = nullptr;
      }
      return *this;
    }
    ReadLoan(const ReadLoan&) = delete;
    ReadLoan& operator=(const ReadLoan&) = delete;

    explicit operator bool() const { return owner_ != nullptr; }
    const char* data() const { return owner_ ? owner_->topic_->payload(id_) : nullptr; }
    size_t size() const { return owner_ ? owner_->topic_->header(id_).size : 0; }
    // Per-publisher sequence number; a gap means this reader missed messages.
    uint64_t sequence() const { return owner_ ? owner_->topic_->header(id_).sequence : 0; }
    // CLOCK_MONOTONIC time at publication.
    uint64_t timestamp_ns() const { return owner_ ? owner_->topic_->header(id_).timestamp_ns : 0; }
    uint32_t publisher_id() const { return owner_ ? owner_->topic_->header(id_).publisher : 0; }
    void reset() {
      if (owner_) owner_->release_loan(id_);
      owner_ = nullptr;
    }

   private:
    friend class Topic;
    Subscriber* owner_ = nullptr;
    uint32_t id_ = 0;
  };

  // A publisher endpoint. Not thread-safe: use one per thread.
  class Publisher {
   public:
    explicit Publisher(Topic& topic) : topic_(&topic) {
      fd_ = topic.open_endpoint_fd();
      try {
        slot_ = topic.acquire_slot(fd_, 0, kMaxPublishers);
      } catch (...) {
        close(fd_);
        throw;
      }
      auto& s = topic.slot(slot_);
      s.pid.store(static_cast<uint32_t>(getpid()), std::memory_order_relaxed);
      s.stats.init();
      counter_ = topic.activate(slot_);
    }
    ~Publisher() {
      topic_->reclaim_slot(slot_, nullptr);
      close(fd_);
    }
    Publisher(const Publisher&) = delete;
    Publisher& operator=(const Publisher&) = delete;

    Status loan(size_t size, WriteLoan& out) {
      if (out || size == 0) return Status::kInvalidArgument;
      if (size > kMaxPayload) return Status::kMessageTooLarge;
      if (held_ >= kMaxLoansPerPublisher) return Status::kLoanLimit;
      uint32_t id;
      if (!try_loan(size, id)) {
        topic_->collect_garbage();
        if (!try_loan(size, id)) {
          topic_->recover(nullptr);
          if (!try_loan(size, id)) return Status::kPoolExhausted;
        }
      }
      out.owner_ = this;
      out.id_ = id;
      return Status::kOk;
    }

    // Consumes the loan whenever it belongs to this publisher.
    PublishResult publish(WriteLoan&& sample) {
      if (sample.owner_ != this) {
        PublishResult r = {Status::kInvalidArgument, 0, 0, 0};
        return r;
      }
      const uint32_t id = sample.id_;
      sample.owner_ = nullptr;
      return publish_ids(&id, 1);
    }

    // Publishes up to kMaxLoansPerPublisher loans in order with one pin pass,
    // one fence and at most one wake-up per queue. Counts in the result are
    // summed over the messages. Nothing is consumed unless every loan
    // belongs to this publisher.
    PublishResult publish(WriteLoan* samples, size_t count) {
      PublishResult r = {Status::kInvalidArgument, 0, 0, 0};
      if (!samples || count == 0 || count > kMaxLoansPerPublisher) return r;
      uint32_t ids[kMaxLoansPerPublisher];
      for (size_t k = 0; k < count; ++k) {
        if (samples[k].owner_ != this) return r;
        for (size_t m = 0; m < k; ++m) {
          if (samples[m].id_ == samples[k].id_) return r;
        }
        ids[k] = samples[k].id_;
      }
      for (size_t k = 0; k < count; ++k) samples[k].owner_ = nullptr;
      return publish_ids(ids, count);
    }
    PublishResult publish_copy(const void* data, size_t size) {
      PublishResult r = {Status::kInvalidArgument, 0, 0, 0};
      if (!data || size == 0) return r;
      if (size > kMaxPayload) {
        r.status = Status::kMessageTooLarge;
        return r;
      }
      if (!topic_->has_targets()) {
        r.status = Status::kNoSubscribers;
        return r;
      }
      WriteLoan sample;
      const Status status = loan(size, sample);
      if (status != Status::kOk) {
        r.status = status;
        return r;
      }
      std::memcpy(sample.data(), data, size);
      return publish(std::move(sample));
    }

    uint32_t slot() const { return slot_; }

   private:
    friend class Topic;

    void begin(uint32_t op, uint32_t id) { topic_->begin_activity(slot_, counter_, op, id); }
    void end() { topic_->end_activity(slot_, counter_); }

    PublishResult publish_ids(const uint32_t* ids, size_t count) {
      PublishResult r = {Status::kOk, 0, 0, 0};
      auto& slot = topic_->slot(slot_);
      uint64_t seq = slot.next_sequence.load(std::memory_order_relaxed);
      const uint64_t now = Config::kTimestamps ? detail::monotonic_ns() : 0;
      for (size_t k = 0; k < count; ++k) {
        detail::BlockHeader& h = topic_->header(ids[k]);
        h.sequence = seq++;
        h.timestamp_ns = now;
        h.publisher = slot_;
      }
      slot.next_sequence.store(seq, std::memory_order_relaxed);

      uint64_t targets = 0;
      for (uint32_t j = kMaxPublishers; j < kSlots; ++j) {
        const auto& s = topic_->slot(j);
        if (detail::lifecycle_state(s.lifecycle.load(std::memory_order_relaxed)) ==
                detail::kSlotActive &&
            s.subscribed.load(std::memory_order_relaxed) &&
            s.group.load(std::memory_order_relaxed) == 0) {
          targets |= member_bit(j);
        }
      }
      if (targets) {
        slot.pinned.store(targets, std::memory_order_relaxed);
        // Pairs with the fence in reclaim_slot(): either this publisher sees
        // the slot leave ACTIVE, or the reclaimer sees its pin and waits.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        for (uint64_t pending = targets; pending; pending &= pending - 1) {
          const uint32_t j = kMaxPublishers + static_cast<uint32_t>(__builtin_ctzll(pending));
          const auto& s = topic_->slot(j);
          if (detail::lifecycle_state(s.lifecycle.load(std::memory_order_acquire)) !=
                  detail::kSlotActive ||
              !s.subscribed.load(std::memory_order_acquire) ||
              s.group.load(std::memory_order_relaxed) != 0) {
            targets &= ~member_bit(j);
          }
        }
      }
      uint64_t woken = 0;
      uint32_t group_wakes[kMaxGroups + 1] = {};
      for (size_t k = 0; k < count; ++k) {
        const uint32_t id = ids[k];
        begin(detail::kOpPublish, id);
        for (uint64_t pending = targets; pending; pending &= pending - 1) {
          const uint32_t j = kMaxPublishers + static_cast<uint32_t>(__builtin_ctzll(pending));
          auto& s = topic_->slot(j);
          ++r.targets;
          const Overflow overflow =
              static_cast<Overflow>(s.overflow.load(std::memory_order_relaxed));
          if (deliver(s.queue, id, overflow, s.stats.dropped_newest, s.stats.dropped_oldest, r)) {
            ++r.delivered;
            woken |= member_bit(j);
          }
        }
        for (uint32_t g = 1; g <= kMaxGroups; ++g) {
          auto& group = topic_->group(g);
          if (group.member_mask.load(std::memory_order_acquire) == 0) continue;
          ++r.targets;
          const Overflow overflow = static_cast<Overflow>(
              detail::policy_overflow(group.policy.load(std::memory_order_relaxed)));
          if (deliver(group.queue, id, overflow, group.dropped_newest, group.dropped_oldest, r)) {
            ++r.delivered;
            ++group_wakes[g];
          }
        }
        SHM_PUBSUB_FAULT_POINT("publish.before_release");
        drop_own(id);
        end();
      }
      if (targets) slot.pinned.store(0, std::memory_order_release);
      held_ -= static_cast<uint32_t>(count);
      slot.stats.held_loans.store(held_, std::memory_order_relaxed);
      // Pairs with the fence in wait_take(): either the waiter sees the
      // queued cell, or this load sees the waiter and wakes it.
      std::atomic_thread_fence(std::memory_order_seq_cst);
      for (uint64_t pending = woken; pending; pending &= pending - 1) {
        const uint32_t j = kMaxPublishers + static_cast<uint32_t>(__builtin_ctzll(pending));
        wake(topic_->slot(j).queue.wake, INT_MAX);
      }
      for (uint32_t g = 1; g <= kMaxGroups; ++g) {
        if (group_wakes[g]) wake(topic_->group(g).queue.wake, static_cast<int>(group_wakes[g]));
      }
      slot.stats.published.store(slot.stats.published.load(std::memory_order_relaxed) + count,
                                 std::memory_order_relaxed);
      if (r.targets == 0) {
        r.status = Status::kNoSubscribers;
      } else if (r.delivered == 0) {
        r.status = Status::kQueueFull;
      } else if (r.delivered < r.targets) {
        r.status = Status::kPartial;
      }
      return r;
    }

    static void wake(detail::WakeWord& w, int count) {
      if (w.waiters.load(std::memory_order_relaxed) == 0) return;
      w.generation.fetch_add(1, std::memory_order_release);
      detail::futex_wake(&w.generation, count);
    }

    bool try_loan(size_t size, uint32_t& id) {
      begin(detail::kOpLoan, detail::kNoBlock);
      if (!topic_->region_->free_list.pop(id)) {
        end();
        return false;
      }
      SHM_PUBSUB_FAULT_POINT("loan.after_pop");
      topic_->record_loan(slot_, id);
      detail::BlockHeader& h = topic_->header(id);
      h.state.store(1ULL << slot_, std::memory_order_release);
      h.size = static_cast<uint32_t>(size);
      h.publisher = slot_;
      SHM_PUBSUB_FAULT_POINT("loan.after_claim");
      end();
      ++held_;
      topic_->slot(slot_).stats.held_loans.store(held_, std::memory_order_relaxed);
      return true;
    }

    template <class Queue>
    bool deliver(Queue& queue, uint32_t id, Overflow overflow,
                 std::atomic<uint64_t>& dropped_newest, std::atomic<uint64_t>& dropped_oldest,
                 PublishResult& r) {
      detail::BlockHeader& h = topic_->header(id);
      h.state.fetch_add(detail::kQueueUnit, std::memory_order_relaxed);
      SHM_PUBSUB_FAULT_POINT("publish.after_ref");
      for (int attempt = 0;; ++attempt) {
        if (queue.try_enqueue(id)) {
          SHM_PUBSUB_FAULT_POINT("publish.after_enqueue");
          return true;
        }
        if (overflow != Overflow::kDropOldest || attempt == 2) break;
        uint64_t pos;
        uint32_t old;
        if (queue.try_claim(pos, old)) {
          SHM_PUBSUB_FAULT_POINT("drop_oldest.after_claim");
          // Release the cell before the reference: reclamation of a dead
          // subscriber treats an unreleased claimed cell as still referenced.
          queue.release(pos);
          SHM_PUBSUB_FAULT_POINT("drop_oldest.after_release_cell");
          if (old < kBlockCount) topic_->release_bits(old, detail::kQueueUnit);
          dropped_oldest.fetch_add(1, std::memory_order_relaxed);
          ++r.dropped_oldest;
        }
      }
      h.state.fetch_sub(detail::kQueueUnit, std::memory_order_relaxed);
      dropped_newest.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    void release_loan(uint32_t id) {
      begin(detail::kOpRelease, id);
      drop_own(id);
      end();
      --held_;
      topic_->slot(slot_).stats.held_loans.store(held_, std::memory_order_relaxed);
    }

    void drop_own(uint32_t id) {
      topic_->release_bits(id, 1ULL << slot_);
      topic_->forget_loan(slot_, id);
    }

    Topic* topic_;
    int fd_ = -1;
    uint32_t slot_ = 0;
    uint32_t held_ = 0;
    uint64_t counter_ = 0;
  };

  // A subscriber endpoint. Not thread-safe except for stop(), which any
  // thread may call to cancel blocking calls.
  class Subscriber {
   public:
    explicit Subscriber(Topic& topic, const SubscriberOptions& options = SubscriberOptions())
        : topic_(&topic), group_(options.group) {
      if (options.group > kMaxGroups) throw std::invalid_argument("subscriber group out of range");
      if (group_) {
        topic.claim_group_policy(
            group_, detail::make_policy(static_cast<uint32_t>(options.overflow), false, 0, 0));
      }
      fd_ = topic.open_endpoint_fd();
      try {
        slot_ = topic.acquire_slot(fd_, kMaxPublishers, kSlots);
      } catch (...) {
        close(fd_);
        throw;
      }
      auto& s = topic.slot(slot_);
      s.pid.store(static_cast<uint32_t>(getpid()), std::memory_order_relaxed);
      s.group.store(group_, std::memory_order_relaxed);
      s.overflow.store(static_cast<uint32_t>(options.overflow), std::memory_order_relaxed);
      s.stats.init();
      counter_ = topic.activate(slot_);
      subscribe();
    }
    ~Subscriber() {
      stop();
      unsubscribe();
      topic_->reclaim_slot(slot_, nullptr);
      close(fd_);
    }
    Subscriber(const Subscriber&) = delete;
    Subscriber& operator=(const Subscriber&) = delete;

    void subscribe() {
      if (group_) topic_->join_group(group_, slot_);
      topic_->slot(slot_).subscribed.store(1, std::memory_order_release);
      subscribed_ = true;
    }
    // Leaving a non-durable group as its last member discards the backlog.
    void unsubscribe() {
      topic_->slot(slot_).subscribed.store(0, std::memory_order_release);
      if (group_) {
        begin(detail::kOpLeave, detail::kNoBlock);
        topic_->leave_group(group_, slot_);
        end();
      }
      subscribed_ = false;
    }

    // Cancels current and future blocking calls on this endpoint.
    void stop() {
      if (!running_.exchange(false)) return;
      detail::WakeWord& w = source().wake;
      w.generation.fetch_add(1, std::memory_order_release);
      detail::futex_wake(&w.generation, INT_MAX);
    }

    Status take(ReadLoan& out) {
      if (out) return Status::kInvalidArgument;
      if (!subscribed_) return Status::kNotSubscribed;
      if (held_ >= kMaxLoansPerSubscriber) return Status::kLoanLimit;
      auto& queue = source();
      // An empty poll writes nothing, so idle busy-polling readers do not
      // keep recover() from observing a quiescent snapshot.
      if (!queue.head_ready()) return Status::kWouldBlock;
      begin(detail::kOpTake, detail::kNoBlock);
      for (;;) {
        uint64_t pos;
        uint32_t id;
        if (!queue.try_claim(pos, id)) {
          end();
          return Status::kWouldBlock;
        }
        SHM_PUBSUB_FAULT_POINT("take.after_claim");
        if (id == detail::kTombstone) {
          queue.release(pos);
          continue;
        }
        if (id >= kBlockCount) {
          queue.release(pos);
          end();
          return Status::kCorrupted;
        }
        // One atomic step turns the queue reference into this slot's holder
        // bit, so a crash leaves exactly one of the two.
        topic_->record_loan(slot_, id);
        topic_->header(id).state.fetch_add((1ULL << slot_) - detail::kQueueUnit,
                                           std::memory_order_acq_rel);
        SHM_PUBSUB_FAULT_POINT("take.after_convert");
        queue.release(pos);
        end();
        ++held_;
        auto& stats = topic_->slot(slot_).stats;
        stats.held_loans.store(held_, std::memory_order_relaxed);
        stats.taken.store(stats.taken.load(std::memory_order_relaxed) + 1,
                          std::memory_order_relaxed);
        if (topic_->header(id).size == 0 || topic_->header(id).size > kMaxPayload) {
          release_loan(id);
          return Status::kCorrupted;
        }
        out.owner_ = this;
        out.id_ = id;
        return Status::kOk;
      }
    }

    // Takes up to `count` messages (bounded by the loan limit) into empty
    // loans and returns how many were taken.
    size_t take(ReadLoan* out, size_t count) {
      size_t n = 0;
      while (n < count && take(out[n]) == Status::kOk) ++n;
      return n;
    }

    // Takes up to `max` messages one at a time, calls fn(const ReadLoan&)
    // for each and releases it before the next. Returns how many were taken.
    template <class F>
    size_t drain(F&& fn, size_t max = static_cast<size_t>(-1)) {
      size_t n = 0;
      while (n < max) {
        ReadLoan sample;
        if (take(sample) != Status::kOk) break;
        fn(static_cast<const ReadLoan&>(sample));
        ++n;
      }
      return n;
    }

    Status wait_take(ReadLoan& out, std::chrono::milliseconds timeout) {
      if (timeout.count() < 0 || out) return Status::kInvalidArgument;
      const uint64_t deadline =
          detail::monotonic_ns() + static_cast<uint64_t>(timeout.count()) * 1000000ULL;
      detail::WakeWord& w = source().wake;
      for (;;) {
        if (!running_.load(std::memory_order_acquire)) return stopped();
        Status status = take(out);
        if (status != Status::kWouldBlock) return status;
        const uint64_t now = detail::monotonic_ns();
        if (now >= deadline) return Status::kTimedOut;
        w.waiters.fetch_add(1, std::memory_order_seq_cst);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        const uint32_t generation = w.generation.load(std::memory_order_acquire);
        if (!running_.load(std::memory_order_acquire)) {
          w.waiters.fetch_sub(1, std::memory_order_release);
          return stopped();
        }
        status = take(out);
        if (status != Status::kWouldBlock) {
          w.waiters.fetch_sub(1, std::memory_order_release);
          return status;
        }
        const timespec relative = detail::to_timespec(deadline - now);
        const long rc = detail::futex_wait(&w.generation, generation, &relative);
        const int error = errno;
        w.waiters.fetch_sub(1, std::memory_order_release);
        if (rc == -1 && error != EAGAIN && error != EINTR && error != ETIMEDOUT) {
          return Status::kNotSupported;
        }
      }
    }

    ReceiveResult receive(void* buffer, size_t size) {
      ReadLoan sample;
      return copy_out(take(sample), sample, buffer, size);
    }
    ReceiveResult wait_receive(void* buffer, size_t size, std::chrono::milliseconds timeout) {
      ReadLoan sample;
      return copy_out(wait_take(sample, timeout), sample, buffer, size);
    }

    uint32_t slot() const { return slot_; }
    uint32_t group() const { return group_; }
    bool subscribed() const { return subscribed_; }

    detail::WaitHandle wait_handle() {
      auto& queue = source();
      detail::WaitHandle h = {&queue.wake.generation, &queue.wake.waiters, &queue,
                              &Subscriber::queue_ready, &running_};
      return h;
    }

   private:
    friend class Topic;
    using Queue = detail::MpmcQueue<kQueueCapacity>;

    Queue& source() { return group_ ? topic_->group(group_).queue : topic_->slot(slot_).queue; }
    static bool queue_ready(const void* queue) {
      return static_cast<const Queue*>(queue)->head_ready();
    }

    // A stopping group member may have absorbed a wake-one meant for the
    // group; pass it on so that queued work is not left behind.
    Status stopped() {
      if (group_) {
        detail::WakeWord& w = source().wake;
        if (source().size_approx() > 0 && w.waiters.load(std::memory_order_acquire) > 0) {
          w.generation.fetch_add(1, std::memory_order_release);
          detail::futex_wake(&w.generation, 1);
        }
      }
      return Status::kStopped;
    }

    static ReceiveResult copy_out(Status status, const ReadLoan& sample, void* buffer,
                                  size_t size) {
      ReceiveResult r = {status, 0};
      if (status != Status::kOk) return r;
      r.size = sample.size();
      if (!buffer || size < r.size) {
        r.status = Status::kBufferTooSmall;
        return r;
      }
      std::memcpy(buffer, sample.data(), r.size);
      return r;
    }

    void begin(uint32_t op, uint32_t id) { topic_->begin_activity(slot_, counter_, op, id); }
    void end() { topic_->end_activity(slot_, counter_); }

    void release_loan(uint32_t id) {
      begin(detail::kOpRelease, id);
      topic_->release_bits(id, 1ULL << slot_);
      topic_->forget_loan(slot_, id);
      end();
      --held_;
      topic_->slot(slot_).stats.held_loans.store(held_, std::memory_order_relaxed);
    }

    Topic* topic_;
    uint32_t group_;
    int fd_ = -1;
    uint32_t slot_ = 0;
    uint32_t held_ = 0;
    uint64_t counter_ = 0;
    bool subscribed_ = false;
    std::atomic<bool> running_{true};
  };

  struct SlotInfo {
    uint32_t index;
    bool publisher;
    bool live;
    uint32_t state;
    uint32_t generation;
    uint32_t pid;
    uint32_t group;
    bool subscribed;
    uint64_t pins;
    uint64_t queue_depth;
    uint32_t held_loans;
    uint32_t op;
    uint32_t op_block;
    uint64_t published;
    uint64_t taken;
    uint64_t dropped_newest;
    uint64_t dropped_oldest;
  };
  struct GroupInfo {
    uint32_t id;
    uint32_t members;
    uint64_t member_mask;
    bool policy_set;
    Overflow overflow;
    uint64_t queue_depth;
    uint64_t dropped_newest;
    uint64_t dropped_oldest;
  };
  struct Snapshot {
    uint32_t layout_version = 0;
    uint64_t config_hash = 0;
    uint32_t block_count = 0;
    uint32_t block_size = 0;
    uint32_t queue_capacity = 0;
    uint32_t free_blocks = 0;
    bool free_list_consistent = true;
    std::vector<SlotInfo> slots;
    std::vector<GroupInfo> groups;
  };

  explicit Topic(const Options& options = Options()) : options_(options) { open_region(); }
  explicit Topic(const std::string& name) {
    options_.name = name;
    open_region();
  }
  // Endpoints and their loans must be destroyed first.
  ~Topic() {
    close_region();
    if (options_.unlink_on_destroy) shm_unlink(options_.name.c_str());
  }
  Topic(const Topic&) = delete;
  Topic& operator=(const Topic&) = delete;

  static bool destroy(const std::string& name) { return shm_unlink(name.c_str()) == 0; }
  static constexpr size_t max_payload_size() { return kMaxPayload; }
  const Options& options() const { return options_; }

  // Reclaims slots whose owners exited: their loans, queued references,
  // group memberships and publisher pins. Returns the number reclaimed. When
  // it reclaims any, it also attempts recover() once, because a dead owner
  // may have been mid-operation.
  size_t collect_garbage() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t reclaimed = 0;
    for (uint32_t i = 0; i < kSlots; ++i) {
      if (detail::lifecycle_state(slot(i).lifecycle.load(std::memory_order_acquire)) ==
          detail::kSlotFree) {
        continue;
      }
      if (detail::ofd_lock(fd_, i, F_WRLCK) != 0) continue;
      if (reclaim_slot(i, nullptr)) ++reclaimed;
      detail::ofd_lock(fd_, i, F_UNLCK);
    }
    if (reclaimed) recover_locked(nullptr);
    return reclaimed;
  }

  // Full consistency check and repair. Reclaims dead slots, then rebuilds
  // every block's reference word from the queues and the live holders, frees
  // orphaned blocks, fills queue holes left by dead producers and releases
  // cells claimed by dead consumers. The repair needs a window in which no
  // live endpoint is mid-operation; returns kBusy when it sees none.
  Status recover(RecoveryReport* report = nullptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    return recover_locked(report);
  }

  Snapshot snapshot() const { return make_snapshot(*region_, blocks_, fd_); }

  // Reads a region through a read-only mapping without registering.
  static Status inspect(const std::string& name, Snapshot& out) {
    const int fd = shm_open(name.c_str(), O_RDONLY, 0);
    if (fd < 0) return Status::kInvalidArgument;
    struct stat st {};
    if (fstat(fd, &st) != 0 || static_cast<size_t>(st.st_size) != detail::region_size<Config>()) {
      close(fd);
      return Status::kNotSupported;
    }
    void* p = mmap(nullptr, detail::region_size<Config>(), PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
      close(fd);
      return Status::kNotSupported;
    }
    const Region* region = static_cast<const Region*>(p);
    Status status = Status::kOk;
    if (region->init_state.load(std::memory_order_acquire) != detail::kReady ||
        region->magic != detail::kMagic || region->version != detail::kLayoutVersion ||
        region->config_hash != detail::config_hash<Config>()) {
      status = Status::kNotSupported;
    } else {
      out =
          make_snapshot(*region, static_cast<const char*>(p) + detail::blocks_offset<Config>(), fd);
    }
    munmap(p, detail::region_size<Config>());
    close(fd);
    return status;
  }

  uint32_t subscriber_count() const {
    uint32_t n = 0;
    for (uint32_t i = kMaxPublishers; i < kSlots; ++i) {
      const auto& s = region_->slots[i];
      if (detail::lifecycle_state(s.lifecycle.load(std::memory_order_acquire)) ==
              detail::kSlotActive &&
          s.subscribed.load(std::memory_order_acquire)) {
        ++n;
      }
    }
    return n;
  }

 private:
  using Region = detail::Region<Config>;
  using SlotRecord = detail::Slot<kQueueCapacity, kLoanEntries>;
  using GroupRecord = detail::GroupRecord<kQueueCapacity>;
  using Queue = detail::MpmcQueue<kQueueCapacity>;

  [[noreturn]] static void throw_errno(const char* what) {
    throw std::system_error(errno, std::generic_category(), what);
  }

  SlotRecord& slot(uint32_t i) { return region_->slots[i]; }
  GroupRecord& group(uint32_t g) { return region_->groups[g]; }
  detail::BlockHeader& header(uint32_t id) {
    return *reinterpret_cast<detail::BlockHeader*>(blocks_ + static_cast<size_t>(id) * kBlockSize);
  }
  char* payload(uint32_t id) {
    return blocks_ + static_cast<size_t>(id) * kBlockSize + sizeof(detail::BlockHeader);
  }
  static bool is_publisher_slot(uint32_t i) { return i < kMaxPublishers; }

  void open_region() {
    if (options_.name.size() < 2 || options_.name[0] != '/') {
      throw std::invalid_argument("shared-memory names must start with '/'");
    }
    const size_t size = detail::region_size<Config>();
    bool creator = false;
    fd_ = shm_open(options_.name.c_str(), O_CREAT | O_EXCL | O_RDWR, options_.mode);
    if (fd_ >= 0) {
      creator = true;
      // Best effort: without it the umask still bounds the mode.
      static_cast<void>(fchmod(fd_, options_.mode));
    } else if (errno == EEXIST) {
      fd_ = shm_open(options_.name.c_str(), O_RDWR, 0);
    }
    if (fd_ < 0) throw_errno("shm_open");
    try {
      if (creator && ftruncate(fd_, static_cast<off_t>(size)) != 0) {
        const int error = errno;
        shm_unlink(options_.name.c_str());
        errno = error;
        throw_errno("ftruncate");
      }
      if (!creator) wait_for(size);
      void* p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
      if (p == MAP_FAILED) throw_errno("mmap");
      region_ = static_cast<Region*>(p);
      blocks_ = static_cast<char*>(p) + detail::blocks_offset<Config>();
      if (creator) {
        new (region_) Region();
        region_->version = detail::kLayoutVersion;
        region_->magic = detail::kMagic;
        region_->config_hash = detail::config_hash<Config>();
        region_->region_size = size;
        region_->block_count = kBlockCount;
        region_->block_size = kBlockSize;
        region_->max_publishers = kMaxPublishers;
        region_->max_subscribers = kMaxSubscribers;
        region_->max_groups = kMaxGroups;
        region_->queue_capacity = kQueueCapacity;
        region_->free_list.init();
        for (uint32_t i = 0; i < kSlots; ++i) region_->slots[i].init();
        for (uint32_t g = 0; g <= kMaxGroups; ++g) region_->groups[g].init();
        for (uint32_t id = 0; id < kBlockCount; ++id) {
          new (&header(id)) detail::BlockHeader();
          header(id).state.store(0, std::memory_order_relaxed);
        }
        region_->init_state.store(detail::kReady, std::memory_order_release);
      } else {
        const auto deadline = std::chrono::steady_clock::now() + options_.initialization_timeout;
        while (region_->init_state.load(std::memory_order_acquire) != detail::kReady) {
          if (std::chrono::steady_clock::now() > deadline) {
            throw std::runtime_error("shared-memory initialization timeout");
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (region_->magic != detail::kMagic || region_->version != detail::kLayoutVersion ||
            region_->config_hash != detail::config_hash<Config>() || region_->region_size != size) {
          throw std::runtime_error("shared-memory layout or configuration mismatch");
        }
      }
    } catch (...) {
      close_region();
      throw;
    }
  }

  void wait_for(size_t size) {
    const auto deadline = std::chrono::steady_clock::now() + options_.initialization_timeout;
    for (;;) {
      struct stat st {};
      if (fstat(fd_, &st) != 0) throw_errno("fstat");
      if (static_cast<size_t>(st.st_size) == size) return;
      if (st.st_size != 0 || std::chrono::steady_clock::now() > deadline) {
        throw std::runtime_error("shared-memory layout size mismatch");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  Status recover_locked(RecoveryReport* report) {
    RecoveryReport rep;
    if (detail::ofd_lock(fd_, detail::kMaintenanceByte, F_WRLCK) != 0) return Status::kBusy;
    bool locked[kSlots];
    uint64_t live = 0;
    for (uint32_t i = 0; i < kSlots; ++i) {
      locked[i] = detail::ofd_lock(fd_, i, F_WRLCK) == 0;
      if (!locked[i]) {
        live |= 1ULL << i;
      } else if (detail::lifecycle_state(slot(i).lifecycle.load(std::memory_order_acquire)) !=
                     detail::kSlotFree &&
                 reclaim_slot(i, &rep)) {
        ++rep.slots_reclaimed;
      }
    }
    const Status status = repair(live, rep);
    for (uint32_t i = 0; i < kSlots; ++i) {
      if (locked[i]) detail::ofd_lock(fd_, i, F_UNLCK);
    }
    detail::ofd_lock(fd_, detail::kMaintenanceByte, F_UNLCK);
    if (report) *report = rep;
    return status;
  }

  void close_region() {
    if (region_) {
      munmap(region_, detail::region_size<Config>());
      region_ = nullptr;
      blocks_ = nullptr;
    }
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

  int open_endpoint_fd() const {
    const int fd = shm_open(options_.name.c_str(), O_RDWR, 0);
    if (fd < 0) throw_errno("shm_open");
    return fd;
  }

  // Locks a free slot byte with the endpoint's own description, reclaiming
  // leftovers of a previous owner that died. recover() briefly locks every
  // free slot byte, so a full table is only reported after waiting for the
  // maintenance byte and scanning again.
  uint32_t acquire_slot(int fd, uint32_t first, uint32_t last) {
    bool contended = false;
    uint32_t i = try_acquire_slot(fd, first, last, contended);
    if (i != last) return i;
    if (!contended) throw std::runtime_error("endpoint slot limit reached");
    const auto wait = std::min(options_.initialization_timeout, std::chrono::milliseconds(100));
    const auto deadline = std::chrono::steady_clock::now() + wait;
    bool shared = false;
    while (!(shared = detail::ofd_lock(fd, detail::kMaintenanceByte, F_RDLCK) == 0) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    i = try_acquire_slot(fd, first, last, contended);
    if (shared) detail::ofd_lock(fd, detail::kMaintenanceByte, F_UNLCK);
    if (i == last) throw std::runtime_error("endpoint slot limit reached");
    return i;
  }

  uint32_t try_acquire_slot(int fd, uint32_t first, uint32_t last, bool& contended) {
    for (uint32_t i = first; i < last; ++i) {
      const int error = detail::ofd_lock(fd, i, F_WRLCK);
      if (error == EAGAIN || error == EACCES) {
        contended = true;
        continue;
      }
      if (error != 0) {
        errno = error;
        throw_errno("slot lock");
      }
      if (detail::lifecycle_state(slot(i).lifecycle.load(std::memory_order_acquire)) !=
              detail::kSlotFree &&
          !reclaim_slot(i, nullptr)) {
        detail::ofd_lock(fd, i, F_UNLCK);
        contended = true;
        continue;
      }
      return i;
    }
    return last;
  }

  // Returns the slot's activity counter. Counters never restart, so recovery
  // cannot mistake a reused slot for an untouched one.
  uint64_t activate(uint32_t i) {
    auto& s = slot(i);
    const uint64_t lc = s.lifecycle.load(std::memory_order_acquire);
    const uint64_t counter = s.activity.load(std::memory_order_relaxed) >> 32;
    s.lifecycle.store(
        detail::make_lifecycle(detail::lifecycle_generation(lc) + 1, detail::kSlotActive),
        std::memory_order_release);
    return counter;
  }

  void begin_activity(uint32_t i, uint64_t& counter, uint32_t op, uint32_t id) {
    ++counter;
    slot(i).activity.store((counter << 32) | (static_cast<uint64_t>(op) << 24) | (id & 0xffffff),
                           std::memory_order_relaxed);
    // Recovery that observes any later store of this operation also observes
    // the activity word above.
    std::atomic_thread_fence(std::memory_order_release);
  }
  void end_activity(uint32_t i, uint64_t& counter) {
    ++counter;
    slot(i).activity.store(counter << 32, std::memory_order_release);
  }

  void release_bits(uint32_t id, uint64_t bits) {
    const uint64_t previous = header(id).state.fetch_sub(bits, std::memory_order_acq_rel);
    if (previous == bits) {
      SHM_PUBSUB_FAULT_POINT("release.before_push");
      region_->free_list.push(id);
    }
  }

  // Clears a dead slot's holder bit, releasing the block if it was the last
  // reference.
  bool clear_holder(uint32_t id, uint32_t i) {
    const uint64_t bit = 1ULL << i;
    uint64_t state = header(id).state.load(std::memory_order_acquire);
    while (state & bit) {
      if (header(id).state.compare_exchange_weak(state, state - bit, std::memory_order_acq_rel)) {
        if (state == bit) {
          SHM_PUBSUB_FAULT_POINT("release.before_push");
          region_->free_list.push(id);
        }
        return true;
      }
    }
    return false;
  }

  uint64_t live_publishers() const {
    uint64_t pins = 0;
    for (uint32_t p = 0; p < kMaxPublishers; ++p) {
      if (detail::ofd_held_elsewhere(fd_, p)) pins |= 1ULL << p;
    }
    return pins;
  }

  bool has_targets() const {
    for (uint32_t i = kMaxPublishers; i < kSlots; ++i) {
      const auto& s = region_->slots[i];
      if (detail::lifecycle_state(s.lifecycle.load(std::memory_order_relaxed)) ==
              detail::kSlotActive &&
          s.subscribed.load(std::memory_order_relaxed) &&
          s.group.load(std::memory_order_relaxed) == 0) {
        return true;
      }
    }
    for (uint32_t g = 1; g <= kMaxGroups; ++g) {
      if (region_->groups[g].member_mask.load(std::memory_order_relaxed)) return true;
    }
    return false;
  }

  void record_loan(uint32_t i, uint32_t id) {
    auto& s = slot(i);
    for (uint32_t k = 0; k < kLoanEntries; ++k) {
      if (s.held[k].load(std::memory_order_relaxed) == detail::kNoEntry) {
        s.held[k].store(id, std::memory_order_relaxed);
        return;
      }
    }
  }
  void forget_loan(uint32_t i, uint32_t id) {
    auto& s = slot(i);
    for (uint32_t k = 0; k < kLoanEntries; ++k) {
      if (s.held[k].load(std::memory_order_relaxed) == id) {
        s.held[k].store(detail::kNoEntry, std::memory_order_relaxed);
        return;
      }
    }
  }

  static uint64_t member_bit(uint32_t slot_index) { return 1ULL << (slot_index - kMaxPublishers); }

  // The first member fixes the group's policy; later members must match it.
  void claim_group_policy(uint32_t g, uint32_t desired) {
    uint32_t current = 0;
    if (group(g).policy.compare_exchange_strong(current, desired, std::memory_order_acq_rel)) {
      return;
    }
    if (current != desired) {
      throw std::invalid_argument("subscriber options conflict with the group's policy");
    }
  }

  void join_group(uint32_t g, uint32_t i) {
    group(g).member_mask.fetch_or(member_bit(i), std::memory_order_acq_rel);
  }

  void leave_group(uint32_t g, uint32_t i) {
    auto& rec = group(g);
    const uint64_t bit = member_bit(i);
    const uint64_t previous = rec.member_mask.fetch_and(~bit, std::memory_order_acq_rel);
    if (previous == bit && !detail::policy_durable(rec.policy.load(std::memory_order_relaxed))) {
      drain_group(rec.queue);
    }
  }

  // Consumes a memberless group's backlog. A crash between releasing a cell
  // and dropping its reference only leaks the block until recover().
  void drain_group(Queue& q) {
    uint64_t pos;
    uint32_t id;
    while (q.try_claim(pos, id)) {
      q.release(pos);
      SHM_PUBSUB_FAULT_POINT("group.drain_after_release");
      if (id < kBlockCount) release_bits(id, detail::kQueueUnit);
    }
  }

  // The caller holds slot i's lock byte, so no owner can use the slot. The
  // steps are idempotent: a reclaimer that dies is resumed by the next one.
  bool reclaim_slot(uint32_t i, RecoveryReport* report) {
    auto& s = slot(i);
    uint64_t lc = s.lifecycle.load(std::memory_order_acquire);
    for (;;) {
      if (detail::lifecycle_state(lc) == detail::kSlotFree) return false;
      if (detail::lifecycle_state(lc) == detail::kSlotReclaiming) break;
      const uint64_t next = (lc & ~detail::kStateMask) | detail::kSlotReclaiming;
      if (s.lifecycle.compare_exchange_weak(lc, next, std::memory_order_acq_rel)) break;
    }
    uint64_t counter = s.activity.load(std::memory_order_relaxed) >> 32;
    begin_activity(i, counter, detail::kOpReclaim, detail::kNoBlock);
    s.subscribed.store(0, std::memory_order_release);
    if (is_publisher_slot(i)) {
      if (s.pinned.exchange(0, std::memory_order_acq_rel) && report) ++report->pins_cleared;
    } else {
      std::atomic_thread_fence(std::memory_order_seq_cst);
      const uint64_t bit = member_bit(i);
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
      for (;;) {
        uint64_t waiting = 0;
        for (uint32_t p = 0; p < kMaxPublishers; ++p) {
          if (slot(p).pinned.load(std::memory_order_acquire) & bit) waiting |= 1ULL << p;
        }
        // A dead publisher never touches the queue again; its mask is stale.
        if (!(waiting & live_publishers())) break;
        if (std::chrono::steady_clock::now() > deadline) {
          end_activity(i, counter);
          return false;
        }
        std::this_thread::yield();
      }
      SHM_PUBSUB_FAULT_POINT("reclaim.after_pin_wait");
      drain_dead_queue(s.queue, i);
      const uint32_t g = s.group.load(std::memory_order_relaxed);
      if (g >= 1 && g <= kMaxGroups) leave_group(g, i);
    }
    for (uint32_t k = 0; k < kLoanEntries; ++k) {
      const uint32_t id = s.held[k].load(std::memory_order_relaxed);
      if (id < kBlockCount) clear_holder(id, i);
      s.held[k].store(detail::kNoEntry, std::memory_order_relaxed);
    }
    s.queue.init();
    s.stats.held_loans.store(0, std::memory_order_relaxed);
    end_activity(i, counter);
    s.lifecycle.store(detail::make_lifecycle(detail::lifecycle_generation(lc), detail::kSlotFree),
                      std::memory_order_release);
    return true;
  }

  // No publisher is pinned on this queue and its consumer is gone, so the
  // remaining cells can be resolved without races. A cell is marked released
  // before its reference is dropped, which makes a repeated pass harmless.
  void drain_dead_queue(Queue& q, uint32_t owner) {
    const uint64_t d = q.dequeue_pos.load(std::memory_order_acquire);
    const uint64_t e = q.enqueue_pos.load(std::memory_order_acquire);
    for (uint64_t pos = d >= kQueueCapacity ? d - kQueueCapacity : 0; pos < d; ++pos) {
      auto& c = q.cell(pos);
      uint64_t seq = c.sequence.load(std::memory_order_acquire);
      if (seq != pos + 1) continue;
      const uint32_t id = c.value.load(std::memory_order_relaxed);
      if (!c.sequence.compare_exchange_strong(seq, pos + kQueueCapacity,
                                              std::memory_order_acq_rel)) {
        continue;
      }
      SHM_PUBSUB_FAULT_POINT("reclaim.after_mark");
      // Claimed but never released: either the owner died before converting
      // the reference into its holder bit, or a dropping publisher died
      // before releasing the cell (it drops the reference afterwards).
      if (id < kBlockCount &&
          !(header(id).state.load(std::memory_order_acquire) & (1ULL << owner))) {
        release_bits(id, detail::kQueueUnit);
      }
    }
    for (uint64_t pos = d; pos < e && pos - d <= kQueueCapacity; ++pos) {
      auto& c = q.cell(pos);
      uint64_t seq = c.sequence.load(std::memory_order_acquire);
      if (seq != pos + 1) continue;
      const uint32_t id = c.value.load(std::memory_order_relaxed);
      if (!c.sequence.compare_exchange_strong(seq, pos + kQueueCapacity,
                                              std::memory_order_acq_rel)) {
        continue;
      }
      SHM_PUBSUB_FAULT_POINT("reclaim.after_mark");
      if (id < kBlockCount) release_bits(id, detail::kQueueUnit);
    }
  }

  struct CellFix {
    Queue* queue;
    uint64_t pos;
    bool hole;
  };

  void scan_queue(Queue& q, std::vector<uint32_t>& refs, std::vector<CellFix>& fixes,
                  RecoveryReport& rep) {
    const uint64_t d = q.dequeue_pos.load(std::memory_order_acquire);
    const uint64_t e = q.enqueue_pos.load(std::memory_order_acquire);
    if (e < d || e - d > kQueueCapacity) {
      ++rep.inconsistencies;
      return;
    }
    for (uint64_t pos = d >= kQueueCapacity ? d - kQueueCapacity : 0; pos < d; ++pos) {
      if (q.cell(pos).sequence.load(std::memory_order_acquire) == pos + 1) {
        CellFix fix = {&q, pos, false};
        fixes.push_back(fix);
      }
    }
    for (uint64_t pos = d; pos < e; ++pos) {
      const uint64_t seq = q.cell(pos).sequence.load(std::memory_order_acquire);
      if (seq == pos + 1) {
        const uint32_t id = q.cell(pos).value.load(std::memory_order_relaxed);
        if (id < kBlockCount) {
          ++refs[id];
        } else if (id != detail::kTombstone) {
          ++rep.inconsistencies;
        }
      } else if (seq == pos) {
        CellFix fix = {&q, pos, true};
        fixes.push_back(fix);
      }
    }
  }

  Status repair(uint64_t live, RecoveryReport& rep) {
    uint64_t activity[kSlots] = {};
    for (uint32_t i = 0; i < kSlots; ++i) {
      if (!(live & (1ULL << i))) continue;
      activity[i] = slot(i).activity.fetch_add(0, std::memory_order_acq_rel);
      if (detail::activity_op(activity[i]) != detail::kOpIdle) return Status::kBusy;
    }
    std::vector<uint64_t> states(kBlockCount);
    std::vector<uint32_t> refs(kBlockCount, 0);
    std::vector<char> in_free(kBlockCount, 0);
    std::vector<CellFix> fixes;
    bool list_ok = region_->free_list.for_each_free([&](uint32_t id) {
      if (in_free[id]) ++rep.inconsistencies;
      in_free[id] = 1;
    });
    if (!list_ok) ++rep.inconsistencies;
    for (uint32_t id = 0; id < kBlockCount; ++id) {
      states[id] = header(id).state.load(std::memory_order_acquire);
    }
    for (uint32_t j = kMaxPublishers; j < kSlots; ++j) {
      if (detail::lifecycle_state(slot(j).lifecycle.load(std::memory_order_acquire)) !=
          detail::kSlotFree) {
        scan_queue(slot(j).queue, refs, fixes, rep);
      }
    }
    for (uint32_t g = 1; g <= kMaxGroups; ++g) scan_queue(group(g).queue, refs, fixes, rep);
    std::atomic_thread_fence(std::memory_order_acquire);
    for (uint32_t i = 0; i < kSlots; ++i) {
      if (!(live & (1ULL << i))) continue;
      if (slot(i).activity.fetch_add(0, std::memory_order_acq_rel) != activity[i]) {
        return Status::kBusy;
      }
    }
    if (!list_ok) return Status::kCorrupted;

    // The snapshot above is consistent. Cells and blocks found unowned in it
    // cannot become reachable again, so the repairs below are safe even if
    // live endpoints resume meanwhile; reference words are fixed by CAS.
    for (size_t k = 0; k < fixes.size(); ++k) {
      auto& c = fixes[k].queue->cell(fixes[k].pos);
      uint64_t expected = fixes[k].hole ? fixes[k].pos : fixes[k].pos + 1;
      if (fixes[k].hole) c.value.store(detail::kTombstone, std::memory_order_relaxed);
      const uint64_t next = fixes[k].hole ? fixes[k].pos + 1 : fixes[k].pos + kQueueCapacity;
      if (c.sequence.compare_exchange_strong(expected, next, std::memory_order_acq_rel)) {
        ++(fixes[k].hole ? rep.holes_filled : rep.cells_released);
      }
    }
    for (uint32_t p = 0; p < kMaxPublishers; ++p) {
      if (!(live & (1ULL << p)) && slot(p).pinned.exchange(0, std::memory_order_acq_rel)) {
        ++rep.pins_cleared;
      }
    }
    const uint64_t live_members = live >> kMaxPublishers;
    for (uint32_t g = 1; g <= kMaxGroups; ++g) {
      const uint64_t stale = group(g).member_mask.load(std::memory_order_acquire) & ~live_members;
      if (stale) {
        group(g).member_mask.fetch_and(~stale, std::memory_order_acq_rel);
        ++rep.members_cleared;
      }
    }
    const uint64_t holders = live & detail::kHolderMask;
    for (uint32_t id = 0; id < kBlockCount; ++id) {
      const uint64_t observed = states[id];
      const uint64_t computed =
          (observed & holders) | (static_cast<uint64_t>(refs[id]) << detail::kMaxHolderBits);
      if (in_free[id]) {
        if (observed != 0 || refs[id] != 0) ++rep.inconsistencies;
        continue;
      }
      if (computed == observed && computed != 0) continue;
      uint64_t expected = observed;
      if (!header(id).state.compare_exchange_strong(expected, computed,
                                                    std::memory_order_acq_rel)) {
        continue;
      }
      if (computed == 0) {
        region_->free_list.force_push(id);
        ++rep.orphans_freed;
      } else {
        ++rep.states_repaired;
      }
    }
    return Status::kOk;
  }

  static Snapshot make_snapshot(const Region& region, const char* blocks, int fd) {
    Snapshot snap;
    snap.layout_version = region.version;
    snap.config_hash = region.config_hash;
    snap.block_count = kBlockCount;
    snap.block_size = kBlockSize;
    snap.queue_capacity = kQueueCapacity;
    uint32_t free_blocks = 0;
    snap.free_list_consistent = region.free_list.for_each_free([&](uint32_t) { ++free_blocks; });
    snap.free_blocks = free_blocks;
    (void)blocks;
    for (uint32_t i = 0; i < kSlots; ++i) {
      const auto& s = region.slots[i];
      const uint64_t lc = s.lifecycle.load(std::memory_order_acquire);
      const uint64_t act = s.activity.load(std::memory_order_acquire);
      SlotInfo info;
      info.index = i;
      info.publisher = is_publisher_slot(i);
      info.live = detail::ofd_held_elsewhere(fd, i);
      info.state = static_cast<uint32_t>(detail::lifecycle_state(lc));
      info.generation = detail::lifecycle_generation(lc);
      info.pid = s.pid.load(std::memory_order_relaxed);
      info.group = s.group.load(std::memory_order_relaxed);
      info.subscribed = s.subscribed.load(std::memory_order_relaxed) != 0;
      info.pins = s.pinned.load(std::memory_order_relaxed);
      info.queue_depth = info.publisher ? 0 : s.queue.size_approx();
      info.held_loans = s.stats.held_loans.load(std::memory_order_relaxed);
      info.op = detail::activity_op(act);
      info.op_block = detail::activity_block(act);
      info.published = s.stats.published.load(std::memory_order_relaxed);
      info.taken = s.stats.taken.load(std::memory_order_relaxed);
      info.dropped_newest = s.stats.dropped_newest.load(std::memory_order_relaxed);
      info.dropped_oldest = s.stats.dropped_oldest.load(std::memory_order_relaxed);
      snap.slots.push_back(info);
    }
    for (uint32_t g = 1; g <= kMaxGroups; ++g) {
      const auto& rec = region.groups[g];
      GroupInfo info;
      info.id = g;
      info.member_mask = rec.member_mask.load(std::memory_order_relaxed);
      info.members = static_cast<uint32_t>(__builtin_popcountll(info.member_mask));
      const uint32_t policy = rec.policy.load(std::memory_order_relaxed);
      info.policy_set = (policy & detail::kPolicySet) != 0;
      info.overflow = static_cast<Overflow>(detail::policy_overflow(policy));
      info.queue_depth = rec.queue.size_approx();
      info.dropped_newest = rec.dropped_newest.load(std::memory_order_relaxed);
      info.dropped_oldest = rec.dropped_oldest.load(std::memory_order_relaxed);
      snap.groups.push_back(info);
    }
    return snap;
  }

  Options options_;
  int fd_ = -1;
  Region* region_ = nullptr;
  char* blocks_ = nullptr;
  std::mutex mutex_;
};

template <class C>
constexpr uint32_t Topic<C>::kBlockCount;
template <class C>
constexpr uint32_t Topic<C>::kBlockSize;
template <class C>
constexpr uint32_t Topic<C>::kQueueCapacity;
template <class C>
constexpr uint32_t Topic<C>::kMaxPublishers;
template <class C>
constexpr uint32_t Topic<C>::kMaxSubscribers;
template <class C>
constexpr uint32_t Topic<C>::kMaxGroups;
template <class C>
constexpr uint32_t Topic<C>::kMaxLoansPerPublisher;
template <class C>
constexpr uint32_t Topic<C>::kMaxLoansPerSubscriber;
template <class C>
constexpr uint32_t Topic<C>::kLoanEntries;
template <class C>
constexpr uint32_t Topic<C>::kSlots;
template <class C>
constexpr size_t Topic<C>::kMaxPayload;
template <class C>
constexpr uint64_t Topic<C>::kRequiredBlocks;

}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_SHM_PUBSUB_HPP_
