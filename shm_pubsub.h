#ifndef SHM_PUBSUB_H_
#define SHM_PUBSUB_H_

#include <fcntl.h>
#include <linux/futex.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

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
#include <thread>
#include <utility>

#include "lock_free_list.h"

constexpr size_t MAX_WRITERS = 8;
constexpr size_t MAX_READERS = 16;
constexpr size_t BLOCK_COUNT = 32;
constexpr size_t BLOCK_SIZE = 4096;
constexpr size_t QUEUE_CAPACITY = 32;

struct AtomicQueue {
  struct Cell {
    std::atomic<size_t> sequence;
    std::atomic<size_t> data;
    Cell() : sequence(0), data(0) {}
  };
  static_assert((QUEUE_CAPACITY & (QUEUE_CAPACITY - 1)) == 0,
                "queue capacity must be a power of two");
  Cell queue[QUEUE_CAPACITY];
  std::atomic<size_t> enqueue_pos;
  std::atomic<size_t> dequeue_pos;
  AtomicQueue() : enqueue_pos(0), dequeue_pos(0) {}
  void init() {
    enqueue_pos.store(0);
    dequeue_pos.store(0);
    for (size_t i = 0; i < QUEUE_CAPACITY; ++i) queue[i].sequence.store(i);
  }
  bool enqueue(size_t value) {
    size_t pos = enqueue_pos.load(std::memory_order_relaxed);
    Cell* cell;
    for (;;) {
      cell = &queue[pos & (QUEUE_CAPACITY - 1)];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t d = static_cast<intptr_t>(seq - pos);
      if (d == 0) {
        if (enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (d < 0)
        return false;
      else
        pos = enqueue_pos.load(std::memory_order_relaxed);
    }
    cell->data.store(value, std::memory_order_relaxed);
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }
  bool dequeue(size_t& value) {
    size_t pos = dequeue_pos.load(std::memory_order_relaxed);
    Cell* cell;
    for (;;) {
      cell = &queue[pos & (QUEUE_CAPACITY - 1)];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t d = static_cast<intptr_t>(seq - (pos + 1));
      if (d == 0) {
        if (dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (d < 0)
        return false;
      else
        pos = dequeue_pos.load(std::memory_order_relaxed);
    }
    value = cell->data.load(std::memory_order_relaxed);
    cell->sequence.store(pos + QUEUE_CAPACITY, std::memory_order_release);
    return true;
  }
  bool contains(size_t value) const {
    size_t b = dequeue_pos.load(std::memory_order_acquire),
           e = enqueue_pos.load(std::memory_order_acquire);
    for (size_t p = b; p != e && p - b < QUEUE_CAPACITY; ++p)
      if (queue[p & (QUEUE_CAPACITY - 1)].sequence.load(std::memory_order_acquire) == p + 1 &&
          queue[p & (QUEUE_CAPACITY - 1)].data.load(std::memory_order_relaxed) == value)
        return true;
    return false;
  }
  void clear() {
    size_t ignored;
    while (dequeue(ignored)) {
    }
  }
};

struct DataBlock {
  std::atomic<size_t> ref_count;
  size_t data_len;
  uint64_t reserved;
  char data[BLOCK_SIZE - 24];
  DataBlock() : ref_count(0), data_len(0), reserved(0), data{} {}
};
static_assert(sizeof(DataBlock) == BLOCK_SIZE, "unsupported DataBlock ABI");

enum SlotState : uint32_t { SLOT_FREE, SLOT_ACTIVE, SLOT_RECLAIMING };
// The state and in-flight publisher count share one CAS word.  In particular,
// acquiring a publisher reference and changing ACTIVE to RECLAIMING can never
// pass each other between two independent atomic operations.
struct ReaderInfo {
  std::atomic<uint64_t> lifecycle;
  std::atomic<bool> subscribed;
  AtomicQueue broadcast_queue;
  ReaderInfo() : lifecycle(SLOT_FREE), subscribed(false) {}
};

struct SharedMeta {
  static constexpr uint64_t kMagic = 0x53484d5055425355ULL;
  static constexpr uint32_t kVersion = 4;
  std::atomic<uint32_t> init_state;
  uint64_t magic;
  uint32_t version;
  uint32_t reserved;
  size_t layout_size;
  size_t block_count;
  size_t block_size;
  LockFreeFreeList<uint32_t, BLOCK_COUNT> free_list;
  alignas(4) std::atomic<uint32_t> notification;
  ReaderInfo readers[MAX_READERS];
  AtomicQueue shared_queue;
  SharedMeta()
      : init_state(1),
        magic(kMagic),
        version(kVersion),
        reserved(0),
        layout_size(0),
        block_count(BLOCK_COUNT),
        block_size(BLOCK_SIZE),
        notification(0) {}
};

class ShmPubSub {
 public:
  enum Role { PUBLISHER, SUBSCRIBER };
  enum DeliveryMode { BROADCAST, COMPETING };
  enum class Status {
    OK,
    NO_SUBSCRIBERS,
    PARTIAL,
    QUEUE_FULL,
    MESSAGE_TOO_LARGE,
    INVALID_ARGUMENT,
    WRONG_ROLE,
    WOULD_BLOCK,
    BUFFER_TOO_SMALL,
    NOT_SUPPORTED,
    TIMED_OUT,
    STOPPED
  };
  struct Result {
    Status status;
    size_t delivered;
    size_t required_size;
    bool ok() const {
      return status == Status::OK || status == Status::NO_SUBSCRIBERS || status == Status::PARTIAL;
    }
  };
  struct Options {
    std::string name;
    bool unlink_on_destroy;
    uint64_t initialization_timeout_ms;
    Options() : name("/shm_pubsub_mq"), unlink_on_destroy(false), initialization_timeout_ms(5000) {}
  };

  // Each loan holds a pool reference; its endpoint must outlive it.
  class WriteLoan {
   public:
    WriteLoan() = default;
    ~WriteLoan() { reset(); }
    WriteLoan(WriteLoan&& other) noexcept { *this = std::move(other); }
    WriteLoan& operator=(WriteLoan&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = other.owner_;
        id_ = other.id_;
        other.owner_ = nullptr;
      }
      return *this;
    }
    explicit operator bool() const { return owner_ != nullptr; }
    char* data() { return owner_ ? owner_->blocks_[id_].data : nullptr; }
    size_t size() const { return owner_ ? owner_->blocks_[id_].data_len : 0; }
    void reset() {
      if (owner_) owner_->release_ref(id_);
      owner_ = nullptr;
    }

   private:
    friend class ShmPubSub;
    ShmPubSub* owner_ = nullptr;
    size_t id_ = 0;
  };

  class ReadLoan {
   public:
    ReadLoan() = default;
    ~ReadLoan() { reset(); }
    ReadLoan(ReadLoan&& other) noexcept { *this = std::move(other); }
    ReadLoan& operator=(ReadLoan&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = other.owner_;
        id_ = other.id_;
        other.owner_ = nullptr;
      }
      return *this;
    }
    explicit operator bool() const { return owner_ != nullptr; }
    const char* data() const { return owner_ ? owner_->blocks_[id_].data : nullptr; }
    size_t size() const { return owner_ ? owner_->blocks_[id_].data_len : 0; }
    void reset() {
      if (owner_) owner_->release_ref(id_);
      owner_ = nullptr;
    }

   private:
    friend class ShmPubSub;
    ShmPubSub* owner_ = nullptr;
    size_t id_ = 0;
  };

  explicit ShmPubSub(Role role, const Options& options = Options())
      : role_(role), options_(options) {
    try {
      open_region();
      check_platform();
      register_process();
    } catch (...) {
      close_region();
      throw;
    }
  }
  ~ShmPubSub() {
    stop();
    if (reader_info_) reclaim_reader(*reader_info_);
    close_region();
    if (options_.unlink_on_destroy) shm_unlink(options_.name.c_str());
  }
  ShmPubSub(const ShmPubSub&) = delete;
  ShmPubSub& operator=(const ShmPubSub&) = delete;

  // Cancels current and future blocking receives on this endpoint.
  void stop() {
    if (role_ == SUBSCRIBER && running_.exchange(false)) notify();
  }

  static bool destroy(const std::string& name) { return shm_unlink(name.c_str()) == 0; }
  static constexpr size_t max_payload_size() { return sizeof(DataBlock::data); }

  Status loan(size_t len, WriteLoan& out) {
    if (role_ != PUBLISHER) return Status::WRONG_ROLE;
    if (out || len == 0) return Status::INVALID_ARGUMENT;
    if (len > max_payload_size()) return Status::MESSAGE_TOO_LARGE;
    if (try_loan(len, out)) return Status::OK;
    collect_garbage();
    return try_loan(len, out) ? Status::OK : Status::QUEUE_FULL;
  }
  Result publish_loan(WriteLoan&& sample, DeliveryMode mode = BROADCAST) {
    if (role_ != PUBLISHER) return {Status::WRONG_ROLE, 0, 0};
    if (sample.owner_ != this) return {Status::INVALID_ARGUMENT, 0, 0};
    return deliver(sample, get_valid_subscriber_count(), mode);
  }
  Result publish_result(const void* data, size_t len, DeliveryMode mode = BROADCAST) {
    if (role_ != PUBLISHER) return {Status::WRONG_ROLE, 0, 0};
    if (!data || len == 0) return {Status::INVALID_ARGUMENT, 0, 0};
    if (len > max_payload_size()) return {Status::MESSAGE_TOO_LARGE, 0, max_payload_size()};
    size_t subscribers = get_valid_subscriber_count();
    if (!subscribers) return {Status::NO_SUBSCRIBERS, 0, 0};
    WriteLoan sample;
    if (!try_loan(len, sample)) {
      collect_garbage();
      subscribers = get_valid_subscriber_count();
      if (!subscribers) return {Status::NO_SUBSCRIBERS, 0, 0};
      if (!try_loan(len, sample)) return {Status::QUEUE_FULL, 0, 0};
    }
    std::memcpy(sample.data(), data, len);
    return deliver(sample, subscribers, mode);
  }
  bool publish(const void* data, size_t len, DeliveryMode mode = BROADCAST) {
    return publish_result(data, len, mode).ok();
  }

  Status take(ReadLoan& out) {
    if (role_ != SUBSCRIBER || !owns_reader_slot()) return Status::WRONG_ROLE;
    if (out) return Status::INVALID_ARGUMENT;
    size_t id;
    if (!reader_info_->broadcast_queue.dequeue(id) && !meta_->shared_queue.dequeue(id))
      return Status::WOULD_BLOCK;
    if (id >= BLOCK_COUNT) return Status::NOT_SUPPORTED;
    out.id_ = id;
    out.owner_ = this;
    return Status::OK;
  }
  Status wait_take(ReadLoan& out, std::chrono::milliseconds timeout) {
    if (role_ != SUBSCRIBER) return Status::WRONG_ROLE;
    if (timeout.count() < 0 || out) return Status::INVALID_ARGUMENT;
    auto start = std::chrono::steady_clock::now();
    while (running_.load(std::memory_order_acquire)) {
      Status status = take(out);
      if (status != Status::WOULD_BLOCK) return status;
      auto remaining = timeout - std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now() - start);
      if (remaining.count() <= 0) return Status::TIMED_OUT;

      // Arm before checking again: either the second take sees the enqueue,
      // or a publisher changes the futex word and wakes (or prevents sleep).
      uint32_t expected = meta_->notification.fetch_or(kWaitArmed) | kWaitArmed;
      if (!running_.load(std::memory_order_acquire)) break;
      status = take(out);
      if (status != Status::WOULD_BLOCK) return status;
      timespec ts{};
      ts.tv_sec = remaining.count() / 1000;
      ts.tv_nsec = (remaining.count() % 1000) * 1000000;
      long rc = syscall(SYS_futex, &meta_->notification, FUTEX_WAIT, expected, &ts, nullptr, 0);
      if (rc == -1 && errno != EAGAIN && errno != EINTR && errno != ETIMEDOUT)
        return Status::NOT_SUPPORTED;
    }
    return Status::STOPPED;
  }
  Result receive_result(void* buf, size_t len, size_t& actual) {
    actual = 0;
    if (role_ != SUBSCRIBER) return {Status::WRONG_ROLE, 0, 0};
    if (!buf && len) return {Status::INVALID_ARGUMENT, 0, 0};
    ReadLoan sample;
    const Status status = take(sample);
    return copy_received(status, sample, buf, len, actual);
  }
  Result receive_wait(void* buf, size_t len, size_t& actual, std::chrono::milliseconds timeout) {
    actual = 0;
    if (role_ != SUBSCRIBER) return {Status::WRONG_ROLE, 0, 0};
    if (!buf && len) return {Status::INVALID_ARGUMENT, 0, 0};
    ReadLoan sample;
    const Status status = wait_take(sample, timeout);
    return copy_received(status, sample, buf, len, actual);
  }
  bool receive(void* buf, size_t len, size_t& actual) {
    return receive_result(buf, len, actual).status == Status::OK;
  }

  // Reap abandoned reader slots without waiting for a publisher or a live reader.
  size_t collect_garbage() {
    std::lock_guard<std::mutex> lock(maintenance_mutex_);
    size_t collected = 0;
    for (size_t i = 0; i < MAX_READERS; ++i) {
      auto& reader = meta_->readers[i];
      if (&reader == reader_info_ ||
          lifecycle_state(reader.lifecycle.load(std::memory_order_acquire)) == SLOT_FREE)
        continue;
      if (set_slot_lock(MAX_WRITERS + i, F_WRLCK) != 0) continue;
      if (reclaim_reader(reader)) ++collected;
      set_slot_lock(MAX_WRITERS + i, F_UNLCK);
    }
    return collected;
  }

  bool subscribe() {
    if (role_ != SUBSCRIBER || !owns_reader_slot()) return false;
    reader_info_->subscribed.store(true, std::memory_order_release);
    return true;
  }
  bool unsubscribe() {
    if (role_ != SUBSCRIBER || !owns_reader_slot()) return false;
    reader_info_->subscribed.store(false, std::memory_order_release);
    return true;
  }
  size_t get_valid_subscriber_count() const {
    size_t n = 0;
    for (size_t i = 0; i < MAX_READERS; ++i)
      if (lifecycle_state(meta_->readers[i].lifecycle.load(std::memory_order_acquire)) ==
              SLOT_ACTIVE &&
          meta_->readers[i].subscribed.load(std::memory_order_acquire))
        ++n;
    return n;
  }

 private:
  static constexpr uint32_t kWaitArmed = uint32_t(1) << 31;
  static constexpr uint64_t kStateMask = 3;
  static constexpr uint64_t kPublisherIncrement = 4;
  static constexpr uint64_t kPublisherMask = 0xfffffffcULL;
  static SlotState lifecycle_state(uint64_t lifecycle) {
    return static_cast<SlotState>(lifecycle & kStateMask);
  }
  static uint32_t lifecycle_generation(uint64_t lifecycle) {
    return static_cast<uint32_t>(lifecycle >> 32);
  }
  static uint64_t make_lifecycle(uint32_t generation, SlotState state) {
    return (static_cast<uint64_t>(generation) << 32) | state;
  }
  bool owns_reader_slot() const {
    if (!reader_info_) return false;
    uint64_t v = reader_info_->lifecycle.load(std::memory_order_acquire);
    return lifecycle_generation(v) == reader_generation_ && lifecycle_state(v) == SLOT_ACTIVE;
  }
  size_t region_size() const { return sizeof(SharedMeta) + BLOCK_COUNT * sizeof(DataBlock); }
  void open_region() {
    bool creator = false;
    shm_fd_ = shm_open(options_.name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0660);
    if (shm_fd_ >= 0)
      creator = true;
    else if (errno == EEXIST)
      shm_fd_ = shm_open(options_.name.c_str(), O_RDWR, 0660);
    if (shm_fd_ < 0) throw std::runtime_error("shm_open failed");
    if (creator && ftruncate(shm_fd_, region_size()) != 0) {
      shm_unlink(options_.name.c_str());
      throw std::runtime_error("ftruncate failed");
    }
    if (!creator) {
      auto deadline = now() + options_.initialization_timeout_ms;
      struct stat st{};
      for (;;) {
        if (fstat(shm_fd_, &st) != 0) {
          throw std::runtime_error("fstat failed");
        }
        if (static_cast<size_t>(st.st_size) == region_size()) break;
        if (st.st_size != 0 || now() > deadline) {
          throw std::runtime_error("shared-memory layout size mismatch");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    shm_ptr_ = mmap(nullptr, region_size(), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (shm_ptr_ == MAP_FAILED) {
      throw std::runtime_error("mmap failed");
    }
    meta_ = static_cast<SharedMeta*>(shm_ptr_);
    blocks_ = reinterpret_cast<DataBlock*>(static_cast<char*>(shm_ptr_) + sizeof(SharedMeta));
    if (creator) {
      new (meta_) SharedMeta();
      meta_->layout_size = region_size();
      meta_->free_list.init();
      meta_->shared_queue.init();
      for (size_t i = 0; i < MAX_READERS; ++i) meta_->readers[i].broadcast_queue.init();
      for (size_t i = 0; i < BLOCK_COUNT; ++i) new (&blocks_[i]) DataBlock();
      meta_->init_state.store(2, std::memory_order_release);
    } else {
      auto deadline = now() + options_.initialization_timeout_ms;
      while (meta_->init_state.load(std::memory_order_acquire) != 2) {
        if (now() > deadline) {
          throw std::runtime_error("shared-memory initialization timeout");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      if (meta_->magic != SharedMeta::kMagic || meta_->version != SharedMeta::kVersion ||
          meta_->layout_size != region_size()) {
        throw std::runtime_error("shared-memory version mismatch");
      }
    }
  }
  void check_platform() {
    static_assert(sizeof(std::atomic<uint32_t>) == sizeof(uint32_t), "unsupported futex word ABI");
    if (!meta_->shared_queue.enqueue_pos.is_lock_free() ||
        !meta_->readers[0].lifecycle.is_lock_free() ||
        !meta_->readers[0].subscribed.is_lock_free() || !meta_->notification.is_lock_free() ||
        !meta_->free_list.is_lock_free())
      throw std::runtime_error("required atomics are not lock-free on this platform");
  }
  int set_slot_lock(size_t slot, short type) const {
    struct flock lock{};
    lock.l_type = type;
    lock.l_whence = SEEK_SET;
    lock.l_start = static_cast<off_t>(slot);
    lock.l_len = 1;
    int rc;
    do {
      rc = fcntl(shm_fd_, F_OFD_SETLK, &lock);
    } while (rc == -1 && errno == EINTR);
    return rc == 0 ? 0 : errno;
  }
  void register_process() {
    const size_t offset = role_ == PUBLISHER ? 0 : MAX_WRITERS;
    const size_t slots = role_ == PUBLISHER ? MAX_WRITERS : MAX_READERS;
    for (size_t i = 0; i < slots; ++i) {
      const int error = set_slot_lock(offset + i, F_WRLCK);
      if (error == EACCES || error == EAGAIN) continue;
      if (error != 0)
        throw std::runtime_error(std::string("shared-memory slot lock: ") + std::strerror(error));
      if (role_ == PUBLISHER) return;

      auto& reader = meta_->readers[i];
      reclaim_reader(reader);
      uint64_t previous = reader.lifecycle.load(std::memory_order_acquire);
      if (lifecycle_state(previous) != SLOT_FREE) {
        set_slot_lock(offset + i, F_UNLCK);
        continue;
      }
      reader.broadcast_queue.init();
      reader.subscribed.store(false, std::memory_order_relaxed);
      reader_generation_ = lifecycle_generation(previous) + 1;
      reader.lifecycle.store(make_lifecycle(reader_generation_, SLOT_ACTIVE),
                             std::memory_order_release);
      reader_info_ = &reader;
      return;
    }
    throw std::runtime_error("process slot limit reached");
  }
  // The caller holds the slot's OFD lock. Publishers pin ACTIVE slots with
  // the lifecycle counter and never use this lock on the data path.
  bool reclaim_reader(ReaderInfo& reader) {
    uint64_t state = reader.lifecycle.load(std::memory_order_acquire);
    while (lifecycle_state(state) == SLOT_ACTIVE &&
           !reader.lifecycle.compare_exchange_weak(state, (state & ~kStateMask) | SLOT_RECLAIMING,
                                                   std::memory_order_acq_rel)) {
    }
    if (lifecycle_state(state) == SLOT_FREE) return false;
    reader.subscribed.store(false, std::memory_order_release);
    if (reader.lifecycle.load(std::memory_order_acquire) & kPublisherMask) return false;
    drain(reader);
    reader.lifecycle.store(make_lifecycle(lifecycle_generation(state), SLOT_FREE),
                           std::memory_order_release);
    return true;
  }
  void notify() {
    uint32_t state = meta_->notification.load(std::memory_order_relaxed);
    // Even without an armed waiter, the RMW orders queue publication before
    // a waiter that arms next and retries its queues.
    for (;;) {
      const bool armed = state & kWaitArmed;
      const uint32_t next = armed ? (state + 1) & ~kWaitArmed : state;
      if (meta_->notification.compare_exchange_weak(state, next)) {
        if (armed)
          syscall(SYS_futex, &meta_->notification, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0);
        return;
      }
    }
  }
  bool try_loan(size_t len, WriteLoan& out) {
    uint32_t id;
    if (!meta_->free_list.pop(id)) return false;
    blocks_[id].ref_count.store(1, std::memory_order_relaxed);
    blocks_[id].data_len = len;
    out.id_ = id;
    out.owner_ = this;
    return true;
  }
  Result deliver(WriteLoan& sample, size_t subscribers, DeliveryMode mode) {
    if (!subscribers) {
      sample.reset();
      return {Status::NO_SUBSCRIBERS, 0, 0};
    }
    const size_t id = sample.id_;
    const size_t delivered = mode == COMPETING ? publish_competing(id) : publish_broadcast(id);
    sample.reset();
    if (delivered == 0) return {Status::QUEUE_FULL, 0, 0};
    notify();
    if (mode == BROADCAST && delivered < subscribers) return {Status::PARTIAL, delivered, 0};
    return {Status::OK, delivered, 0};
  }
  static Result copy_received(Status status, const ReadLoan& sample, void* buf, size_t len,
                              size_t& actual) {
    if (status != Status::OK) return {status, 0, 0};
    actual = sample.size();
    if (len < actual) return {Status::BUFFER_TOO_SMALL, 0, actual};
    std::memcpy(buf, sample.data(), actual);
    return {Status::OK, 1, actual};
  }
  void close_region() {
    if (shm_ptr_ != MAP_FAILED) {
      munmap(shm_ptr_, region_size());
      shm_ptr_ = MAP_FAILED;
    }
    if (shm_fd_ >= 0) {
      close(shm_fd_);
      shm_fd_ = -1;
    }
  }
  size_t publish_competing(size_t id) {
    blocks_[id].ref_count.fetch_add(1);
    if (meta_->shared_queue.enqueue(id)) return 1;
    release_ref(id);
    return 0;
  }
  size_t publish_broadcast(size_t id) {
    size_t n = 0;
    for (auto& r : meta_->readers) {
      uint64_t e = r.lifecycle.load(std::memory_order_acquire);
      while (lifecycle_state(e) == SLOT_ACTIVE &&
             !r.lifecycle.compare_exchange_weak(e, e + kPublisherIncrement,
                                                std::memory_order_acq_rel)) {
      }
      if (lifecycle_state(e) != SLOT_ACTIVE) continue;
      if (r.subscribed.load(std::memory_order_acquire)) {
        blocks_[id].ref_count.fetch_add(1);
        if (r.broadcast_queue.enqueue(id))
          ++n;
        else
          release_ref(id);
      }
      r.lifecycle.fetch_sub(kPublisherIncrement, std::memory_order_release);
    }
    return n;
  }
  void release_ref(size_t id) {
    if (blocks_[id].ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1)
      meta_->free_list.push(static_cast<uint32_t>(id));
  }
  void drain(ReaderInfo& r) {
    size_t id;
    while (r.broadcast_queue.dequeue(id))
      if (id < BLOCK_COUNT) release_ref(id);
  }
  static uint64_t now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }
  Role role_;
  Options options_;
  int shm_fd_ = -1;
  void* shm_ptr_ = MAP_FAILED;
  SharedMeta* meta_ = nullptr;
  DataBlock* blocks_ = nullptr;
  ReaderInfo* reader_info_ = nullptr;
  uint32_t reader_generation_ = 0;
  std::mutex maintenance_mutex_;
  std::atomic<bool> running_{true};
};

#endif
