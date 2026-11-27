#ifndef SHM_PUBSUB_WAIT_SET_HPP_
#define SHM_PUBSUB_WAIT_SET_HPP_

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "shm_pubsub/detail/platform.hpp"
#include "shm_pubsub/status.hpp"

namespace shm_pubsub {
namespace detail {

struct WaitHandle {
  std::atomic<uint32_t>* generation;
  std::atomic<uint32_t>* waiters;
  const void* source;
  bool (*ready)(const void* source);
  const std::atomic<bool>* running;
};

}  // namespace detail

// Waits on several subscribers, possibly from topics with different
// configurations, with one futex_waitv call (Linux 5.16+). No thread is
// created. A ready result means a take() is likely to succeed; for consumer
// groups another member may win the message first.
class WaitSet {
 public:
  static bool supported() { return detail::futex_waitv_supported(); }

  template <class Subscriber>
  bool add(Subscriber& subscriber) {
    if (handles_.size() >= detail::kFutexWaitvMax) return false;
    handles_.push_back(subscriber.wait_handle());
    return true;
  }
  size_t size() const { return handles_.size(); }
  void clear() { handles_.clear(); }

  // kOk when a message is waiting, kWouldBlock when the queue is empty,
  // kStopped when the subscriber has been stopped.
  Status observe(size_t index) const {
    if (index >= handles_.size()) return Status::kInvalidArgument;
    if (!handles_[index].running->load(std::memory_order_acquire)) return Status::kStopped;
    if (handles_[index].ready(handles_[index].source)) return Status::kOk;
    return Status::kWouldBlock;
  }

  // On kOk, *ready_index names a subscriber in insertion order.
  Status wait(std::chrono::milliseconds timeout, size_t* ready_index) {
    return wait_masked(timeout, ready_index, &skip_none);
  }

  // `skip(i)` leaves that subscriber out of the poll and the futex wait.
  // An empty selection returns kWouldBlock without sleeping.
  template <class Skip>
  Status wait_masked(std::chrono::milliseconds timeout, size_t* ready_index, Skip skip) {
    if (timeout.count() < 0 || !ready_index || handles_.empty()) return Status::kInvalidArgument;
    if (!supported()) return Status::kNotSupported;
    const uint64_t deadline =
        detail::monotonic_ns() + static_cast<uint64_t>(timeout.count()) * 1000000ULL;
    std::vector<detail::FutexWaitv> waits;
    std::vector<size_t> armed;
    waits.reserve(handles_.size());
    armed.reserve(handles_.size());
    for (;;) {
      Status status = poll_masked(ready_index, skip);
      if (status != Status::kWouldBlock) return status;
      size_t selected = 0;
      for (size_t i = 0; i < handles_.size(); ++i) {
        if (!skip(i)) ++selected;
      }
      if (selected == 0) return Status::kWouldBlock;
      armed.clear();
      for (size_t i = 0; i < handles_.size(); ++i) {
        if (skip(i)) continue;
        handles_[i].waiters->fetch_add(1, std::memory_order_seq_cst);
        armed.push_back(i);
      }
      std::atomic_thread_fence(std::memory_order_seq_cst);
      waits.clear();
      for (size_t n = 0; n < armed.size(); ++n) {
        const detail::WaitHandle& handle = handles_[armed[n]];
        detail::FutexWaitv word;
        word.val = handle.generation->load(std::memory_order_acquire);
        word.uaddr = reinterpret_cast<uint64_t>(detail::futex_address(handle.generation));
        word.flags = detail::kFutex2SizeU32;
        word.reserved = 0;
        waits.push_back(word);
      }
      status = poll_masked(ready_index, skip);
      if (status == Status::kWouldBlock && detail::monotonic_ns() >= deadline) {
        status = Status::kTimedOut;
      }
      if (status == Status::kWouldBlock && !waits.empty()) {
        const timespec abs = detail::to_timespec(deadline);
        const long rc = syscall(detail::kSysFutexWaitv, waits.data(),
                                static_cast<unsigned>(waits.size()), 0, &abs, CLOCK_MONOTONIC);
        if (rc == -1 && errno != EAGAIN && errno != EINTR && errno != ETIMEDOUT) {
          status = Status::kNotSupported;
        }
      }
      for (size_t n = 0; n < armed.size(); ++n) {
        handles_[armed[n]].waiters->fetch_sub(1, std::memory_order_release);
      }
      if (status != Status::kWouldBlock) return status;
    }
  }

 private:
  static bool skip_none(size_t) { return false; }

  template <class Skip>
  Status poll_masked(size_t* ready_index, Skip skip) const {
    for (size_t i = 0; i < handles_.size(); ++i) {
      if (skip(i)) continue;
      if (!handles_[i].running->load(std::memory_order_acquire)) {
        *ready_index = i;
        return Status::kStopped;
      }
    }
    for (size_t i = 0; i < handles_.size(); ++i) {
      if (skip(i)) continue;
      if (handles_[i].ready(handles_[i].source)) {
        *ready_index = i;
        return Status::kOk;
      }
    }
    return Status::kWouldBlock;
  }

  std::vector<detail::WaitHandle> handles_;
};

}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_WAIT_SET_HPP_
