#ifndef SHM_PUBSUB_EVENTFD_BRIDGE_HPP_
#define SHM_PUBSUB_EVENTFD_BRIDGE_HPP_

#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <system_error>
#include <thread>
#include <vector>

#include "shm_pubsub/wait_set.hpp"

namespace shm_pubsub {

// A thread that waits on a WaitSet and writes to an eventfd, so an epoll or
// libuv loop can wake on subscribers without depending on futex_waitv itself.
//
// The eventfd is an edge-triggered semaphore. A subscriber produces one tick
// when it becomes ready, and another only after it has been observed empty
// again, so an unread message does not spin the counter. The caller should
// drain that subscriber after each tick. A stopped subscriber produces one
// tick and is then dropped; when none remain, the thread exits.
class EventfdBridge {
 public:
  EventfdBridge() : fd_(eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE | EFD_NONBLOCK)) {
    if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  ~EventfdBridge() {
    stop();
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

  EventfdBridge(const EventfdBridge&) = delete;
  EventfdBridge& operator=(const EventfdBridge&) = delete;

  template <class Subscriber>
  bool add(Subscriber& subscriber) {
    if (thread_.joinable()) return false;
    return set_.add(subscriber);
  }

  int fd() const { return fd_; }

  // Spawns the waiter. kNotSupported when the kernel has no futex_waitv.
  Status start() {
    if (thread_.joinable()) return Status::kInvalidArgument;
    if (!WaitSet::supported()) return Status::kNotSupported;
    if (set_.size() == 0) return Status::kInvalidArgument;
    stop_.store(false);
    thread_ = std::thread(&EventfdBridge::run, this);
    return Status::kOk;
  }

  void stop() {
    stop_.store(true);
    if (thread_.joinable()) thread_.join();
  }

 private:
  // 0: idle or not yet reported, 1: ready and already signaled, 2: dropped.
  void run() {
    const size_t count = set_.size();
    std::vector<unsigned char> state(count, 0);
    while (!stop_.load(std::memory_order_acquire)) {
      bool need_wait = false;
      bool any_live = false;
      for (size_t i = 0; i < count; ++i) {
        if (state[i] == 2) continue;
        any_live = true;
        const Status status = set_.observe(i);
        if (status == Status::kStopped) {
          signal();
          state[i] = 2;
        } else if (status == Status::kOk) {
          if (state[i] == 0) signal();
          state[i] = 1;
        } else {
          state[i] = 0;
          need_wait = true;
        }
      }
      if (!any_live || stop_.load(std::memory_order_acquire)) break;
      if (!need_wait) {
        // Every live subscriber is still ready and already signaled. Sleep
        // until the consumer drains one; there is no futex for "not ready".
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      size_t index = 0;
      const Status status = set_.wait_masked(std::chrono::milliseconds(20), &index,
                                             [&state](size_t i) { return state[i] != 0; });
      (void)index;
      if (status == Status::kNotSupported || status == Status::kInvalidArgument) break;
    }
  }

  void signal() {
    const uint64_t one = 1;
    if (write(fd_, &one, sizeof(one)) < 0 && errno != EAGAIN) {
      // The counter is saturated; the reader is already behind.
    }
  }

  WaitSet set_;
  int fd_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_EVENTFD_BRIDGE_HPP_
