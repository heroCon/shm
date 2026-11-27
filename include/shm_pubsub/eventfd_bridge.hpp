#ifndef SHM_PUBSUB_EVENTFD_BRIDGE_HPP_
#define SHM_PUBSUB_EVENTFD_BRIDGE_HPP_

#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <system_error>
#include <thread>

#include "shm_pubsub/wait_set.hpp"

namespace shm_pubsub {

// A thread that waits on a WaitSet and writes to an eventfd, so an epoll or
// libuv loop can wake on subscribers without depending on futex_waitv itself.
// The eventfd is a semaphore: one successful wait produces one read.
class EventfdBridge {
 public:
  EventfdBridge() : fd_(eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE | EFD_NONBLOCK)) {
    if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  ~EventfdBridge() { stop(); }

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
  void run() {
    while (!stop_.load(std::memory_order_acquire)) {
      size_t index = 0;
      const Status status = set_.wait(std::chrono::milliseconds(20), &index);
      if (status == Status::kOk || status == Status::kStopped) signal();
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
