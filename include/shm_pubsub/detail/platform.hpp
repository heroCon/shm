#ifndef SHM_PUBSUB_DETAIL_PLATFORM_HPP_
#define SHM_PUBSUB_DETAIL_PLATFORM_HPP_

#include <fcntl.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>

namespace shm_pubsub {
namespace detail {

static_assert(sizeof(std::atomic<uint32_t>) == sizeof(uint32_t),
              "futex words must be plain 32-bit integers");

inline uint32_t* futex_address(std::atomic<uint32_t>* word) {
  return reinterpret_cast<uint32_t*>(word);
}

inline long futex_wait(std::atomic<uint32_t>* word, uint32_t expected, const timespec* relative) {
  return syscall(SYS_futex, futex_address(word), FUTEX_WAIT, expected, relative, nullptr, 0);
}

inline void futex_wake(std::atomic<uint32_t>* word, int count) {
  syscall(SYS_futex, futex_address(word), FUTEX_WAKE, count, nullptr, nullptr, 0);
}

#if defined(SYS_futex_waitv)
constexpr long kSysFutexWaitv = SYS_futex_waitv;
#elif defined(__x86_64__) || defined(__aarch64__) || defined(__i386__) || defined(__riscv)
constexpr long kSysFutexWaitv = 449;
#else
constexpr long kSysFutexWaitv = -1;
#endif

struct FutexWaitv {
  uint64_t val;
  uint64_t uaddr;
  uint32_t flags;
  uint32_t reserved;
};
constexpr uint32_t kFutex2SizeU32 = 0x02;
constexpr unsigned kFutexWaitvMax = 128;

inline bool futex_waitv_supported() {
  static const bool supported = []() -> bool {
    if (kSysFutexWaitv < 0) return false;
    const long rc = syscall(kSysFutexWaitv, nullptr, 0, 0, nullptr, CLOCK_MONOTONIC);
    return !(rc == -1 && (errno == ENOSYS || errno == EPERM));
  }();
  return supported;
}

inline uint64_t monotonic_ns() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

inline timespec to_timespec(uint64_t ns) {
  timespec ts;
  ts.tv_sec = static_cast<time_t>(ns / 1000000000ULL);
  ts.tv_nsec = static_cast<long>(ns % 1000000000ULL);
  return ts;
}

// Open-file-description locks on single bytes of the region file. They belong
// to a file description, not to a process, so every endpoint opens its own.
inline int ofd_lock(int fd, uint32_t byte, short type) {
  struct flock lock {};
  lock.l_type = type;
  lock.l_whence = SEEK_SET;
  lock.l_start = static_cast<off_t>(byte);
  lock.l_len = 1;
  int rc;
  do {
    rc = fcntl(fd, F_OFD_SETLK, &lock);
  } while (rc == -1 && errno == EINTR);
  return rc == 0 ? 0 : errno;
}

// True when a description other than fd's holds the byte. Errors count as
// held so that callers never reclaim a slot they could not check.
inline bool ofd_held_elsewhere(int fd, uint32_t byte) {
  struct flock lock {};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = static_cast<off_t>(byte);
  lock.l_len = 1;
  if (fcntl(fd, F_OFD_GETLK, &lock) != 0) return true;
  return lock.l_type != F_UNLCK;
}

}  // namespace detail
}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_DETAIL_PLATFORM_HPP_
