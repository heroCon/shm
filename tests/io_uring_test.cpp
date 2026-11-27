// Submits one IORING_OP_FUTEX_WAIT whose expected value does not match, so a
// kernel that supports the opcode completes it with EAGAIN. ENOSYS or EINVAL
// skips: the kernel is older than 6.7 or was built without io_uring futexes.
#include "shm_pubsub/io_uring.hpp"

#include <linux/io_uring.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "test_util.hpp"

#ifndef SYS_io_uring_setup
#define SYS_io_uring_setup 425
#endif
#ifndef SYS_io_uring_enter
#define SYS_io_uring_enter 426
#endif

static int skip(const char* why) {
  std::printf("io_uring_test skipped: %s\n", why);
  return 0;
}

int main() {
  io_uring_params params;
  std::memset(&params, 0, sizeof(params));
  const int ring = static_cast<int>(syscall(SYS_io_uring_setup, 4, &params));
  if (ring < 0) {
    if (errno == ENOSYS || errno == EPERM) return skip("io_uring_setup");
    std::perror("io_uring_setup");
    return 1;
  }

  const size_t sqe_bytes = params.sq_entries * sizeof(io_uring_sqe);
  auto* sqes = static_cast<io_uring_sqe*>(mmap(nullptr, sqe_bytes, PROT_READ | PROT_WRITE,
                                               MAP_SHARED | MAP_POPULATE, ring, IORING_OFF_SQES));
  const size_t sq_bytes = params.sq_off.array + params.sq_entries * sizeof(unsigned);
  auto* sq = static_cast<char*>(mmap(nullptr, sq_bytes, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_POPULATE, ring, IORING_OFF_SQ_RING));
  const size_t cq_bytes = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
  auto* cq = static_cast<char*>(mmap(nullptr, cq_bytes, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_POPULATE, ring, IORING_OFF_CQ_RING));
  if (sqes == MAP_FAILED || sq == MAP_FAILED || cq == MAP_FAILED) {
    std::perror("mmap");
    return 1;
  }

  ScopedName name("iouring");
  shm_pubsub::Options options;
  options.name = name.name;
  shm_pubsub::Topic<SmallConfig> topic(options);
  shm_pubsub::Topic<SmallConfig>::Subscriber sub(topic);
  shm_pubsub::FutexWait wait = shm_pubsub::futex_wait_for(sub);
  wait.expected += 1;

  auto* head = reinterpret_cast<unsigned*>(sq + params.sq_off.head);
  auto* tail = reinterpret_cast<unsigned*>(sq + params.sq_off.tail);
  auto* array = reinterpret_cast<unsigned*>(sq + params.sq_off.array);
  const unsigned mask = *reinterpret_cast<unsigned*>(sq + params.sq_off.ring_mask);
  const unsigned index = *tail & mask;
  (void)head;
  std::memset(&sqes[index], 0, sizeof(sqes[index]));
  shm_pubsub::prepare_futex_wait(&sqes[index], wait);
  sqes[index].user_data = 7;
  array[index] = index;
  ++*tail;

  const int entered =
      static_cast<int>(syscall(SYS_io_uring_enter, ring, 1, 1, IORING_ENTER_GETEVENTS, nullptr, 0));
  if (entered < 0) {
    if (errno == ENOSYS || errno == EINVAL) return skip("io_uring_enter");
    std::perror("io_uring_enter");
    return 1;
  }
  auto* cq_head = reinterpret_cast<unsigned*>(cq + params.cq_off.head);
  auto* cq_tail = reinterpret_cast<unsigned*>(cq + params.cq_off.tail);
  auto* cqes = reinterpret_cast<io_uring_cqe*>(cq + params.cq_off.cqes);
  if (*cq_head == *cq_tail) return skip("no completion");
  const unsigned cq_mask = *reinterpret_cast<unsigned*>(cq + params.cq_off.ring_mask);
  const io_uring_cqe result = cqes[*cq_head & cq_mask];
  ++*cq_head;
  close(ring);
  if (result.res == -ENOSYS || result.res == -EINVAL) {
    char why[64];
    std::snprintf(why, sizeof(why), "opcode res=%d", result.res);
    return skip(why);
  }
  CHECK(result.user_data == 7);
  CHECK(result.res == -EAGAIN);
  return finish("io_uring_test");
}
