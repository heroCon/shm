// Submits one IORING_OP_FUTEX_WAIT whose expected value does not match, so a
// kernel that supports the opcode completes it with EAGAIN. ENOSYS or EINVAL
// skips: the kernel is older than 6.7 or was built without io_uring futexes.
#include "shm_pubsub/io_uring.hpp"

#include <linux/io_uring.h>
#include <linux/time_types.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
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

  auto* sq_head = reinterpret_cast<unsigned*>(sq + params.sq_off.head);
  auto* sq_tail = reinterpret_cast<unsigned*>(sq + params.sq_off.tail);
  auto* array = reinterpret_cast<unsigned*>(sq + params.sq_off.array);
  const unsigned sq_mask = *reinterpret_cast<unsigned*>(sq + params.sq_off.ring_mask);
  auto* cq_head = reinterpret_cast<unsigned*>(cq + params.cq_off.head);
  auto* cq_tail = reinterpret_cast<unsigned*>(cq + params.cq_off.tail);
  auto* cqes = reinterpret_cast<io_uring_cqe*>(cq + params.cq_off.cqes);
  const unsigned cq_mask = *reinterpret_cast<unsigned*>(cq + params.cq_off.ring_mask);
  (void)sq_head;

  auto submit = [&](unsigned count, unsigned complete) -> int {
    return static_cast<int>(syscall(SYS_io_uring_enter, ring, count, complete,
                                    complete ? IORING_ENTER_GETEVENTS : 0, nullptr, 0));
  };
  auto push = [&](const io_uring_sqe& sqe) {
    const unsigned index = *sq_tail & sq_mask;
    sqes[index] = sqe;
    array[index] = index;
    ++*sq_tail;
  };
  auto pop = [&]() -> io_uring_cqe {
    const io_uring_cqe result = cqes[*cq_head & cq_mask];
    ++*cq_head;
    return result;
  };

  ScopedName name("iouring");
  shm_pubsub::Options options;
  options.name = name.name;
  shm_pubsub::Topic<SmallConfig> topic(options);
  shm_pubsub::Topic<SmallConfig>::Subscriber sub(topic);
  bool should_wait = false;
  shm_pubsub::FutexWait wait = shm_pubsub::arm_futex_wait(sub, &should_wait);
  CHECK(should_wait);
  CHECK(wait.armed);
  const uint64_t armed_generation = wait.expected;
  wait.expected += 1;

  io_uring_sqe probe;
  std::memset(&probe, 0, sizeof(probe));
  shm_pubsub::prepare_futex_wait(&probe, wait);
  probe.user_data = 7;
  push(probe);
  const int entered = submit(1, 1);
  shm_pubsub::disarm_futex_wait(&wait);
  CHECK(!wait.armed);
  CHECK(sub.wait_handle().waiters->load() == 0);
  if (entered < 0) {
    if (errno == ENOSYS || errno == EINVAL) return skip("io_uring_enter");
    std::perror("io_uring_enter");
    return 1;
  }
  if (*cq_head == *cq_tail) return skip("no completion");
  const io_uring_cqe result = pop();
  if (result.res == -ENOSYS || result.res == -EINVAL) {
    char why[64];
    std::snprintf(why, sizeof(why), "opcode res=%d", result.res);
    return skip(why);
  }
  CHECK(result.user_data == 7);
  CHECK(result.res == -EAGAIN);

  // A matching wait stays armed across the submit. Publishing must complete
  // it: either the wake returns 0, or the generation moved first and the
  // opcode returns EAGAIN. Without the waiter count, wake() does neither and
  // the linked timeout fires instead.
  wait = shm_pubsub::arm_futex_wait(sub, &should_wait);
  CHECK(should_wait);
  CHECK(wait.expected == armed_generation);
  __kernel_timespec budget;
  budget.tv_sec = 0;
  budget.tv_nsec = 500000000L;
  io_uring_sqe linked;
  std::memset(&linked, 0, sizeof(linked));
  shm_pubsub::prepare_futex_wait(&linked, wait);
  linked.flags = IOSQE_IO_LINK;
  linked.user_data = 8;
  push(linked);
  io_uring_sqe timeout;
  std::memset(&timeout, 0, sizeof(timeout));
  timeout.opcode = IORING_OP_LINK_TIMEOUT;
  timeout.addr = reinterpret_cast<uint64_t>(&budget);
  timeout.len = 1;
  timeout.user_data = 9;
  push(timeout);
  CHECK(submit(2, 0) >= 0);
  shm_pubsub::Topic<SmallConfig>::Publisher pub(topic);
  char payload = 'z';
  CHECK(pub.publish_copy(&payload, 1).status == shm_pubsub::Status::kOk);
  CHECK(submit(0, 2) >= 0);
  bool saw_wait = false;
  int wait_res = 0;
  while (*cq_head != *cq_tail) {
    const io_uring_cqe done = pop();
    if (done.user_data == 8) {
      saw_wait = true;
      wait_res = done.res;
    }
  }
  shm_pubsub::disarm_futex_wait(&wait);
  CHECK(saw_wait);
  CHECK(wait_res == 0 || wait_res == -EAGAIN);
  shm_pubsub::Topic<SmallConfig>::ReadLoan loan;
  CHECK(sub.take(loan) == shm_pubsub::Status::kOk);
  CHECK(loan.size() == 1 && static_cast<const char*>(loan.data())[0] == 'z');
  CHECK(sub.wait_handle().waiters->load() == 0);
  close(ring);
  return finish("io_uring_test");
}
