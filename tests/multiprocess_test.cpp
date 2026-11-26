#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>

#include "test_util.hpp"

using shm_pubsub::Status;
using Topic = shm_pubsub::Topic<SmallConfig>;

static void kill_and_reap(pid_t child) {
  kill(child, SIGKILL);
  waitpid(child, nullptr, 0);
}

// A blocked subscriber in another process is woken by a publish.
static void check_cross_process_wakeup(const shm_pubsub::Options& o) {
  int ready[2];
  CHECK(pipe(ready) == 0);
  const pid_t child = fork();
  if (child == 0) {
    try {
      Topic topic(o);
      Topic::Subscriber sub(topic);
      char x = 1;
      if (write(ready[1], &x, 1) != 1) _exit(5);
      char out[16];
      const auto start = std::chrono::steady_clock::now();
      const auto r = sub.wait_receive(out, sizeof(out), std::chrono::seconds(5));
      const bool woke = std::chrono::steady_clock::now() - start < std::chrono::seconds(4);
      _exit(woke && r.status == Status::kOk && r.size == 6 && std::memcmp(out, "hello", 6) == 0
                ? 0
                : 2);
    } catch (...) {
      _exit(4);
    }
  }
  Topic topic(o);
  Topic::Publisher pub(topic);
  char x;
  CHECK(read(ready[0], &x, 1) == 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const auto r = pub.publish_copy("hello", 6);
  CHECK(r.status == Status::kOk && r.delivered == 1);
  int status = 0;
  waitpid(child, &status, 0);
  CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  close(ready[0]);
  close(ready[1]);
}

// A stopped reader keeps its slot; a killed one is reclaimed with its queue.
static void check_reader_reclaim(const shm_pubsub::Options& o) {
  const pid_t child = fork();
  if (child == 0) {
    try {
      Topic topic(o);
      Topic::Subscriber sub(topic);
      raise(SIGSTOP);
      _exit(2);
    } catch (...) {
      _exit(3);
    }
  }
  int status = 0;
  CHECK(waitpid(child, &status, WUNTRACED) == child && WIFSTOPPED(status));
  Topic topic(o);
  Topic::Publisher pub(topic);
  CHECK(topic.collect_garbage() == 0 && topic.subscriber_count() == 1);
  for (uint32_t i = 0; i < Topic::kQueueCapacity; ++i) {
    CHECK(pub.publish_copy("x", 1).status == Status::kOk);
  }
  CHECK(pub.publish_copy("x", 1).status == Status::kQueueFull);
  CHECK(free_blocks(topic) == Topic::kBlockCount - Topic::kQueueCapacity);
  kill_and_reap(child);
  CHECK(topic.collect_garbage() == 1);
  CHECK(topic.subscriber_count() == 0);
  CHECK(free_blocks(topic) == Topic::kBlockCount);
  Topic::Subscriber replacement(topic);
  Topic::ReadLoan read;
  CHECK(replacement.take(read) == Status::kWouldBlock);
  CHECK(pub.publish_copy("y", 1).status == Status::kOk);
  CHECK(replacement.take(read) == Status::kOk && read.data()[0] == 'y');
}

// Registration after a crash reclaims the slot it lands on.
static void check_slot_reuse(const shm_pubsub::Options& o) {
  for (uint32_t round = 0; round < Topic::kMaxSubscribers * 2; ++round) {
    const pid_t child = fork();
    if (child == 0) {
      try {
        Topic topic(o);
        Topic::Subscriber sub(topic);
        raise(SIGKILL);
      } catch (...) {
      }
      _exit(3);
    }
    waitpid(child, nullptr, 0);
  }
  Topic topic(o);
  Topic::Subscriber sub(topic);
  topic.collect_garbage();
  CHECK(topic.subscriber_count() == 1);
  CHECK(free_blocks(topic) == Topic::kBlockCount);
}

int main() {
  ScopedName name("multiprocess");
  shm_pubsub::Options o;
  o.name = name.name;
  check_cross_process_wakeup(o);
  check_reader_reclaim(o);
  check_slot_reuse(o);
  return finish("multiprocess_test");
}
