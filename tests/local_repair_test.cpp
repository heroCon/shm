// Kills an endpoint inside a queue operation while other endpoints keep
// running, and checks that collect_garbage() alone unblocks the readers and
// returns every block, without the quiescent window recover() needs.
// Built with SHM_PUBSUB_FAULT_INJECTION.
#include <signal.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "test_util.hpp"

using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

namespace {

struct Shared {
  std::atomic<long> received{0};
  std::atomic<int> ready{0};
  std::atomic<int> stop{0};
};

Shared* make_shared() {
  void* p =
      mmap(nullptr, sizeof(Shared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  return new (p) Shared();
}

// A reader that never sleeps and never calls recover().
pid_t spawn_reader(const shm_pubsub::Options& o, Shared* g, uint32_t group) {
  const pid_t pid = fork();
  if (pid == 0) {
    prctl(PR_SET_PDEATHSIG, SIGKILL);
    Topic inner(o);
    SubscriberOptions so;
    so.group = group;
    Topic::Subscriber sub(inner, so);
    g->ready.fetch_add(1);
    while (!g->stop.load()) {
      Topic::ReadLoan loan;
      if (sub.take(loan) == Status::kOk) g->received.fetch_add(1);
    }
    _exit(0);
  }
  return pid;
}

bool killed_at(const char* spec, void (*body)(const shm_pubsub::Options&),
               const shm_pubsub::Options& o) {
  const pid_t pid = fork();
  if (pid == 0) {
    setenv("SHM_PUBSUB_CRASH_AT", spec, 1);
    body(o);
    _exit(0);
  }
  int st = 0;
  waitpid(pid, &st, 0);
  return WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL;
}

void publish_some(const shm_pubsub::Options& o) {
  Topic topic(o);
  Topic::Publisher pub(topic);
  for (int i = 0; i < 64; ++i) {
    pub.publish_copy("x", 1);
    usleep(200);
  }
}

// After the victim died: collect_garbage(), then a healthy publisher must
// reach the busy reader(s), and stopping everything must return all blocks.
void check_repair(const char* label, Topic& topic, Shared* g, pid_t* readers, int n,
                  bool expect_local) {
  shm_pubsub::RecoveryReport report;
  const size_t reclaimed = topic.collect_garbage(&report);
  CHECK(reclaimed >= 1);
  if (expect_local) CHECK(!report.full_repair);
  const long before = g->received.load();
  {
    Topic::Publisher pub(topic);
    for (int i = 0; i < 200; ++i) {
      while (pub.publish_copy("y", 1).status != Status::kOk) std::this_thread::yield();
    }
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (g->received.load() - before < 200 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const long got = g->received.load() - before;
  CHECK(got >= 200);
  g->stop.store(1);
  for (int k = 0; k < n; ++k) waitpid(readers[k], nullptr, 0);
  CHECK(free_blocks(topic) == Topic::kBlockCount);
  shm_pubsub::RecoveryReport leftover;
  CHECK(topic.recover(&leftover) == Status::kOk);
  CHECK(leftover.orphans_freed == 0u);
  CHECK(leftover.states_repaired == 0u);
  CHECK(leftover.holes_filled == 0u);
  CHECK(leftover.cells_released == 0u);
  std::printf("%-28s reclaimed=%zu holes=%u cells=%u recounted=%u full_repair=%d received=%ld\n",
              label, reclaimed, report.holes_filled, report.cells_released, report.blocks_recounted,
              report.full_repair, got);
}

void hole_in_private_queue() {
  ScopedName name("local_hole");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Shared* g = make_shared();
  pid_t reader = spawn_reader(o, g, 0);
  while (g->ready.load() < 1) std::this_thread::yield();
  CHECK(killed_at("queue.after_reserve:5", publish_some, o));
  check_repair("hole (private queue)", topic, g, &reader, 1, true);
}

void hole_in_group_queue() {
  ScopedName name("local_group_hole");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Shared* g = make_shared();
  pid_t readers[2] = {spawn_reader(o, g, 1), spawn_reader(o, g, 1)};
  while (g->ready.load() < 2) std::this_thread::yield();
  CHECK(killed_at("queue.after_reserve:5", publish_some, o));
  check_repair("hole (group queue)", topic, g, readers, 2, true);
}

void claimed_by_dead_member() {
  ScopedName name("local_claim");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Shared* g = make_shared();
  pid_t reader = spawn_reader(o, g, 1);
  while (g->ready.load() < 1) std::this_thread::yield();
  // The victim claims a cell and dies before converting it.
  {
    const pid_t pid = fork();
    if (pid == 0) {
      setenv("SHM_PUBSUB_CRASH_AT", "take.after_claim:1", 1);
      Topic inner(o);
      SubscriberOptions so;
      so.group = 1;
      Topic::Subscriber sub(inner, so);
      Topic::Publisher pub(inner);
      for (;;) {
        pub.publish_copy("z", 1);
        Topic::ReadLoan loan;
        sub.take(loan);
      }
    }
    int st = 0;
    waitpid(pid, &st, 0);
    CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL);
  }
  check_repair("claimed cell (group)", topic, g, &reader, 1, true);
}

void converted_by_dead_member() {
  ScopedName name("local_convert");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Shared* g = make_shared();
  pid_t reader = spawn_reader(o, g, 1);
  while (g->ready.load() < 1) std::this_thread::yield();
  {
    const pid_t pid = fork();
    if (pid == 0) {
      setenv("SHM_PUBSUB_CRASH_AT", "take.after_convert:1", 1);
      Topic inner(o);
      SubscriberOptions so;
      so.group = 1;
      Topic::Subscriber sub(inner, so);
      Topic::Publisher pub(inner);
      for (;;) {
        pub.publish_copy("z", 1);
        Topic::ReadLoan loan;
        sub.take(loan);
      }
    }
    int st = 0;
    waitpid(pid, &st, 0);
    CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL);
  }
  check_repair("converted cell (group)", topic, g, &reader, 1, true);
}

void dead_publisher_mid_fanout() {
  ScopedName name("local_fanout");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Shared* g = make_shared();
  pid_t readers[2] = {spawn_reader(o, g, 0), spawn_reader(o, g, 0)};
  while (g->ready.load() < 2) std::this_thread::yield();
  CHECK(killed_at("publish.after_ref:7", publish_some, o));
  check_repair("publisher mid fan-out", topic, g, readers, 2, true);
}

}  // namespace

int main() {
  hole_in_private_queue();
  hole_in_group_queue();
  claimed_by_dead_member();
  converted_by_dead_member();
  dead_publisher_mid_fanout();
  return finish("local_repair_test");
}
