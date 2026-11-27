// Kills a process at every fault-injection point and verifies that the
// surviving processes recover every block and keep exchanging messages.
// Built with SHM_PUBSUB_FAULT_INJECTION.
#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "test_util.hpp"

using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

static void drain(Topic::Subscriber& sub) {
  Topic::ReadLoan loan;
  while (sub.take(loan) == Status::kOk) loan.reset();
}

static void crash_reserve(const shm_pubsub::Options& o);
static void crash_claim(const shm_pubsub::Options& o);
static void crash_hold(const shm_pubsub::Options& o);

// Exercises every point: loans, fan-out to private and group queues, drops
// under kDropOldest, takes, releases and the reclamation of a dead reader.
[[noreturn]] static void victim(const shm_pubsub::Options& o) {
  Topic topic(o);
  Topic::Publisher pub(topic);
  SubscriberOptions dropping;
  dropping.overflow = Overflow::kDropOldest;
  Topic::Subscriber never_reads(topic, dropping);
  Topic::Subscriber reader(topic);
  SubscriberOptions worker;
  worker.group = 1;
  Topic::Subscriber group_member(topic, worker);
  for (uint32_t i = 0; i < 3 * Topic::kQueueCapacity; ++i) {
    Topic::WriteLoan loan;
    if (pub.loan(8, loan) != Status::kOk) _exit(10);
    std::memcpy(loan.data(), &i, sizeof(i));
    pub.publish(std::move(loan));
    drain(reader);
    drain(group_member);
  }
  for (int round = 0; round < 5; ++round) {
    int ready[2];
    if (pipe(ready) != 0) _exit(11);
    const pid_t grandchild = fork();
    if (grandchild == 0) {
      prctl(PR_SET_PDEATHSIG, SIGKILL);
      Topic inner(o);
      Topic::Subscriber doomed(inner);
      char x = 1;
      if (write(ready[1], &x, 1) != 1) _exit(12);
      pause();
      _exit(0);
    }
    char x;
    if (read(ready[0], &x, 1) != 1) _exit(13);
    for (int i = 0; i < 3; ++i) pub.publish_copy("d", 1);
    kill(grandchild, SIGKILL);
    waitpid(grandchild, nullptr, 0);
    close(ready[0]);
    close(ready[1]);
    topic.collect_garbage();
  }

  // Points the loop above never reaches. Five of each, so hit counts 1, 2, 3
  // and 5 all land. Children override SHM_PUBSUB_CRASH_AT; this process keeps
  // its own, so a repair point fires here.
  for (int n = 0; n < 5; ++n) {
    Topic::Subscriber temp(topic);
  }
  auto child = [&](const char* spec, void (*fn)(const shm_pubsub::Options&)) {
    const pid_t pid = fork();
    if (pid == 0) {
      setenv("SHM_PUBSUB_CRASH_AT", spec, 1);
      fn(o);
      _exit(0);
    }
    waitpid(pid, nullptr, 0);
  };
  for (int n = 0; n < 5; ++n) {
    child("queue.after_reserve:1", crash_reserve);
    topic.collect_garbage();
    child("publish.after_ref:1", crash_reserve);
    topic.collect_garbage();
    child("take.after_claim:1", crash_claim);
    topic.collect_garbage();
    child("take.after_convert:1", crash_hold);
    topic.collect_garbage();
  }
  _exit(0);
}

static void crash_reserve(const shm_pubsub::Options& o) {
  Topic topic(o);
  Topic::Publisher pub(topic);
  Topic::Subscriber sub(topic);
  for (int i = 0; i < 8; ++i) pub.publish_copy("h", 1);
  _exit(0);
}

static void crash_claim(const shm_pubsub::Options& o) {
  Topic topic(o);
  Topic::Publisher pub(topic);
  SubscriberOptions same;
  same.group = 1;
  Topic::Subscriber sub(topic, same);
  for (;;) {
    pub.publish_copy("c", 1);
    Topic::ReadLoan loan;
    sub.take(loan);
  }
}

static void crash_hold(const shm_pubsub::Options& o) {
  Topic topic(o);
  SubscriberOptions alo;
  alo.group = 2;
  alo.delivery = shm_pubsub::Delivery::kAtLeastOnce;
  Topic::Subscriber sub(topic, alo);
  Topic::Publisher pub(topic);
  for (;;) {
    pub.publish_copy("a", 1);
    Topic::ReadLoan loan;
    sub.take(loan);
  }
}

static bool recover_fully(Topic& topic, shm_pubsub::RecoveryReport& report) {
  for (int attempt = 0; attempt < 100; ++attempt) {
    const Status s = topic.recover(&report);
    if (s == Status::kOk) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

// With garbage_only, the survivors call only collect_garbage(); a later
// recover() must then find nothing left to repair.
static bool run_point(const char* point, int hit, bool garbage_only) {
  ScopedName name("crash_matrix");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Topic::Subscriber survivor(topic);
  SubscriberOptions worker;
  worker.group = 1;
  Topic::Subscriber survivor_worker(topic, worker);

  const std::string spec = std::string(point) + ":" + std::to_string(hit);
  const pid_t child = fork();
  if (child == 0) {
    setenv("SHM_PUBSUB_CRASH_AT", spec.c_str(), 1);
    victim(o);
  }
  int status = 0;
  waitpid(child, &status, 0);
  const bool crashed = WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
  if (!crashed) {
    std::fprintf(stderr, "%s: point not reached (status %d)\n", spec.c_str(), status);
    return false;
  }

  drain(survivor);
  drain(survivor_worker);
  shm_pubsub::RecoveryReport report;
  bool recovered;
  if (garbage_only) {
    recovered = topic.collect_garbage() >= 1;
    shm_pubsub::RecoveryReport leftover;
    recovered &= recover_fully(topic, leftover) && leftover.orphans_freed == 0 &&
                 leftover.states_repaired == 0 && leftover.holes_filled == 0 &&
                 leftover.cells_released == 0 && leftover.pins_cleared == 0 &&
                 leftover.slots_reclaimed == 0;
  } else {
    recovered = recover_fully(topic, report);
  }
  drain(survivor);
  drain(survivor_worker);
  const uint32_t free_after = free_blocks(topic);

  bool works = true;
  {
    Topic::Publisher pub(topic);
    for (uint32_t i = 0; i < 2 * Topic::kQueueCapacity; ++i) {
      works &= pub.publish_copy("ok", 3).status == Status::kOk;
      Topic::ReadLoan a, b;
      works &= survivor.take(a) == Status::kOk && a.size() == 3;
      works &= survivor_worker.take(b) == Status::kOk && b.size() == 3;
    }
  }
  const bool conserved =
      free_blocks(topic) == Topic::kBlockCount && free_after == Topic::kBlockCount;
  if (garbage_only) {
    std::printf("%-34s collect_garbage only: recovered=%d free=%u/%u works=%d\n", spec.c_str(),
                recovered, free_after, Topic::kBlockCount, works);
    return recovered && conserved && works;
  }
  std::printf(
      "%-34s recovered=%d free=%u/%u works=%d reclaimed=%u orphans=%u repaired=%u "
      "holes=%u cells=%u pins=%u\n",
      spec.c_str(), recovered, free_after, Topic::kBlockCount, works, report.slots_reclaimed,
      report.orphans_freed, report.states_repaired, report.holes_filled, report.cells_released,
      report.pins_cleared);
  return recovered && conserved && works && report.inconsistencies == 0;
}

int main() {
  int failures = 0;
  for (size_t p = 0; p < shm_pubsub::detail::kFaultPointCount; ++p) {
    const int hits[] = {1, 2, 3, 5};
    for (int hit : hits) {
      if (!run_point(shm_pubsub::detail::kFaultPoints[p], hit, false)) ++failures;
    }
    if (!run_point(shm_pubsub::detail::kFaultPoints[p], 1, true)) ++failures;
  }
  if (failures) std::fprintf(stderr, "crash_matrix_test: %d failing point(s)\n", failures);
  return failures ? 1 : 0;
}
