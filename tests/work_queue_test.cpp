// Work-queue semantics of consumer groups: durable backlogs, at-least-once
// redelivery after a member dies, requeue with delivery counts and
// dead-lettering, and publishers that block on a full group queue.
// Built with SHM_PUBSUB_FAULT_INJECTION.
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <thread>

#include "test_util.hpp"

using shm_pubsub::Delivery;
using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

namespace {

SubscriberOptions worker(Delivery delivery = Delivery::kAtMostOnce, uint32_t max = 0,
                         bool durable = false, Overflow overflow = Overflow::kDropNewest) {
  SubscriberOptions o;
  o.group = 1;
  o.delivery = delivery;
  o.max_deliveries = max;
  o.durable = durable;
  o.overflow = overflow;
  return o;
}

uint32_t value_of(const Topic::ReadLoan& loan) {
  uint32_t v = 0;
  std::memcpy(&v, loan.data(), sizeof(v));
  return v;
}

void publish_value(Topic::Publisher& pub, uint32_t v) {
  CHECK(pub.publish_copy(&v, sizeof(v)).status == Status::kOk);
}

const Topic::GroupInfo* group_info(const Topic::Snapshot& snap, uint32_t g) {
  for (const auto& info : snap.groups) {
    if (info.id == g) return &info;
  }
  return nullptr;
}

template <class F>
bool throws_invalid(F&& f) {
  try {
    f();
  } catch (const std::invalid_argument&) {
    return true;
  }
  return false;
}

void check_options() {
  ScopedName name("wq_options");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  SubscriberOptions bad;
  bad.overflow = Overflow::kBlock;
  CHECK(throws_invalid([&] { Topic::Subscriber s(topic, bad); }));
  bad = SubscriberOptions();
  bad.delivery = Delivery::kAtLeastOnce;
  CHECK(throws_invalid([&] { Topic::Subscriber s(topic, bad); }));
  CHECK(throws_invalid([&] { Topic::Subscriber s(topic, worker(Delivery::kAtMostOnce, 255)); }));
  Topic::Subscriber first(topic, worker(Delivery::kAtLeastOnce, 3, true));
  CHECK(throws_invalid([&] { Topic::Subscriber s(topic, worker(Delivery::kAtLeastOnce, 3)); }));
  CHECK(
      throws_invalid([&] { Topic::Subscriber s(topic, worker(Delivery::kAtMostOnce, 3, true)); }));
  Topic::Subscriber same(topic, worker(Delivery::kAtLeastOnce, 3, true));
  const auto snap = topic.snapshot();
  const auto* g = group_info(snap, 1);
  CHECK(g && g->durable && g->delivery == Delivery::kAtLeastOnce && g->max_deliveries == 3);
}

void check_durable_backlog() {
  ScopedName name("wq_durable");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Topic::Publisher pub(topic);
  {
    Topic::Subscriber w(topic, worker(Delivery::kAtMostOnce, 0, true));
    for (uint32_t v = 0; v < 3; ++v) publish_value(pub, v);
  }
  CHECK(free_blocks(topic) == Topic::kBlockCount - 3);
  Topic::Subscriber later(topic, worker(Delivery::kAtMostOnce, 0, true));
  for (uint32_t v = 0; v < 3; ++v) {
    Topic::ReadLoan loan;
    CHECK(later.take(loan) == Status::kOk);
    CHECK(loan && value_of(loan) == v && loan.delivery_count() == 1);
  }
  CHECK(free_blocks(topic) == Topic::kBlockCount);
}

void check_requeue() {
  ScopedName name("wq_requeue");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Topic::Publisher pub(topic);
  Topic::Subscriber w(topic, worker(Delivery::kAtLeastOnce, 2));
  publish_value(pub, 7);
  Topic::ReadLoan loan;
  CHECK(w.take(loan) == Status::kOk);
  CHECK(loan.delivery_count() == 1);
  CHECK(loan.requeue() == Status::kOk);
  CHECK(!loan);
  CHECK(w.take(loan) == Status::kOk);
  CHECK(value_of(loan) == 7 && loan.delivery_count() == 2);
  CHECK(loan.requeue() == Status::kDeadLettered);
  CHECK(!loan);
  CHECK(w.take(loan) == Status::kWouldBlock);
  const auto snap = topic.snapshot();
  const auto* g = group_info(snap, 1);
  CHECK(g && g->redelivered == 1 && g->dead_lettered == 1);
  CHECK(free_blocks(topic) == Topic::kBlockCount);

  Topic::Subscriber reader(topic);
  publish_value(pub, 8);
  Topic::ReadLoan mine;
  CHECK(reader.take(mine) == Status::kOk);
  CHECK(mine.requeue() == Status::kInvalidArgument);
  CHECK(mine && value_of(mine) == 8);
}

// A member killed while holding a loan (or between claiming and converting
// a cell) gives the message back to the group under at-least-once, and loses
// it under at-most-once.
void check_redelivery_after_death(Delivery delivery, const char* crash_at) {
  ScopedName name("wq_death");
  shm_pubsub::Options o;
  o.name = name.name;
  o.self_heal = false;
  Topic topic(o);
  Topic::Subscriber survivor(topic, worker(delivery));
  Topic::Publisher pub(topic);
  int ready[2];
  CHECK(pipe(ready) == 0);
  const pid_t child = fork();
  if (child == 0) {
    if (crash_at) setenv("SHM_PUBSUB_CRASH_AT", crash_at, 1);
    Topic inner(o);
    Topic::Subscriber doomed(inner, worker(delivery));
    char x = 1;
    if (write(ready[1], &x, 1) != 1) _exit(2);
    for (;;) {
      Topic::ReadLoan loan;
      if (doomed.take(loan) == Status::kOk) {
        if (write(ready[1], &x, 1) != 1) _exit(3);
        pause();
      }
    }
  }
  char x;
  CHECK(read(ready[0], &x, 1) == 1);
  survivor.unsubscribe();
  publish_value(pub, 42);
  if (crash_at) {
    int st = 0;
    waitpid(child, &st, 0);
    CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL);
  } else {
    CHECK(read(ready[0], &x, 1) == 1);
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
  }
  close(ready[0]);
  close(ready[1]);
  survivor.subscribe();
  shm_pubsub::RecoveryReport report;
  CHECK(topic.collect_garbage(&report) == 1);
  CHECK(!report.full_repair);
  Topic::ReadLoan loan;
  const Status s = survivor.take(loan);
  if (delivery == Delivery::kAtLeastOnce) {
    CHECK(s == Status::kOk);
    CHECK(loan && value_of(loan) == 42 && loan.delivery_count() == 2);
  } else {
    CHECK(s == Status::kWouldBlock);
  }
  loan.reset();
  CHECK(free_blocks(topic) == Topic::kBlockCount);
  std::printf("%-14s death %-18s redelivered=%d\n",
              delivery == Delivery::kAtLeastOnce ? "at-least-once" : "at-most-once",
              crash_at ? crash_at : "holding a loan", s == Status::kOk);
}

void check_block() {
  ScopedName name("wq_block");
  shm_pubsub::Options o;
  o.name = name.name;
  o.block_timeout = std::chrono::milliseconds(100);
  Topic topic(o);
  Topic::Publisher pub(topic);
  Topic::Subscriber w(topic, worker(Delivery::kAtMostOnce, 0, false, Overflow::kBlock));
  for (uint32_t v = 0; v < Topic::kQueueCapacity; ++v) publish_value(pub, v);

  // Nobody takes: the publish gives up after block_timeout.
  auto start = std::chrono::steady_clock::now();
  uint32_t v = 100;
  const auto r = pub.publish_copy(&v, sizeof(v));
  auto waited = std::chrono::steady_clock::now() - start;
  CHECK(r.status == Status::kQueueFull);
  CHECK(waited >= std::chrono::milliseconds(90));
  CHECK(free_blocks(topic) == Topic::kBlockCount - Topic::kQueueCapacity);

  // A consumer frees a cell: the blocked publish completes.
  std::thread consumer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    Topic::ReadLoan loan;
    CHECK(w.take(loan) == Status::kOk);
  });
  start = std::chrono::steady_clock::now();
  v = 101;
  CHECK(pub.publish_copy(&v, sizeof(v)).status == Status::kOk);
  waited = std::chrono::steady_clock::now() - start;
  consumer.join();
  CHECK(waited >= std::chrono::milliseconds(20) && waited < std::chrono::milliseconds(90));

  // A batch keeps its order when the second half has to wait.
  Topic::ReadLoan loan;
  CHECK(w.take(loan) == Status::kOk);
  loan.reset();
  std::thread drainer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    for (int i = 0; i < 2; ++i) {
      Topic::ReadLoan l;
      while (w.take(l) != Status::kOk) std::this_thread::yield();
    }
  });
  Topic::WriteLoan batch[2];
  for (uint32_t k = 0; k < 2; ++k) {
    CHECK(pub.loan(4, batch[k]) == Status::kOk);
    const uint32_t value = 200 + k;
    std::memcpy(batch[k].data(), &value, sizeof(value));
  }
  CHECK(pub.publish(batch, 2).status == Status::kOk);
  drainer.join();
  uint32_t seen[Topic::kQueueCapacity] = {};
  uint32_t n = 0;
  for (;;) {
    Topic::ReadLoan l;
    if (w.take(l) != Status::kOk || n == Topic::kQueueCapacity) break;
    seen[n++] = value_of(l);
  }
  CHECK(n >= 2 && seen[n - 2] == 200 && seen[n - 1] == 201);
  CHECK(free_blocks(topic) == Topic::kBlockCount);
}

}  // namespace

int main() {
  check_options();
  check_durable_backlog();
  check_requeue();
  check_redelivery_after_death(Delivery::kAtLeastOnce, nullptr);
  check_redelivery_after_death(Delivery::kAtMostOnce, nullptr);
  check_redelivery_after_death(Delivery::kAtLeastOnce, "take.after_claim:1");
  check_redelivery_after_death(Delivery::kAtLeastOnce, "take.after_convert:1");
  check_block();
  return finish("work_queue_test");
}
