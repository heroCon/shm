// Regressions for the 0.2 starvation bugs: a stalled reader or a competing
// backlog could exhaust the shared pool and block every other reader.
#include <vector>

#include "test_util.hpp"

using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

static void check_stalled_reader_does_not_starve_others(Topic& topic, Overflow overflow) {
  Topic::Publisher pub(topic);
  SubscriberOptions options;
  options.overflow = overflow;
  Topic::Subscriber stalled(topic, options);
  Topic::Subscriber fast(topic);
  size_t fast_received = 0;
  uint64_t last_sequence = 0;
  for (int i = 0; i < 1000; ++i) {
    const auto r = pub.publish_copy("m", 1);
    CHECK(r.ok() && r.delivered >= 1);
    Topic::ReadLoan loan;
    while (fast.take(loan) == Status::kOk) {
      ++fast_received;
      last_sequence = loan.sequence();
      loan.reset();
    }
  }
  CHECK(fast_received == 1000);
  const auto snap = topic.snapshot();
  CHECK(snap.slots[stalled.slot()].queue_depth == Topic::kQueueCapacity);
  if (overflow == Overflow::kDropNewest) {
    CHECK(snap.slots[stalled.slot()].dropped_newest == 1000 - Topic::kQueueCapacity);
  } else {
    CHECK(snap.slots[stalled.slot()].dropped_oldest == 1000 - Topic::kQueueCapacity);
    Topic::ReadLoan loan;
    CHECK(stalled.take(loan) == Status::kOk &&
          loan.sequence() + Topic::kQueueCapacity == last_sequence + 1);
  }
}

// Messages queued for a group with no active member keep their bounded
// share of the pool and never block broadcast loans.
static void check_group_backlog_does_not_block_broadcast(Topic& topic) {
  Topic::Publisher pub(topic);
  {
    SubscriberOptions worker;
    worker.group = 2;
    Topic::Subscriber w(topic, worker);
    for (int i = 0; i < 64; ++i) pub.publish_copy("w", 1);
    w.unsubscribe();
    Topic::ReadLoan loan;
    CHECK(w.take(loan) == Status::kNotSubscribed);
  }
  Topic::Subscriber reader(topic);
  std::vector<Topic::WriteLoan> loans(Topic::kMaxLoansPerPublisher);
  for (auto& loan : loans) CHECK(pub.loan(1, loan) == Status::kOk);
  loans.clear();
  for (int i = 0; i < 100; ++i) {
    CHECK(pub.publish_copy("b", 1).status == Status::kOk);
    Topic::ReadLoan loan;
    CHECK(reader.take(loan) == Status::kOk);
  }
  SubscriberOptions worker;
  worker.group = 2;
  Topic::Subscriber late(topic, worker);
  size_t backlog = 0;
  Topic::ReadLoan loan;
  while (late.take(loan) == Status::kOk) {
    ++backlog;
    loan.reset();
  }
  CHECK(backlog == Topic::kQueueCapacity);
}

// The pool covers every bounded reference, so subscribers that hold their
// maximum loans and full queues still leave publishers able to loan.
static void check_pool_budget(Topic& topic) {
  std::vector<Topic::Subscriber*> subs;
  for (uint32_t i = 0; i < Topic::kMaxSubscribers; ++i)
    subs.push_back(new Topic::Subscriber(topic));
  std::vector<Topic::Publisher*> pubs;
  for (uint32_t i = 0; i < Topic::kMaxPublishers; ++i) pubs.push_back(new Topic::Publisher(topic));
  std::vector<Topic::ReadLoan> held;
  for (uint32_t round = 0; round < Topic::kMaxLoansPerSubscriber; ++round) {
    pubs[0]->publish_copy("h", 1);
    for (auto* s : subs) {
      held.push_back(Topic::ReadLoan());
      CHECK(s->take(held.back()) == Status::kOk);
    }
  }
  for (uint32_t i = 0; i < 3 * Topic::kQueueCapacity; ++i) pubs[0]->publish_copy("q", 1);
  std::vector<Topic::WriteLoan> loans;
  for (auto* p : pubs) {
    for (uint32_t i = 0; i < Topic::kMaxLoansPerPublisher; ++i) {
      loans.push_back(Topic::WriteLoan());
      CHECK(p->loan(1, loans.back()) == Status::kOk);
    }
  }
  loans.clear();
  held.clear();
  for (auto* p : pubs) delete p;
  for (auto* s : subs) delete s;
}

int main() {
  ScopedName name("fairness");
  shm_pubsub::Options options;
  options.name = name.name;
  Topic topic(options);
  check_stalled_reader_does_not_starve_others(topic, Overflow::kDropNewest);
  check_stalled_reader_does_not_starve_others(topic, Overflow::kDropOldest);
  check_group_backlog_does_not_block_broadcast(topic);
  check_pool_budget(topic);
  CHECK(free_blocks(topic) == Topic::kBlockCount);
  return finish("fairness_test");
}
