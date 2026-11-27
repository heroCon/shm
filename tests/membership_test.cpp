// Group membership must survive a member killed at any instant: after
// reclamation the member mask equals the live members, so publishes neither
// go to a memberless group nor skip a group with a live member.
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <memory>
#include <random>

#include "test_util.hpp"

using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

static void check_kill_while_toggling(const shm_pubsub::Options& o, int rounds) {
  std::mt19937 rng(7);
  int wrong = 0;
  for (int r = 0; r < rounds; ++r) {
    const bool sibling = r % 2 == 1;
    Topic::destroy(o.name);
    Topic topic(o);
    SubscriberOptions in_group;
    in_group.group = 1;
    std::unique_ptr<Topic::Subscriber> live;
    if (sibling) live.reset(new Topic::Subscriber(topic, in_group));
    int ready[2];
    CHECK(pipe(ready) == 0);
    const pid_t child = fork();
    if (child == 0) {
      Topic t(o);
      Topic::Subscriber s(t, in_group);
      char x = 1;
      if (write(ready[1], &x, 1) != 1) _exit(2);
      for (;;) {
        s.unsubscribe();
        s.subscribe();
      }
    }
    char x;
    CHECK(read(ready[0], &x, 1) == 1);
    close(ready[0]);
    close(ready[1]);
    usleep(rng() % 200);
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    topic.collect_garbage();
    const uint32_t members = topic.snapshot().groups[0].members;
    Topic::Publisher pub(topic);
    const shm_pubsub::PublishResult pr = pub.publish_copy("x", 1);
    if (members != (sibling ? 1u : 0u) ||
        (sibling ? pr.delivered != 1 : pr.status != Status::kNoSubscribers)) {
      if (++wrong <= 3) {
        std::fprintf(stderr, "round %d: members=%u status=%s delivered=%u\n", r, members,
                     shm_pubsub::to_string(pr.status), pr.delivered);
      }
    }
    if (live) {
      Topic::ReadLoan loan;
      while (live->take(loan) == Status::kOk) loan.reset();
    }
  }
  CHECK(wrong == 0);
}

static void check_policy_and_drain(Topic& topic) {
  SubscriberOptions oldest;
  oldest.group = 2;
  oldest.overflow = Overflow::kDropOldest;
  Topic::Publisher pub(topic);
  {
    Topic::Subscriber a(topic, oldest);
    bool threw = false;
    try {
      SubscriberOptions newest;
      newest.group = 2;
      Topic::Subscriber b(topic, newest);
    } catch (const std::invalid_argument&) {
      threw = true;
    }
    CHECK(threw);
    Topic::Subscriber same(topic, oldest);
    CHECK(topic.snapshot().groups[1].members == 2);
    for (int i = 0; i < 10; ++i) CHECK(pub.publish_copy("w", 1).delivered == 1);
    CHECK(topic.snapshot().groups[1].queue_depth == Topic::kQueueCapacity);
  }
  // The last member left, so the backlog was discarded.
  const auto snap = topic.snapshot();
  CHECK(snap.groups[1].members == 0 && snap.groups[1].queue_depth == 0);
  CHECK(snap.free_blocks == Topic::kBlockCount);
  CHECK(snap.groups[1].policy_set && snap.groups[1].overflow == Overflow::kDropOldest);
}

int main() {
  ScopedName name("membership");
  shm_pubsub::Options o;
  o.name = name.name;
  check_kill_while_toggling(o, 400);
  {
    Topic::destroy(o.name);
    Topic topic(o);
    check_policy_and_drain(topic);
  }
  return finish("membership_test");
}
