// Regression for the 0.2 thundering herd: one futex word per region woke
// every blocked reader for every message.
#include <sys/resource.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "test_util.hpp"

using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

struct WaitStats {
  std::atomic<long> switches{0};
  std::atomic<long> received{0};
};

static void wait_loop(Topic::Subscriber& sub, WaitStats& stats) {
  rusage before{}, after{};
  getrusage(RUSAGE_THREAD, &before);
  for (;;) {
    Topic::ReadLoan loan;
    const Status s = sub.wait_take(loan, std::chrono::seconds(10));
    if (s == Status::kOk) {
      ++stats.received;
    } else if (s == Status::kStopped) {
      break;
    }
  }
  getrusage(RUSAGE_THREAD, &after);
  stats.switches += after.ru_nvcsw - before.ru_nvcsw;
}

static double wakeups_per_message(uint32_t group, int readers, int messages, long* received,
                                  long* delivered) {
  ScopedName name(group ? "wake_group" : "wake_private");
  shm_pubsub::Options options;
  options.name = name.name;
  Topic topic(options);
  Topic::Publisher pub(topic);
  SubscriberOptions sub_options;
  sub_options.group = group;
  std::vector<std::unique_ptr<Topic::Subscriber>> subs;
  for (int i = 0; i < readers; ++i) subs.emplace_back(new Topic::Subscriber(topic, sub_options));
  WaitStats stats;
  std::vector<std::thread> threads;
  for (int i = 0; i < readers; ++i) {
    threads.emplace_back([&stats, &subs, i] { wait_loop(*subs[i], stats); });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  *delivered = 0;
  for (int i = 0; i < messages; ++i) {
    *delivered += pub.publish_copy("m", 1).delivered;
    std::this_thread::sleep_for(std::chrono::microseconds(500));
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (stats.received.load() < *delivered && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  for (auto& s : subs) s->stop();
  for (auto& t : threads) t.join();
  *received = stats.received.load();
  // Each reader also sleeps once more before the stop wake-up.
  return static_cast<double>(stats.switches.load() - readers) / messages;
}

int main() {
  const int messages = 400;
  long received = 0, delivered = 0;
  const double group_rate = wakeups_per_message(1, 6, messages, &received, &delivered);
  std::printf("group of 6: %.2f wakeups per message, %ld/%ld received\n", group_rate, received,
              delivered);
  CHECK(received == delivered);
  CHECK(delivered > messages / 2);
  CHECK(group_rate < 2.0);

  const double private_rate = wakeups_per_message(0, 6, messages, &received, &delivered);
  std::printf("6 private readers: %.2f wakeups per message, %ld/%ld received\n", private_rate,
              received, delivered);
  CHECK(received == delivered);
  CHECK(delivered > 6L * messages / 2);
  CHECK(private_rate < 6.0 * 1.5);

  // A stopped group member does not strand work queued for the others.
  ScopedName name("wake_stop");
  shm_pubsub::Options options;
  options.name = name.name;
  Topic topic(options);
  Topic::Publisher pub(topic);
  SubscriberOptions in_group;
  in_group.group = 1;
  Topic::Subscriber a(topic, in_group);
  Topic::Subscriber b(topic, in_group);
  Status status_a = Status::kOk, status_b = Status::kOk;
  std::thread ta([&] {
    Topic::ReadLoan loan;
    status_a = a.wait_take(loan, std::chrono::seconds(5));
  });
  std::thread tb([&] {
    Topic::ReadLoan loan;
    status_b = b.wait_take(loan, std::chrono::seconds(5));
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  a.stop();
  ta.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  CHECK(pub.publish_copy("job", 4).status == Status::kOk);
  tb.join();
  CHECK(status_a == Status::kStopped);
  CHECK(status_b == Status::kOk);
  return finish("wakeup_test");
}
