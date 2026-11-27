// recover() briefly locks every free slot byte. Endpoint constructors running
// at the same time must wait and rescan instead of reporting a full table,
// while a really full table is still reported.
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "test_util.hpp"

using Topic = shm_pubsub::Topic<SmallConfig>;

int main() {
  ScopedName name("slot_contention");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  std::atomic<bool> run{true};
  std::thread maintainer([&] {
    while (run.load()) topic.recover();
  });
  long ok = 0, failed = 0;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      Topic::Subscriber s(topic);
      Topic::Publisher p(topic);
      ++ok;
    } catch (const std::exception&) {
      ++failed;
    }
  }
  run = false;
  maintainer.join();
  std::printf("constructed=%ld failed=%ld\n", ok, failed);
  CHECK(ok > 0);
  CHECK(failed == 0);

  std::vector<std::unique_ptr<Topic::Subscriber>> all;
  for (uint32_t i = 0; i < Topic::kMaxSubscribers; ++i)
    all.emplace_back(new Topic::Subscriber(topic));
  bool threw = false;
  try {
    Topic::Subscriber extra(topic);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  CHECK(threw);
  return finish("slot_contention_test");
}
