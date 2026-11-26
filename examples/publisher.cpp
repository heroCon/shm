// Publishes a TestTopic every millisecond, building it in place (zero copy).
// Usage: publisher_example [/name]
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "shm_pubsub/shm_pubsub.hpp"
#include "test_topic.h"

static std::atomic<bool> running(true);
static void stop(int) { running.store(false); }

int main(int argc, char** argv) {
  std::signal(SIGINT, stop);
  std::signal(SIGTERM, stop);
  shm_pubsub::Options options;
  if (argc > 1) options.name = argv[1];
  try {
    shm_pubsub::Topic<> topic(options);
    shm_pubsub::Topic<>::Publisher pub(topic);
    uint64_t counter = 0;
    while (running.load()) {
      shm_pubsub::Topic<>::WriteLoan loan;
      const auto status = pub.loan(sizeof(TestTopic), loan);
      if (status != shm_pubsub::Status::kOk) {
        std::cerr << "loan: " << shm_pubsub::to_string(status) << '\n';
        return 1;
      }
      TestTopic* msg = new (loan.data()) TestTopic();
      msg->timestamp = ++counter;
      const auto r = pub.publish(std::move(loan));
      if (r.status != shm_pubsub::Status::kOk && r.status != shm_pubsub::Status::kNoSubscribers) {
        std::cerr << "publish: " << shm_pubsub::to_string(r.status) << " delivered " << r.delivered
                  << '/' << r.targets << '\n';
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
