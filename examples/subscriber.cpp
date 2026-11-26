// Receives TestTopic messages in place and reports gaps from sequence numbers.
// Usage: subscriber_example [/name]
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>

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
    shm_pubsub::Topic<>::Subscriber sub(topic);
    uint64_t expected = 0;
    bool first = true;
    while (running.load()) {
      shm_pubsub::Topic<>::ReadLoan loan;
      const auto status = sub.wait_take(loan, std::chrono::milliseconds(250));
      if (status == shm_pubsub::Status::kTimedOut) continue;
      if (status != shm_pubsub::Status::kOk) {
        std::cerr << "receive: " << shm_pubsub::to_string(status) << '\n';
        return 1;
      }
      if (!first && loan.sequence() != expected) {
        std::cout << "missed " << loan.sequence() - expected << " message(s)\n";
      }
      first = false;
      expected = loan.sequence() + 1;
      const TestTopic* msg = reinterpret_cast<const TestTopic*>(loan.data());
      std::cout << "received " << msg->timestamp << '\n';
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
