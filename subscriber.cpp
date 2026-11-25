#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>

#include "shm_pubsub.h"
#include "test_topic.h"
static std::atomic<bool> running(true);
static void stop(int) { running.store(false); }
int main(int argc, char** argv) {
  std::signal(SIGINT, stop);
  std::signal(SIGTERM, stop);
  ShmPubSub::Options o;
  if (argc > 1) o.name = argv[1];
  try {
    ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
    sub.subscribe();
    TestTopic msg{};
    size_t n = 0;
    while (running.load()) {
      auto r = sub.receive_wait(&msg, sizeof(msg), n, std::chrono::milliseconds(250));
      if (r.status == ShmPubSub::Status::OK)
        std::cout << "received " << msg.timestamp << '\n';
      else if (r.status != ShmPubSub::Status::TIMED_OUT) {
        std::cerr << "receive failed: " << static_cast<int>(r.status) << '\n';
        return 1;
      }
    }
    sub.stop();
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
