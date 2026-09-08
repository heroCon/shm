#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

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
      auto r = sub.receive_result(&msg, sizeof(msg), n);
      if (r.status == ShmPubSub::Status::OK)
        std::cout << "received " << msg.timestamp << '\n';
      else
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    sub.stop();
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
