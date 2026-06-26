#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "TestTopic.h"
#include "shm_pubsub.h"

int main() {
  ShmPubSub pub(ShmPubSub::PUBLISHER);
  TestTopic msg{};

  uint64_t count = 0;
  while (true) {
    msg.timestamp = count++;
    bool ret = pub.publish(&msg, sizeof(TestTopic));
    std::cout << "publish " << count << std::endl;
    if (!ret) {
      std::cerr << "publish failed" << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  return 0;
}
