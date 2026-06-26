#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include "TestTopic.h"
#include "shm_pubsub.h"

int main() {
  ShmPubSub sub(ShmPubSub::SUBSCRIBER);
  sub.subscribe();  // 订阅数据

  TestTopic buf{};
  size_t actual_len = 0;

  // 循环接收数据（非阻塞）
  while (true) {
    if (sub.receive(&buf, sizeof(buf), actual_len)) {
      std::cout << "Received: " << buf.timestamp << std::endl;
      memset(&buf, 0, sizeof(buf));
    }
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }

  return 0;
}
