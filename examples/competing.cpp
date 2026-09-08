#include <iostream>

#include "shm_pubsub.h"
int main() {
  ShmPubSub::Options o;
  o.name = "/shm_competing_example";
  ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
  sub.subscribe();
  char data[4096];
  size_t size = 0;
  while (true) {
    auto r = sub.receive_result(data, sizeof(data), size);
    if (r.status == ShmPubSub::Status::OK)
      std::cout.write(data, size);
    else
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}
