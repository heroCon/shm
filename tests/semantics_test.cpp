#include <unistd.h>

#include <iostream>
#include <string>

#include "shm_pubsub.h"
static int check(bool condition, const char* message) {
  if (!condition) std::cerr << message << '\n';
  return condition ? 0 : 1;
}
int main() {
  ShmPubSub::Options o;
  o.name = "/shm_semantics_" + std::to_string(getpid());
  ShmPubSub::destroy(o.name);
  int errors = 0;
  {
    ShmPubSub pub(ShmPubSub::PUBLISHER, o);
    errors += check(pub.publish_result("x", 1).status == ShmPubSub::Status::NO_SUBSCRIBERS,
                    "no-subscriber status");
    ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
    sub.subscribe();
    std::string large(BLOCK_SIZE, 'x');
    auto too_large = pub.publish_result(large.data(), large.size());
    errors += check(too_large.status == ShmPubSub::Status::MESSAGE_TOO_LARGE, "oversize status");
    errors +=
        check(pub.publish_result("hello", 6).status == ShmPubSub::Status::OK, "publish status");
    char small[2];
    size_t actual = 0;
    auto receive = sub.receive_result(small, sizeof(small), actual);
    errors += check(receive.status == ShmPubSub::Status::BUFFER_TOO_SMALL && actual == 6,
                    "small-buffer status");
  }
  ShmPubSub::destroy(o.name);
  return errors ? 1 : 0;
}
