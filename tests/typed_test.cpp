#include "shm_pubsub/typed.hpp"

#include <poll.h>
#include <unistd.h>

#include <cstdint>

#include "shm_pubsub/eventfd_bridge.hpp"
#include "test_util.hpp"

struct Sample {
  int32_t a;
  int32_t b;
};

int main() {
  ScopedName name("typed");
  shm_pubsub::Options options;
  options.name = name.name;
  using Topic = shm_pubsub::Topic<SmallConfig>;
  Topic topic(options);
  shm_pubsub::TypedPublisher<Sample, SmallConfig> pub(topic);
  shm_pubsub::TypedSubscriber<Sample, SmallConfig> sub(topic);
  Sample in = {3, 4};
  CHECK(pub.publish(in) == shm_pubsub::Status::kOk);
  Sample out = {};
  CHECK(sub.take(out) == shm_pubsub::Status::kOk);
  CHECK(out.a == 3 && out.b == 4);

  shm_pubsub::EventfdBridge bridge;
  CHECK(bridge.add(sub.subscriber()));
  const shm_pubsub::Status started = bridge.start();
  if (started == shm_pubsub::Status::kNotSupported) {
    std::printf("eventfd bridge skipped: no futex_waitv\n");
    return finish("typed_test");
  }
  CHECK(started == shm_pubsub::Status::kOk);
  in.a = 9;
  CHECK(pub.publish(in) == shm_pubsub::Status::kOk);
  pollfd pfd = {};
  pfd.fd = bridge.fd();
  pfd.events = POLLIN;
  CHECK(poll(&pfd, 1, 1000) == 1);
  uint64_t ticks = 0;
  CHECK(read(bridge.fd(), &ticks, sizeof(ticks)) == static_cast<ssize_t>(sizeof(ticks)));
  CHECK(ticks == 1);
  // The message is still unread. A level-triggered loop would keep writing
  // the semaphore; one tick is the whole rising edge.
  CHECK(poll(&pfd, 1, 30) == 0);
  CHECK(sub.take(out) == shm_pubsub::Status::kOk && out.a == 9);
  bridge.stop();

  shm_pubsub::TypedSubscriber<Sample, SmallConfig> stopping(topic);
  shm_pubsub::EventfdBridge stopped_bridge;
  CHECK(stopped_bridge.add(stopping.subscriber()));
  CHECK(stopped_bridge.start() == shm_pubsub::Status::kOk);
  stopping.subscriber().stop();
  pfd.fd = stopped_bridge.fd();
  CHECK(poll(&pfd, 1, 1000) == 1);
  CHECK(read(stopped_bridge.fd(), &ticks, sizeof(ticks)) == static_cast<ssize_t>(sizeof(ticks)));
  CHECK(ticks == 1);
  CHECK(poll(&pfd, 1, 30) == 0);
  stopped_bridge.stop();
  return finish("typed_test");
}
