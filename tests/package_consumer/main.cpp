#include <unistd.h>

#include <cstdio>
#include <string>

#include "shm_pubsub/shm_pubsub.hpp"

int main() {
  shm_pubsub::Options options;
  options.name = "/shm_pubsub_consumer_" + std::to_string(getpid());
  options.unlink_on_destroy = true;
  shm_pubsub::Topic<> topic(options);
  shm_pubsub::Topic<>::Publisher pub(topic);
  shm_pubsub::Topic<>::Subscriber sub(topic);
  if (!pub.publish_copy("hello", 5).ok()) return 1;
  shm_pubsub::Topic<>::ReadLoan loan;
  if (sub.take(loan) != shm_pubsub::Status::kOk || loan.size() != 5) return 1;
  std::printf("shm_pubsub %d.%d.%d ok\n", SHM_PUBSUB_VERSION_MAJOR, SHM_PUBSUB_VERSION_MINOR,
              SHM_PUBSUB_VERSION_PATCH);
  return 0;
}
