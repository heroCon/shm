// One publisher, two private subscribers: both see the same block.
#include <iostream>

#include "shm_pubsub/shm_pubsub.hpp"

int main() {
  using Topic = shm_pubsub::Topic<>;
  shm_pubsub::Options options;
  options.name = "/shm_pubsub_broadcast_example";
  options.unlink_on_destroy = true;
  Topic topic(options);
  Topic::Subscriber a(topic);
  Topic::Subscriber b(topic);
  Topic::Publisher pub(topic);

  const char message[] = "broadcast payload";
  const auto r = pub.publish_copy(message, sizeof(message));
  std::cout << "status=" << shm_pubsub::to_string(r.status) << " delivered=" << r.delivered << '/'
            << r.targets << '\n';
  Topic::ReadLoan la, lb;
  if (a.take(la) == shm_pubsub::Status::kOk && b.take(lb) == shm_pubsub::Status::kOk) {
    std::cout << "a=" << la.data() << " b=" << lb.data()
              << " same_block=" << (la.data() == lb.data()) << '\n';
  }
}
