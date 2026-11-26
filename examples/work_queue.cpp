// A consumer group shares work while a private subscriber observes every
// message: broadcast and work queue are the same mechanism.
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "shm_pubsub/shm_pubsub.hpp"

int main() {
  using Topic = shm_pubsub::Topic<>;
  shm_pubsub::Options options;
  options.name = "/shm_pubsub_work_queue_example";
  options.unlink_on_destroy = true;
  Topic topic(options);
  Topic::Publisher pub(topic);
  Topic::Subscriber auditor(topic);

  shm_pubsub::SubscriberOptions worker_options;
  worker_options.group = 1;
  std::vector<Topic::Subscriber*> workers;
  for (int i = 0; i < 3; ++i) workers.push_back(new Topic::Subscriber(topic, worker_options));
  std::atomic<int> done[3] = {{0}, {0}, {0}};
  std::vector<std::thread> threads;
  for (int i = 0; i < 3; ++i) {
    threads.emplace_back([&, i] {
      for (;;) {
        Topic::ReadLoan job;
        const auto status = workers[i]->wait_take(job, std::chrono::seconds(1));
        if (status == shm_pubsub::Status::kStopped) return;
        if (status == shm_pubsub::Status::kOk) ++done[i];
      }
    });
  }
  int audited = 0;
  for (int job = 0; job < 6; ++job) {
    pub.publish_copy(&job, sizeof(job));
    Topic::ReadLoan seen;
    if (auditor.wait_take(seen, std::chrono::seconds(1)) == shm_pubsub::Status::kOk) ++audited;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  for (auto* w : workers) w->stop();
  for (auto& t : threads) t.join();
  std::cout << "auditor saw " << audited << " jobs; workers did " << done[0] << ", " << done[1]
            << ", " << done[2] << '\n';
  for (auto* w : workers) delete w;
}
