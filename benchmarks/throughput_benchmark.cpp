// Zero-copy throughput: one publisher thread, N busy-polling subscriber
// threads in one process. A publish counts once at least one subscriber
// queued it; one that reached nobody is retried. `delivered` counts queue
// entries and should equal `received`.
// Usage: throughput_benchmark [subscribers] [messages] [bytes]
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "shm_pubsub/shm_pubsub.hpp"

using Topic = shm_pubsub::Topic<>;
using shm_pubsub::Status;

int main(int argc, char** argv) {
  const int subscribers = argc > 1 ? std::atoi(argv[1]) : 1;
  const long count = argc > 2 ? std::atol(argv[2]) : 3000000;
  const size_t bytes = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 64;
  if (subscribers < 1 || bytes == 0 || bytes > Topic::max_payload_size()) return 2;
  shm_pubsub::Options o;
  o.name = "/shm_pubsub_throughput_" + std::to_string(getpid());
  o.unlink_on_destroy = true;
  Topic topic(o);
  Topic::Publisher pub(topic);
  std::vector<std::unique_ptr<Topic::Subscriber>> subs;
  for (int i = 0; i < subscribers; ++i) subs.emplace_back(new Topic::Subscriber(topic));
  std::atomic<bool> done{false};
  std::atomic<long> received{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < subscribers; ++i) {
    threads.emplace_back([&, i] {
      long n = 0;
      while (!done.load(std::memory_order_relaxed)) {
        Topic::ReadLoan loan;
        if (subs[i]->take(loan) == Status::kOk) ++n;
      }
      received += n;
    });
  }
  long retries = 0, partial = 0, delivered = 0;
  const auto start = std::chrono::steady_clock::now();
  for (long i = 0; i < count;) {
    Topic::WriteLoan loan;
    if (pub.loan(bytes, loan) != Status::kOk) {
      ++retries;
      continue;
    }
    loan.data()[0] = static_cast<char>(i);
    const shm_pubsub::PublishResult r = pub.publish(std::move(loan));
    if (r.delivered == 0) {
      ++retries;
      continue;
    }
    if (r.status == Status::kPartial) ++partial;
    delivered += r.delivered;
    ++i;
  }
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  done = true;
  for (auto& t : threads) t.join();
  std::printf(
      "transport,subscribers,message_bytes,messages,seconds,publish_per_sec,retries,partial,"
      "delivered,received\n");
  std::printf("shm,%d,%zu,%ld,%.4f,%.0f,%ld,%ld,%ld,%ld\n", subscribers, bytes, count, seconds,
              count / seconds, retries, partial, delivered, received.load());
}
