// Zero-copy throughput in one process: P publisher threads and N subscriber
// threads. A publish counts once at least one subscriber queued it; one that
// reached nobody is retried. `delivered` counts queue entries and should
// equal `received` unless --idle is given.
//
// Usage: throughput_benchmark [subscribers] [messages] [bytes] [options]
//   --publishers N   publisher threads (default 1), messages are split evenly
//   --batch N        publish and drain N messages per call (default 1)
//   --no-timestamps  use a Config with kTimestamps = false
//   --idle           subscribers never read and use kDropOldest, so every
//                    publish evicts the oldest message (publisher-side cost)
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "shm_pubsub/shm_pubsub.hpp"

using shm_pubsub::Status;

struct NoTimestamps : shm_pubsub::DefaultConfig {
  static constexpr bool kTimestamps = false;
};

struct Args {
  int subscribers = 1;
  long count = 3000000;
  size_t bytes = 64;
  int publishers = 1;
  int batch = 1;
  bool no_timestamps = false;
  bool idle = false;
};

template <class Config>
int run(const Args& a, const char* config_name) {
  using Topic = shm_pubsub::Topic<Config>;
  if (a.subscribers < 1 || a.publishers < 1 || a.bytes == 0 ||
      a.bytes > Topic::max_payload_size() || a.batch < 1 ||
      static_cast<uint32_t>(a.batch) > Topic::kMaxLoansPerPublisher ||
      static_cast<uint32_t>(a.publishers) > Topic::kMaxPublishers ||
      static_cast<uint32_t>(a.subscribers) > Topic::kMaxSubscribers) {
    std::fprintf(stderr, "invalid arguments for %s\n", config_name);
    return 2;
  }
  shm_pubsub::Options o;
  o.name = "/shm_pubsub_throughput_" + std::to_string(getpid());
  o.unlink_on_destroy = true;
  Topic topic(o);
  std::vector<std::unique_ptr<typename Topic::Subscriber>> subs;
  shm_pubsub::SubscriberOptions so;
  if (a.idle) so.overflow = shm_pubsub::Overflow::kDropOldest;
  for (int i = 0; i < a.subscribers; ++i)
    subs.emplace_back(new typename Topic::Subscriber(topic, so));
  std::atomic<bool> done{false};
  std::atomic<long> received{0};
  std::vector<std::thread> readers;
  if (!a.idle) {
    for (int i = 0; i < a.subscribers; ++i) {
      readers.emplace_back([&, i] {
        long n = 0;
        while (!done.load(std::memory_order_relaxed)) {
          if (a.batch > 1) {
            n += static_cast<long>(subs[i]->drain([](const typename Topic::ReadLoan&) {},
                                                  static_cast<size_t>(a.batch)));
          } else {
            typename Topic::ReadLoan loan;
            if (subs[i]->take(loan) == Status::kOk) ++n;
          }
        }
        received += n;
      });
    }
  }
  std::atomic<long> retries{0}, partial{0}, delivered{0};
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> writers;
  const long per_publisher = a.count / a.publishers;
  for (int p = 0; p < a.publishers; ++p) {
    writers.emplace_back([&] {
      typename Topic::Publisher pub(topic);
      long r_retries = 0, r_partial = 0, r_delivered = 0;
      std::vector<typename Topic::WriteLoan> loans(static_cast<size_t>(a.batch));
      ++ready;
      while (!go.load(std::memory_order_acquire)) {
      }
      for (long i = 0; i < per_publisher;) {
        int got = 0;
        for (; got < a.batch; ++got) {
          if (pub.loan(a.bytes, loans[got]) != Status::kOk) break;
          loans[got].data()[0] = static_cast<char>(i + got);
        }
        if (got == 0) {
          ++r_retries;
          continue;
        }
        shm_pubsub::PublishResult r;
        if (a.batch > 1) {
          r = pub.publish(loans.data(), static_cast<size_t>(got));
        } else {
          r = pub.publish(std::move(loans[0]));
        }
        if (r.delivered == 0) {
          ++r_retries;
          continue;
        }
        if (r.status == Status::kPartial) ++r_partial;
        r_delivered += r.delivered;
        i += got;
      }
      retries += r_retries;
      partial += r_partial;
      delivered += r_delivered;
    });
  }
  while (ready.load() < a.publishers) std::this_thread::yield();
  const auto start = std::chrono::steady_clock::now();
  go.store(true, std::memory_order_release);
  for (auto& t : writers) t.join();
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  done = true;
  for (auto& t : readers) t.join();
  const long total = per_publisher * a.publishers;
  std::printf(
      "transport,config,publishers,subscribers,batch,idle,message_bytes,messages,seconds,"
      "publish_per_sec,retries,partial,delivered,received\n");
  std::printf("shm,%s,%d,%d,%d,%d,%zu,%ld,%.4f,%.0f,%ld,%ld,%ld,%ld\n", config_name, a.publishers,
              a.subscribers, a.batch, a.idle ? 1 : 0, a.bytes, total, seconds, total / seconds,
              retries.load(), partial.load(), delivered.load(), received.load());
  return 0;
}

int main(int argc, char** argv) {
  Args a;
  int positional = 0;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--publishers" && i + 1 < argc) {
      a.publishers = std::atoi(argv[++i]);
    } else if (arg == "--batch" && i + 1 < argc) {
      a.batch = std::atoi(argv[++i]);
    } else if (arg == "--no-timestamps") {
      a.no_timestamps = true;
    } else if (arg == "--idle") {
      a.idle = true;
    } else if (positional == 0) {
      a.subscribers = std::atoi(argv[i]);
      ++positional;
    } else if (positional == 1) {
      a.count = std::atol(argv[i]);
      ++positional;
    } else if (positional == 2) {
      a.bytes = std::strtoull(argv[i], nullptr, 10);
      ++positional;
    } else {
      std::fprintf(stderr, "unknown argument %s\n", argv[i]);
      return 2;
    }
  }
  if (a.no_timestamps) return run<NoTimestamps>(a, "no_timestamps");
  return run<shm_pubsub::DefaultConfig>(a, "default");
}
