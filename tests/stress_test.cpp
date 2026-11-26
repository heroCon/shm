// Multi-publisher, multi-subscriber stress with reader churn, consumer groups
// and both overflow policies. Verifies payload integrity and that every block
// returns to the pool.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include "test_util.hpp"

using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<>;

static size_t payload_size(uint64_t publisher, uint64_t seq) {
  return 16 + (seq * 37 + publisher) % (Topic::max_payload_size() - 16);
}

int main(int argc, char** argv) {
  const int duration_ms = argc > 1 ? std::atoi(argv[1]) : 1500;
  ScopedName name("stress");
  shm_pubsub::Options options;
  options.name = name.name;
  Topic topic(options);
  std::atomic<bool> run{true};
  std::atomic<long> delivered{0}, received{0}, corrupt{0}, reconnects{0};
  std::vector<std::thread> threads;
  for (int p = 0; p < 3; ++p) {
    threads.emplace_back([&] {
      Topic::Publisher pub(topic);
      uint64_t seq = 0;
      while (run.load(std::memory_order_relaxed)) {
        Topic::WriteLoan loan;
        const size_t len = payload_size(pub.slot(), seq);
        if (pub.loan(len, loan) != Status::kOk) {
          std::this_thread::yield();
          continue;
        }
        const uint64_t header[2] = {pub.slot(), seq};
        std::memcpy(loan.data(), header, sizeof(header));
        std::memset(loan.data() + 16, 'a' + static_cast<int>((seq + pub.slot()) % 26), len - 16);
        delivered += pub.publish(std::move(loan)).delivered;
        ++seq;
      }
    });
  }
  for (int s = 0; s < 5; ++s) {
    threads.emplace_back([&, s] {
      while (run.load(std::memory_order_relaxed)) {
        SubscriberOptions o;
        o.group = s >= 3 ? 1 : 0;
        o.overflow = s % 2 ? Overflow::kDropOldest : Overflow::kDropNewest;
        Topic::Subscriber sub(topic, o);
        for (int i = 0; run.load(std::memory_order_relaxed) && (s == 0 || i < 300); ++i) {
          Topic::ReadLoan loan;
          if (sub.wait_take(loan, std::chrono::milliseconds(5)) != Status::kOk) continue;
          ++received;
          uint64_t header[2];
          std::memcpy(header, loan.data(), sizeof(header));
          const char expected = static_cast<char>('a' + (header[1] + header[0]) % 26);
          if (header[0] != loan.publisher_id() || header[1] != loan.sequence() ||
              loan.size() != payload_size(header[0], header[1])) {
            ++corrupt;
            continue;
          }
          for (size_t k = 16; k < loan.size(); ++k) {
            if (loan.data()[k] != expected) {
              ++corrupt;
              break;
            }
          }
        }
        ++reconnects;
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
  run = false;
  for (auto& t : threads) t.join();
  {
    SubscriberOptions o;
    o.group = 1;
    Topic::Subscriber leftover(topic, o);
    Topic::ReadLoan loan;
    while (leftover.take(loan) == Status::kOk) loan.reset();
  }
  const uint32_t free_count = free_blocks(topic);
  std::printf("delivered=%ld received=%ld corrupt=%ld reconnects=%ld free=%u/%u\n",
              delivered.load(), received.load(), corrupt.load(), reconnects.load(), free_count,
              Topic::kBlockCount);
  CHECK(received.load() > 0);
  CHECK(corrupt.load() == 0);
  CHECK(free_count == Topic::kBlockCount);
  return finish("stress_test");
}
