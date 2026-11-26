#include "shm_pubsub/detail/mpmc_queue.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <thread>
#include <vector>

constexpr uint32_t QUEUE_CAPACITY = 32;
using AtomicQueue = shm_pubsub::detail::MpmcQueue<QUEUE_CAPACITY>;

#undef assert
#define assert(condition)           \
  do {                              \
    if (!(condition)) std::abort(); \
  } while (false)

int main() {
  AtomicQueue queue;
  queue.init();

  uint32_t value = 0;
  assert(!queue.try_dequeue(value));

  for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
    assert(queue.try_enqueue(static_cast<uint32_t>(i)));
  }
  assert(!queue.try_enqueue(QUEUE_CAPACITY));
  assert(queue.size_approx() == QUEUE_CAPACITY && queue.head_ready());

  for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
    assert(queue.try_dequeue(value));
    assert(value == i);
  }
  assert(!queue.try_dequeue(value));

  // A claimed cell stays unusable until released.
  assert(queue.try_enqueue(42));
  uint64_t pos = 0;
  assert(queue.try_claim(pos, value) && value == 42);
  for (uint32_t i = 0; i + 1 < QUEUE_CAPACITY; ++i)
    assert(queue.try_enqueue(static_cast<uint32_t>(i)));
  assert(!queue.try_enqueue(7));
  queue.release(pos);
  assert(queue.try_enqueue(7));
  while (queue.try_dequeue(value)) {
  }

  queue.init();
  const size_t producer_count = 4;
  const size_t consumer_count = 4;
  const size_t items_per_producer = 256;
  const size_t total_items = producer_count * items_per_producer;
  const uint64_t expected_sum = (total_items - 1) * total_items / 2;

  std::atomic<size_t> consumed{0};
  std::atomic<uint64_t> sum{0};
  std::vector<std::thread> threads;

  for (size_t p = 0; p < producer_count; ++p) {
    threads.emplace_back([&queue, p]() {
      const size_t base = p * items_per_producer;
      for (size_t i = 0; i < items_per_producer; ++i) {
        while (!queue.try_enqueue(static_cast<uint32_t>(base + i))) {
          std::this_thread::yield();
        }
      }
    });
  }

  for (size_t c = 0; c < consumer_count; ++c) {
    threads.emplace_back([&queue, &consumed, &sum]() {
      uint32_t item = 0;
      while (consumed.load(std::memory_order_acquire) < total_items) {
        if (queue.try_dequeue(item)) {
          sum.fetch_add(item, std::memory_order_acq_rel);
          consumed.fetch_add(1, std::memory_order_acq_rel);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  for (std::thread& thread : threads) {
    thread.join();
  }

  assert(consumed.load() == total_items);
  assert(sum.load() == expected_sum);
  assert(!queue.try_dequeue(value));

  return 0;
}
