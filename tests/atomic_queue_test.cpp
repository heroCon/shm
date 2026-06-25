#include "shm_pubsub.h"

#include <cassert>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

int main() {
    AtomicQueue queue;
    queue.init();

    size_t value = 0;
    assert(!queue.dequeue(value));

    for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
        assert(queue.enqueue(i));
    }
    assert(!queue.enqueue(QUEUE_CAPACITY));
    assert(queue.contains(0));
    assert(queue.contains(QUEUE_CAPACITY - 1));
    assert(!queue.contains(QUEUE_CAPACITY));

    for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
        assert(queue.dequeue(value));
        assert(value == i);
    }
    assert(!queue.dequeue(value));

    assert(queue.enqueue(42));
    queue.clear();
    assert(!queue.dequeue(value));

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
                while (!queue.enqueue(base + i)) {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (size_t c = 0; c < consumer_count; ++c) {
        threads.emplace_back([&queue, &consumed, &sum, total_items]() {
            size_t item = 0;
            while (consumed.load(std::memory_order_acquire) < total_items) {
                if (queue.dequeue(item)) {
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
    assert(!queue.dequeue(value));

    return 0;
}
