#include "shm_pubsub.h"

#include <cassert>
#include <cstddef>

int main() {
    AtomicQueue queue;

    size_t value = 0;
    assert(!queue.dequeue(value));

    for (size_t i = 0; i < QUEUE_CAPACITY - 1; ++i) {
        assert(queue.enqueue(i));
    }
    assert(!queue.enqueue(QUEUE_CAPACITY));
    assert(queue.contains(0));
    assert(queue.contains(QUEUE_CAPACITY - 2));
    assert(!queue.contains(QUEUE_CAPACITY));

    for (size_t i = 0; i < QUEUE_CAPACITY - 1; ++i) {
        assert(queue.dequeue(value));
        assert(value == i);
    }
    assert(!queue.dequeue(value));

    assert(queue.enqueue(42));
    queue.clear();
    assert(!queue.dequeue(value));

    return 0;
}
