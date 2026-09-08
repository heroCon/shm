#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>
#include <vector>

#include "lock_free_list.h"

#undef assert
#define assert(condition) do { if (!(condition)) std::abort(); } while (false)

int main() {
  LockFreeFreeList<uint32_t, 3> free_list;
  free_list.init();

  uint32_t index = 99;
  assert(free_list.pop(index));
  assert(index == 0);
  assert(free_list.pop(index));
  assert(index == 1);
  assert(free_list.pop(index));
  assert(index == 2);
  assert(!free_list.pop(index));

  assert(free_list.push(1));
  assert(!free_list.push(1));
  assert(free_list.pop(index));
  assert(index == 1);

  LockFreeFreeList<uint32_t, 32> concurrent_list;
  concurrent_list.init();
  std::atomic<bool> in_use[32];
  for (size_t i = 0; i < 32; ++i) in_use[i].store(false);
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (size_t worker = 0; worker < 4; ++worker) {
    threads.emplace_back([&]() {
      for (size_t i = 0; i < 10000; ++i) {
        uint32_t value = 0;
        while (!concurrent_list.pop(value)) std::this_thread::yield();
        bool expected = false;
        if (!in_use[value].compare_exchange_strong(expected, true)) failed.store(true);
        expected = true;
        if (!in_use[value].compare_exchange_strong(expected, false)) failed.store(true);
        if (!concurrent_list.push(value)) failed.store(true);
      }
    });
  }
  for (std::thread& thread : threads) thread.join();
  assert(!failed.load());
  for (size_t i = 0; i < 32; ++i) assert(!in_use[i].load());

  return 0;
}
