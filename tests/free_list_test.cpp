#include <cassert>
#include <cstdint>

#include "lockfree_list.hpp"

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

  return 0;
}
