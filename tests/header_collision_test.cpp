// 0.2 defined BLOCK_SIZE and friends at global scope, which collided with
// kernel headers. Everything now lives in namespace shm_pubsub.
#include <linux/fs.h>
#include <sys/mount.h>

#include "shm_pubsub/shm_pubsub.hpp"

int main() {
  static_assert(shm_pubsub::Topic<>::max_payload_size() == 4096 - 64, "default payload");
  return BLOCK_SIZE > 0 ? 0 : 1;
}
