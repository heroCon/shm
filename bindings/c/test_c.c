#include <stdio.h>
#include <unistd.h>

#include <string.h>

#include "shm_pubsub.h"

static int g_failures = 0;
#define CHECK(c)                                                                               \
  do {                                                                                         \
    if (!(c)) g_failures++;                                                                    \
  } while (0)

int main(void) {
  char name[64];
  snprintf(name, sizeof(name), "/shm_pubsub_c_%d", (int)getpid());
  shm_pubsub_topic* topic = NULL;
  CHECK(shm_pubsub_topic_open(name, &topic) == SHM_PUBSUB_OK);
  shm_pubsub_publisher* pub = NULL;
  shm_pubsub_subscriber* sub = NULL;
  CHECK(shm_pubsub_publisher_create(topic, &pub) == SHM_PUBSUB_OK);
  CHECK(shm_pubsub_subscriber_create(topic, 0, 0, &sub) == SHM_PUBSUB_OK);

  void* data = NULL;
  uint32_t id = 0;
  CHECK(shm_pubsub_loan(pub, 4, &data, &id) == SHM_PUBSUB_OK);
  memcpy(data, "abcd", 4);
  CHECK(shm_pubsub_publish_loan(pub, id) == SHM_PUBSUB_OK);

  const void* view = NULL;
  size_t size = 0;
  uint32_t rid = 0;
  CHECK(shm_pubsub_take_loan(sub, &view, &size, &rid) == SHM_PUBSUB_OK);
  CHECK(size == 4 && memcmp(view, "abcd", 4) == 0);
  CHECK(shm_pubsub_release_read(sub, rid) == SHM_PUBSUB_OK);

  CHECK(shm_pubsub_publish_copy(pub, "xy", 2) == SHM_PUBSUB_OK);
  char buf[8];
  CHECK(shm_pubsub_receive(sub, buf, sizeof(buf), &size) == SHM_PUBSUB_OK && size == 2);
  CHECK(shm_pubsub_recover(topic) == SHM_PUBSUB_OK);
  CHECK(shm_pubsub_free_blocks(topic) == 256);

  shm_pubsub_subscriber_destroy(sub);
  shm_pubsub_publisher_destroy(pub);
  shm_pubsub_topic_close(topic);
  return g_failures ? 1 : 0;
}
