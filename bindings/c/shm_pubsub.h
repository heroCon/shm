#ifndef SHM_PUBSUB_H_
#define SHM_PUBSUB_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Status values, same order as shm_pubsub::Status. */
enum {
  SHM_PUBSUB_OK = 0,
  SHM_PUBSUB_NO_SUBSCRIBERS,
  SHM_PUBSUB_PARTIAL,
  SHM_PUBSUB_QUEUE_FULL,
  SHM_PUBSUB_LOAN_LIMIT,
  SHM_PUBSUB_POOL_EXHAUSTED,
  SHM_PUBSUB_MESSAGE_TOO_LARGE,
  SHM_PUBSUB_INVALID_ARGUMENT,
  SHM_PUBSUB_WOULD_BLOCK,
  SHM_PUBSUB_BUFFER_TOO_SMALL,
  SHM_PUBSUB_NOT_SUBSCRIBED,
  SHM_PUBSUB_CORRUPTED,
  SHM_PUBSUB_NOT_SUPPORTED,
  SHM_PUBSUB_TIMED_OUT,
  SHM_PUBSUB_STOPPED,
  SHM_PUBSUB_BUSY,
  SHM_PUBSUB_DEAD_LETTERED
};

typedef struct shm_pubsub_topic shm_pubsub_topic;
typedef struct shm_pubsub_publisher shm_pubsub_publisher;
typedef struct shm_pubsub_subscriber shm_pubsub_subscriber;

int shm_pubsub_topic_open(const char* name, shm_pubsub_topic** out);
void shm_pubsub_topic_close(shm_pubsub_topic* topic);
int shm_pubsub_collect_garbage(shm_pubsub_topic* topic);
int shm_pubsub_recover(shm_pubsub_topic* topic);
unsigned shm_pubsub_free_blocks(shm_pubsub_topic* topic);

int shm_pubsub_publisher_create(shm_pubsub_topic* topic, shm_pubsub_publisher** out);
void shm_pubsub_publisher_destroy(shm_pubsub_publisher* publisher);
int shm_pubsub_publish_copy(shm_pubsub_publisher* publisher, const void* data, size_t size);
/* Zero-copy loan. *id is a handle, *data is valid until publish or release. */
int shm_pubsub_loan(shm_pubsub_publisher* publisher, size_t size, void** data, uint32_t* id);
int shm_pubsub_publish_loan(shm_pubsub_publisher* publisher, uint32_t id);
int shm_pubsub_release_loan(shm_pubsub_publisher* publisher, uint32_t id);

/* group 0 is a private queue. overflow: 0 drop newest, 1 drop oldest, 2 block. */
int shm_pubsub_subscriber_create(shm_pubsub_topic* topic, uint32_t group, uint32_t overflow,
                                 shm_pubsub_subscriber** out);
void shm_pubsub_subscriber_destroy(shm_pubsub_subscriber* subscriber);
int shm_pubsub_receive(shm_pubsub_subscriber* subscriber, void* buffer, size_t capacity,
                       size_t* size);
int shm_pubsub_wait_receive(shm_pubsub_subscriber* subscriber, void* buffer, size_t capacity,
                            size_t* size, int timeout_ms);
int shm_pubsub_take_loan(shm_pubsub_subscriber* subscriber, const void** data, size_t* size,
                         uint32_t* id);
int shm_pubsub_release_read(shm_pubsub_subscriber* subscriber, uint32_t id);

#ifdef __cplusplus
}
#endif

#endif
