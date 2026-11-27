#include "shm_pubsub.h"

#include <cstring>
#include <new>
#include <string>

#include "shm_pubsub/shm_pubsub.hpp"

namespace {

using Topic = shm_pubsub::Topic<>;

int code(shm_pubsub::Status status) { return static_cast<int>(status); }

struct Publisher {
  Topic::Publisher impl;
  Topic::WriteLoan loans[Topic::kMaxLoansPerPublisher];
  explicit Publisher(Topic& topic) : impl(topic) {}
  Topic::WriteLoan* find(uint32_t id) {
    if (id >= Topic::kMaxLoansPerPublisher || !loans[id]) return nullptr;
    return &loans[id];
  }
};

static_assert(static_cast<int>(shm_pubsub::Status::kOk) == SHM_PUBSUB_OK, "status order");
static_assert(static_cast<int>(shm_pubsub::Status::kDeadLettered) == SHM_PUBSUB_DEAD_LETTERED,
              "status order");

struct Subscriber {
  Topic::Subscriber impl;
  Topic::ReadLoan loan;
  explicit Subscriber(Topic& topic, const shm_pubsub::SubscriberOptions& options)
      : impl(topic, options) {}
};

}  // namespace

struct shm_pubsub_topic {
  Topic impl;
  explicit shm_pubsub_topic(const std::string& name) : impl(name) {}
};
struct shm_pubsub_publisher {
  Publisher impl;
  explicit shm_pubsub_publisher(Topic& topic) : impl(topic) {}
};
struct shm_pubsub_subscriber {
  Subscriber impl;
  explicit shm_pubsub_subscriber(Topic& topic, const shm_pubsub::SubscriberOptions& options)
      : impl(topic, options) {}
};

int shm_pubsub_topic_open(const char* name, shm_pubsub_topic** out) {
  if (!name || !out) return SHM_PUBSUB_INVALID_ARGUMENT;
  try {
    *out = new shm_pubsub_topic(name);
  } catch (const std::invalid_argument&) {
    return SHM_PUBSUB_INVALID_ARGUMENT;
  } catch (const std::exception&) {
    return SHM_PUBSUB_NOT_SUPPORTED;
  }
  return SHM_PUBSUB_OK;
}

void shm_pubsub_topic_close(shm_pubsub_topic* topic) { delete topic; }

int shm_pubsub_collect_garbage(shm_pubsub_topic* topic) {
  if (!topic) return SHM_PUBSUB_INVALID_ARGUMENT;
  topic->impl.collect_garbage();
  return SHM_PUBSUB_OK;
}

int shm_pubsub_recover(shm_pubsub_topic* topic) {
  if (!topic) return SHM_PUBSUB_INVALID_ARGUMENT;
  return code(topic->impl.recover());
}

unsigned shm_pubsub_free_blocks(shm_pubsub_topic* topic) {
  return topic ? topic->impl.snapshot().free_blocks : 0;
}

int shm_pubsub_publisher_create(shm_pubsub_topic* topic, shm_pubsub_publisher** out) {
  if (!topic || !out) return SHM_PUBSUB_INVALID_ARGUMENT;
  try {
    *out = new shm_pubsub_publisher(topic->impl);
  } catch (const std::exception&) {
    return SHM_PUBSUB_BUSY;
  }
  return SHM_PUBSUB_OK;
}

void shm_pubsub_publisher_destroy(shm_pubsub_publisher* publisher) { delete publisher; }

int shm_pubsub_publish_copy(shm_pubsub_publisher* publisher, const void* data, size_t size) {
  if (!publisher) return SHM_PUBSUB_INVALID_ARGUMENT;
  return code(publisher->impl.impl.publish_copy(data, size).status);
}

int shm_pubsub_loan(shm_pubsub_publisher* publisher, size_t size, void** data, uint32_t* id) {
  if (!publisher || !data || !id) return SHM_PUBSUB_INVALID_ARGUMENT;
  for (uint32_t i = 0; i < Topic::kMaxLoansPerPublisher; ++i) {
    if (publisher->impl.loans[i]) continue;
    const int status = code(publisher->impl.impl.loan(size, publisher->impl.loans[i]));
    if (status != SHM_PUBSUB_OK) return status;
    *data = publisher->impl.loans[i].data();
    *id = i;
    return SHM_PUBSUB_OK;
  }
  return SHM_PUBSUB_LOAN_LIMIT;
}

int shm_pubsub_publish_loan(shm_pubsub_publisher* publisher, uint32_t id) {
  if (!publisher) return SHM_PUBSUB_INVALID_ARGUMENT;
  Topic::WriteLoan* loan = publisher->impl.find(id);
  if (!loan) return SHM_PUBSUB_INVALID_ARGUMENT;
  const int status = code(publisher->impl.impl.publish(std::move(*loan)).status);
  return status;
}

int shm_pubsub_release_loan(shm_pubsub_publisher* publisher, uint32_t id) {
  if (!publisher) return SHM_PUBSUB_INVALID_ARGUMENT;
  Topic::WriteLoan* loan = publisher->impl.find(id);
  if (!loan) return SHM_PUBSUB_INVALID_ARGUMENT;
  loan->reset();
  return SHM_PUBSUB_OK;
}

int shm_pubsub_subscriber_create(shm_pubsub_topic* topic, uint32_t group, uint32_t overflow,
                                 shm_pubsub_subscriber** out) {
  if (!topic || !out) return SHM_PUBSUB_INVALID_ARGUMENT;
  shm_pubsub::SubscriberOptions options;
  options.group = group;
  options.overflow = static_cast<shm_pubsub::Overflow>(overflow);
  try {
    *out = new shm_pubsub_subscriber(topic->impl, options);
  } catch (const std::invalid_argument&) {
    return SHM_PUBSUB_INVALID_ARGUMENT;
  } catch (const std::exception&) {
    return SHM_PUBSUB_BUSY;
  }
  return SHM_PUBSUB_OK;
}

void shm_pubsub_subscriber_destroy(shm_pubsub_subscriber* subscriber) { delete subscriber; }

int shm_pubsub_receive(shm_pubsub_subscriber* subscriber, void* buffer, size_t capacity,
                       size_t* size) {
  if (!subscriber || !size) return SHM_PUBSUB_INVALID_ARGUMENT;
  const shm_pubsub::ReceiveResult result = subscriber->impl.impl.receive(buffer, capacity);
  *size = result.size;
  return code(result.status);
}

int shm_pubsub_wait_receive(shm_pubsub_subscriber* subscriber, void* buffer, size_t capacity,
                            size_t* size, int timeout_ms) {
  if (!subscriber || !size) return SHM_PUBSUB_INVALID_ARGUMENT;
  const shm_pubsub::ReceiveResult result =
      subscriber->impl.impl.wait_receive(buffer, capacity, std::chrono::milliseconds(timeout_ms));
  *size = result.size;
  return code(result.status);
}

int shm_pubsub_take_loan(shm_pubsub_subscriber* subscriber, const void** data, size_t* size,
                         uint32_t* id) {
  if (!subscriber || !data || !size || !id) return SHM_PUBSUB_INVALID_ARGUMENT;
  if (subscriber->impl.loan) return SHM_PUBSUB_LOAN_LIMIT;
  const int status = code(subscriber->impl.impl.take(subscriber->impl.loan));
  if (status != SHM_PUBSUB_OK) return status;
  *data = subscriber->impl.loan.data();
  *size = subscriber->impl.loan.size();
  *id = 0;
  return SHM_PUBSUB_OK;
}

int shm_pubsub_release_read(shm_pubsub_subscriber* subscriber, uint32_t id) {
  if (!subscriber || id != 0 || !subscriber->impl.loan) return SHM_PUBSUB_INVALID_ARGUMENT;
  subscriber->impl.loan.reset();
  return SHM_PUBSUB_OK;
}
