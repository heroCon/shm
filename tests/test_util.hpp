#ifndef SHM_PUBSUB_TESTS_TEST_UTIL_HPP_
#define SHM_PUBSUB_TESTS_TEST_UTIL_HPP_

#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "shm_pubsub/shm_pubsub.hpp"

static int g_failures = 0;

#define CHECK(condition)                                                                 \
  do {                                                                                   \
    if (!(condition)) {                                                                  \
      std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #condition); \
      ++g_failures;                                                                      \
    }                                                                                    \
  } while (false)

inline std::string unique_name(const char* tag) {
  return std::string("/shm_pubsub_") + tag + "_" + std::to_string(getpid());
}

// Removes the region on scope exit, including on test failure.
struct ScopedName {
  explicit ScopedName(const char* tag) : name(unique_name(tag)) {
    shm_pubsub::Topic<>::destroy(name);
  }
  ~ScopedName() { shm_pubsub::Topic<>::destroy(name); }
  std::string name;
};

// A small configuration that makes pool and queue limits easy to reach.
struct SmallConfig : shm_pubsub::DefaultConfig {
  static constexpr uint32_t kBlockCount = 64;
  static constexpr uint32_t kQueueCapacity = 4;
  static constexpr uint32_t kMaxPublishers = 3;
  static constexpr uint32_t kMaxSubscribers = 6;
  static constexpr uint32_t kMaxGroups = 2;
};

template <class TopicType>
uint32_t free_blocks(TopicType& topic) {
  return topic.snapshot().free_blocks;
}

inline int finish(const char* test) {
  if (g_failures) std::fprintf(stderr, "%s: %d failure(s)\n", test, g_failures);
  return g_failures ? 1 : 0;
}

#endif  // SHM_PUBSUB_TESTS_TEST_UTIL_HPP_
