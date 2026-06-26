#ifndef TEST_TOPIC_H_
#define TEST_TOPIC_H_

#include <cstdint>

struct TestTopic {
  uint64_t timestamp;
  char data[1024 - sizeof(uint64_t)];
};

#endif  // TEST_TOPIC_H_
