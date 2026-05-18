#include <cstdint>

struct TestTopic
{
    uint64_t timestamp;
    char data[1024 - sizeof(uint64_t)];
};
