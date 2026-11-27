// Control-path cost against pool size: region creation, endpoint
// construction plus destruction, recover(), and collect_garbage() after one
// subscriber process was killed.
// Usage: control_path_benchmark
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <string>

#include "shm_pubsub/shm_pubsub.hpp"

using Clock = std::chrono::steady_clock;

static double elapsed_us(Clock::time_point start) {
  return std::chrono::duration<double, std::micro>(Clock::now() - start).count();
}

struct Blocks4k : shm_pubsub::DefaultConfig {
  static constexpr uint32_t kBlockCount = 4096;
};
struct Blocks16k : shm_pubsub::DefaultConfig {
  static constexpr uint32_t kBlockCount = 16384;
};
struct Blocks64kSmall : shm_pubsub::DefaultConfig {
  static constexpr uint32_t kBlockCount = 65536;
  static constexpr uint32_t kBlockSize = 256;
};

template <class Config>
void run(const char* label) {
  using Topic = shm_pubsub::Topic<Config>;
  shm_pubsub::Options o;
  o.name = "/shm_pubsub_control_" + std::to_string(getpid());
  Topic::destroy(o.name);
  Clock::time_point t = Clock::now();
  {
    Topic topic(o);
    const double create = elapsed_us(t);
    const int endpoints = 200;
    t = Clock::now();
    for (int i = 0; i < endpoints; ++i) {
      typename Topic::Subscriber s(topic);
    }
    const double endpoint = elapsed_us(t) / endpoints;
    const int recovers = 20;
    t = Clock::now();
    for (int i = 0; i < recovers; ++i) topic.recover();
    const double recover = elapsed_us(t) / recovers;
    int ready[2];
    if (pipe(ready) != 0) return;
    const pid_t child = fork();
    if (child == 0) {
      Topic inner(o);
      typename Topic::Subscriber s(inner);
      char x = 1;
      if (write(ready[1], &x, 1) != 1) _exit(1);
      pause();
      _exit(0);
    }
    char x;
    if (read(ready[0], &x, 1) != 1) return;
    close(ready[0]);
    close(ready[1]);
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    t = Clock::now();
    const size_t reclaimed = topic.collect_garbage();
    const double gc = elapsed_us(t);
    std::printf("%s,%u,%u,%.0f,%.1f,%.1f,%zu,%.1f\n", label, Topic::kBlockCount, Topic::kBlockSize,
                create, endpoint, recover, reclaimed, gc);
  }
  Topic::destroy(o.name);
}

int main() {
  std::printf(
      "config,blocks,block_bytes,create_us,endpoint_ctor_dtor_us,recover_us,reclaimed,"
      "collect_garbage_us\n");
  run<shm_pubsub::DefaultConfig>("default");
  run<Blocks4k>("4k_blocks");
  run<Blocks16k>("16k_blocks");
  run<Blocks64kSmall>("64k_small_blocks");
}
