#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <thread>

#include "shm_pubsub.h"

static int fail(const char* text) {
  std::cerr << text << '\n';
  return 1;
}
int main() {
  ShmPubSub::Options o;
  o.name = "/shm_pubsub_test_" + std::to_string(getpid());
  ShmPubSub::destroy(o.name);
  pid_t child = fork();
  if (child < 0) return fail("fork failed");
  if (child == 0) {
    try {
      ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
      sub.subscribe();
      char out[16];
      size_t n = 0;
      auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (std::chrono::steady_clock::now() < until) {
        auto r = sub.receive_result(out, sizeof(out), n);
        if (r.status == ShmPubSub::Status::OK)
          _exit(n == 6 && std::memcmp(out, "hello", 6) == 0 ? 0 : 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
      _exit(3);
    } catch (...) {
      _exit(4);
    }
  }
  try {
    ShmPubSub pub(ShmPubSub::PUBLISHER, o);
    for (int i = 0; i < 500 && pub.get_valid_subscriber_count() == 0; ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    auto r = pub.publish_result("hello", 6);
    if (!r.ok()) {
      kill(child, SIGKILL);
      return fail("publish failed");
    }
  } catch (const std::exception& e) {
    kill(child, SIGKILL);
    return fail(e.what());
  }
  int status = 0;
  waitpid(child, &status, 0);
  ShmPubSub::destroy(o.name);
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return fail("child failed");
  return 0;
}
