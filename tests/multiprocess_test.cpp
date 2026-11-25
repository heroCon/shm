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
static int check_reclaim(const ShmPubSub::Options& o) {
  pid_t child = fork();
  if (child < 0) return fail("reclaim fork failed");
  if (child == 0) {
    try {
      ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
      sub.subscribe();
      raise(SIGSTOP);
      _exit(2);
    } catch (...) {
      _exit(3);
    }
  }
  int status = 0;
  const bool paused = waitpid(child, &status, WUNTRACED) == child && WIFSTOPPED(status);
  bool queued = paused;
  bool reclaimed = false;
  try {
    ShmPubSub pub(ShmPubSub::PUBLISHER, o);
    if (paused) {
      queued = pub.collect_garbage() == 0 && pub.get_valid_subscriber_count() == 1;
      for (size_t i = 0; i < BLOCK_COUNT; ++i)
        queued &= pub.publish_result("x", 1).status == ShmPubSub::Status::OK;
    }
    if (paused) {
      kill(child, SIGKILL);
      waitpid(child, nullptr, 0);
    }
    if (queued) {
      // The pool is full of queued references. Allocation pressure reaps the
      // dead reader, then the same region can accept another subscriber.
      ShmPubSub::WriteLoan sample;
      reclaimed =
          pub.loan(1, sample) == ShmPubSub::Status::OK && pub.get_valid_subscriber_count() == 0;
      sample.reset();
      ShmPubSub replacement(ShmPubSub::SUBSCRIBER, o);
      replacement.subscribe();
      ShmPubSub::ReadLoan read;
      reclaimed &= replacement.take(read) == ShmPubSub::Status::WOULD_BLOCK;
      for (size_t i = 0; i < BLOCK_COUNT; ++i)
        reclaimed &= pub.publish_result("y", 1).status == ShmPubSub::Status::OK;
    }
  } catch (const std::exception& e) {
    if (waitpid(child, nullptr, WNOHANG) == 0) {
      kill(child, SIGKILL);
      waitpid(child, nullptr, 0);
    }
    return fail(e.what());
  }
  return queued && reclaimed ? 0 : fail("paused or killed reader reclamation");
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
      const auto start = std::chrono::steady_clock::now();
      auto r = sub.receive_wait(out, sizeof(out), n, std::chrono::seconds(5));
      const bool woke = std::chrono::steady_clock::now() - start < std::chrono::seconds(4);
      const bool received =
          r.status == ShmPubSub::Status::OK && n == 6 && std::memcmp(out, "hello", 6) == 0;
      _exit(woke && received ? 0 : 2);
    } catch (...) {
      _exit(4);
    }
  }
  try {
    ShmPubSub pub(ShmPubSub::PUBLISHER, o);
    for (int i = 0; i < 500 && pub.get_valid_subscriber_count() == 0; ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto r = pub.publish_result("hello", 6);
    if (r.status != ShmPubSub::Status::OK || r.delivered != 1) {
      kill(child, SIGKILL);
      waitpid(child, nullptr, 0);
      ShmPubSub::destroy(o.name);
      return fail("publish failed");
    }
  } catch (const std::exception& e) {
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    ShmPubSub::destroy(o.name);
    return fail(e.what());
  }
  int status = 0;
  waitpid(child, &status, 0);
  ShmPubSub::destroy(o.name);
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return fail("child failed");
  int result = check_reclaim(o);
  ShmPubSub::destroy(o.name);
  return result;
}
