// Cross-process latency: a publisher process stamps each loan with
// CLOCK_MONOTONIC; a subscriber process records receive - send.
// Usage: latency_benchmark [count] [bytes] [futex|spin] [pub_cpu] [sub_cpu] [interval_us]
#include <sched.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "shm_pubsub/shm_pubsub.hpp"

using Topic = shm_pubsub::Topic<>;
using shm_pubsub::Status;

static void pin(int cpu) {
  if (cpu < 0) return;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  sched_setaffinity(0, sizeof(set), &set);
}

int main(int argc, char** argv) {
  const size_t count = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 20000;
  const size_t bytes = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 64;
  const bool spin = argc > 3 && std::string(argv[3]) == "spin";
  const int pub_cpu = argc > 4 ? std::atoi(argv[4]) : -1;
  const int sub_cpu = argc > 5 ? std::atoi(argv[5]) : -1;
  const unsigned interval_us = argc > 6 ? static_cast<unsigned>(std::atoi(argv[6])) : 100;
  if (bytes < sizeof(uint64_t) || bytes > Topic::max_payload_size() || count == 0) return 2;

  shm_pubsub::Options o;
  o.name = "/shm_pubsub_latency_" + std::to_string(getpid());
  Topic::destroy(o.name);
  int ready[2];
  if (pipe(ready) != 0) return 3;
  const pid_t child = fork();
  if (child == 0) {
    pin(sub_cpu);
    Topic topic(o);
    Topic::Subscriber sub(topic);
    char x = 1;
    if (write(ready[1], &x, 1) != 1) _exit(4);
    std::vector<uint64_t> samples;
    samples.reserve(count);
    while (samples.size() < count) {
      Topic::ReadLoan loan;
      const Status s = spin ? sub.take(loan) : sub.wait_take(loan, std::chrono::seconds(2));
      if (s == Status::kOk) {
        uint64_t sent;
        std::memcpy(&sent, loan.data(), sizeof(sent));
        samples.push_back(shm_pubsub::detail::monotonic_ns() - sent);
      } else if (s == Status::kTimedOut) {
        break;
      }
    }
    std::sort(samples.begin(), samples.end());
    auto q = [&](double f) {
      return samples.empty() ? 0.0 : samples[static_cast<size_t>(f * (samples.size() - 1))] / 1e3;
    };
    std::printf("transport,mode,message_bytes,messages,p50_us,p99_us,p999_us,max_us\n");
    std::printf("shm,%s,%zu,%zu,%.3f,%.3f,%.3f,%.3f\n", spin ? "spin" : "futex", bytes,
                samples.size(), q(0.5), q(0.99), q(0.999), q(1.0));
    std::fflush(stdout);
    _exit(samples.size() == count ? 0 : 5);
  }
  char x;
  if (read(ready[0], &x, 1) != 1) return 6;
  pin(pub_cpu);
  {
    Topic topic(o);
    Topic::Publisher pub(topic);
    for (size_t i = 0; i < count;) {
      Topic::WriteLoan loan;
      if (pub.loan(bytes, loan) != Status::kOk) continue;
      const uint64_t now = shm_pubsub::detail::monotonic_ns();
      std::memcpy(loan.data(), &now, sizeof(now));
      if (pub.publish(std::move(loan)).status == Status::kOk) ++i;
      usleep(interval_us);
    }
  }
  int status = 0;
  waitpid(child, &status, 0);
  Topic::destroy(o.name);
  return WIFEXITED(status) ? WEXITSTATUS(status) : 7;
}
