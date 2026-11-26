#ifndef SHM_PUBSUB_DETAIL_FAULT_INJECTION_HPP_
#define SHM_PUBSUB_DETAIL_FAULT_INJECTION_HPP_

#include <cstddef>

namespace shm_pubsub {
namespace detail {

// Every state transition that spans more than one atomic step has a named
// point. Builds that define SHM_PUBSUB_FAULT_INJECTION kill the process with
// SIGKILL on the Nth hit of the point named by SHM_PUBSUB_CRASH_AT=name:N.
constexpr const char* kFaultPoints[] = {
    "loan.after_pop",          "loan.after_claim",
    "publish.after_ref",       "queue.after_reserve",
    "publish.after_enqueue",   "publish.before_release",
    "drop_oldest.after_claim", "drop_oldest.after_release_cell",
    "take.after_claim",        "take.after_convert",
    "release.before_push",     "reclaim.after_pin_wait",
    "reclaim.after_mark",
};
constexpr size_t kFaultPointCount = sizeof(kFaultPoints) / sizeof(kFaultPoints[0]);

}  // namespace detail
}  // namespace shm_pubsub

#ifdef SHM_PUBSUB_FAULT_INJECTION

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>

namespace shm_pubsub {
namespace detail {

struct FaultSpec {
  std::string name;
  std::atomic<long> remaining{0};
  pid_t pid = 0;
};

// Parsed once per process so that forked children read their own setting.
// Fault-injection builds are single-threaded test harnesses.
inline FaultSpec& fault_spec() {
  static FaultSpec spec;
  if (spec.pid != getpid()) {
    spec.pid = getpid();
    spec.name.clear();
    spec.remaining.store(0);
    if (const char* env = std::getenv("SHM_PUBSUB_CRASH_AT")) {
      const std::string text(env);
      const size_t colon = text.rfind(':');
      spec.name = text.substr(0, colon);
      spec.remaining.store(colon == std::string::npos ? 1 : std::atol(text.c_str() + colon + 1));
    }
  }
  return spec;
}

inline void fault_point(const char* name) {
  FaultSpec& spec = fault_spec();
  if (spec.remaining.load(std::memory_order_relaxed) <= 0 || spec.name != name) return;
  if (spec.remaining.fetch_sub(1) == 1) kill(getpid(), SIGKILL);
}

}  // namespace detail
}  // namespace shm_pubsub

#define SHM_PUBSUB_FAULT_POINT(name) ::shm_pubsub::detail::fault_point(name)
#else
#define SHM_PUBSUB_FAULT_POINT(name) ((void)0)
#endif

#endif  // SHM_PUBSUB_DETAIL_FAULT_INJECTION_HPP_
