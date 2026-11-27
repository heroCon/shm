// Read-only inspector for a shm_pubsub region: slot liveness, queue depths,
// loans, drop counters, in-flight operations and pool usage.
// Usage: shmtop /name [--once] [--interval-ms N]
// Built for DefaultConfig; build a copy with your Config for other layouts.
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "shm_pubsub/shm_pubsub.hpp"

using Topic = shm_pubsub::Topic<>;

static const char* state_name(uint32_t state) {
  switch (state) {
    case 0:
      return "free";
    case 1:
      return "active";
    case 2:
      return "reclaim";
  }
  return "?";
}

static const char* op_name(uint32_t op) {
  static const char* names[] = {"-", "loan", "publish", "take", "release", "reclaim"};
  return op < sizeof(names) / sizeof(names[0]) ? names[op] : "?";
}

static void print(const std::string& name, const Topic::Snapshot& snap) {
  std::printf("%s  layout=%u hash=%016llx  blocks free %u/%u (%u bytes each)%s\n", name.c_str(),
              snap.layout_version, static_cast<unsigned long long>(snap.config_hash),
              snap.free_blocks, snap.block_count, snap.block_size,
              snap.free_list_consistent ? "" : "  FREE LIST INCONSISTENT");
  std::printf("%-4s %-4s %-7s %-4s %-7s %-5s %-6s %-6s %-5s %-10s %-10s %-10s %-10s %-12s\n",
              "slot", "kind", "state", "live", "pid", "group", "subscr", "queue", "loans",
              "published", "taken", "drop_new", "drop_old", "in_flight");
  for (const auto& s : snap.slots) {
    if (s.state == 0 && !s.live) continue;
    char op[32];
    if (s.op) {
      std::snprintf(op, sizeof(op), "%s#%u", op_name(s.op), s.op_block);
    } else {
      std::snprintf(op, sizeof(op), "-");
    }
    std::printf(
        "%-4u %-4s %-7s %-4s %-7u %-5u %-6s %-6llu %-5u %-10llu %-10llu %-10llu %-10llu %-12s\n",
        s.index, s.publisher ? "pub" : "sub", state_name(s.state), s.live ? "yes" : "DEAD", s.pid,
        s.group, s.publisher ? "-" : (s.subscribed ? "yes" : "no"),
        static_cast<unsigned long long>(s.queue_depth), s.held_loans,
        static_cast<unsigned long long>(s.published), static_cast<unsigned long long>(s.taken),
        static_cast<unsigned long long>(s.dropped_newest),
        static_cast<unsigned long long>(s.dropped_oldest), op);
  }
  for (const auto& g : snap.groups) {
    if (!g.members && !g.queue_depth && !g.dropped_newest && !g.dropped_oldest) continue;
    const char* policy = !g.policy_set                                     ? "unset"
                         : g.overflow == shm_pubsub::Overflow::kDropOldest ? "drop_oldest"
                                                                           : "drop_newest";
    std::printf(
        "group %u: members=%u mask=%#llx policy=%s queue=%llu drop_new=%llu drop_old=%llu\n", g.id,
        g.members, static_cast<unsigned long long>(g.member_mask), policy,
        static_cast<unsigned long long>(g.queue_depth),
        static_cast<unsigned long long>(g.dropped_newest),
        static_cast<unsigned long long>(g.dropped_oldest));
  }
}

int main(int argc, char** argv) {
  if (argc < 2 || argv[1][0] != '/') {
    std::fprintf(stderr, "usage: %s /name [--once] [--interval-ms N]\n", argv[0]);
    return 2;
  }
  const std::string name = argv[1];
  bool once = false;
  int interval_ms = 1000;
  for (int i = 2; i < argc; ++i) {
    if (std::strcmp(argv[i], "--once") == 0) {
      once = true;
    } else if (std::strcmp(argv[i], "--interval-ms") == 0 && i + 1 < argc) {
      interval_ms = std::atoi(argv[++i]);
    }
  }
  const bool tty = isatty(STDOUT_FILENO);
  for (;;) {
    Topic::Snapshot snap;
    const shm_pubsub::Status status = Topic::inspect(name, snap);
    if (status != shm_pubsub::Status::kOk) {
      std::fprintf(stderr, "%s: %s (missing region or different Config)\n", name.c_str(),
                   shm_pubsub::to_string(status));
      return 1;
    }
    if (!once && tty) std::printf("\033[H\033[2J");
    print(name, snap);
    if (once) return 0;
    std::fflush(stdout);
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  }
}
