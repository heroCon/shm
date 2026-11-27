// Kills random endpoints while the others keep publishing and taking.
// args: seconds [all|pubs] [wait|poll] [none|gc|recover]
// With no args, runs the two modes the stall bound is claimed for.
// SHM_PUBSUB_CHAOS_SECONDS overrides the duration.
#include <signal.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>

#include "test_util.hpp"

using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;
using Clock = std::chrono::steady_clock;

namespace {

enum { kRoles = 5, kPubs = 2, kReaders = 3 };
const char* const kRoleName[kRoles] = {"pub0", "pub1", "private/drop_newest", "private/drop_oldest",
                                       "group"};
const double kStallBoundMs = 500;

struct Shared {
  std::atomic<uint64_t> progress[kRoles];
  std::atomic<uint64_t> corrupt{0};
  std::atomic<uint64_t> changed_while_held{0};
  std::atomic<uint64_t> exceptions{0};
};

Shared* g = nullptr;
bool g_poll = false;

const uint32_t kMagic = 0x5ca1ab1e;

void fill(char* p, size_t size, uint32_t who, uint64_t seq) {
  std::memcpy(p, &kMagic, 4);
  std::memcpy(p + 4, &who, 4);
  std::memcpy(p + 8, &seq, 8);
  for (size_t i = 16; i < size; ++i) p[i] = static_cast<char>(seq * 131 + i);
}

bool valid(const char* p, size_t size) {
  uint32_t magic = 0;
  uint64_t seq = 0;
  if (size < 16 || !p) return false;
  std::memcpy(&magic, p, 4);
  std::memcpy(&seq, p + 8, 8);
  if (magic != kMagic) return false;
  for (size_t i = 16; i < size; ++i) {
    if (p[i] != static_cast<char>(seq * 131 + i)) return false;
  }
  return true;
}

[[noreturn]] void worker(int role, const shm_pubsub::Options& o) {
  prctl(PR_SET_PDEATHSIG, SIGKILL);
  try {
    Topic topic(o);
    std::mt19937 rng(static_cast<unsigned>(getpid()));
    if (role < kPubs) {
      Topic::Publisher pub(topic);
      for (uint64_t seq = 0;; ++seq) {
        const size_t size = 16 + rng() % 48;
        Topic::WriteLoan loan;
        if (pub.loan(size, loan) != Status::kOk) {
          sched_yield();
          continue;
        }
        fill(loan.data(), size, static_cast<uint32_t>(role), seq);
        if (pub.publish(std::move(loan)).delivered) g->progress[role].fetch_add(1);
      }
    }
    SubscriberOptions so;
    so.group = role == 4 ? 1 : 0;
    so.overflow = role == 3 ? Overflow::kDropOldest : Overflow::kDropNewest;
    Topic::Subscriber sub(topic, so);
    for (;;) {
      Topic::ReadLoan a;
      const Status s = g_poll ? sub.take(a) : sub.wait_take(a, std::chrono::milliseconds(20));
      if (s != Status::kOk) continue;
      if (!valid(a.data(), a.size())) g->corrupt.fetch_add(1);
      g->progress[role].fetch_add(1);
      if (rng() % 8 == 0) {
        char before[16];
        std::memcpy(before, a.data(), 16);
        Topic::ReadLoan b;
        if (sub.take(b) == Status::kOk) {
          if (!valid(b.data(), b.size())) g->corrupt.fetch_add(1);
          g->progress[role].fetch_add(1);
        }
        sched_yield();
        if (std::memcmp(before, a.data(), 16) != 0 || !valid(a.data(), a.size())) {
          g->changed_while_held.fetch_add(1);
        }
      }
    }
  } catch (const std::exception& e) {
    if (g->exceptions.fetch_add(1) < 3) {
      std::fprintf(stderr, "worker %s: %s\n", kRoleName[role], e.what());
    }
    _exit(3);
  }
  _exit(0);
}

bool run_mode(int seconds, bool pubs_only, bool poll, const char* maintenance) {
  g_poll = poll;
  void* mem =
      mmap(nullptr, sizeof(Shared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  g = new (mem) Shared();
  ScopedName name("chaos");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  pid_t pid[kRoles];
  auto spawn = [&](int role) {
    const pid_t c = fork();
    if (c == 0) worker(role, o);
    pid[role] = c;
  };
  for (int r = 0; r < kRoles; ++r) spawn(r);

  std::mt19937 rng(12345);
  long kills = 0;
  uint64_t last[kRoles] = {};
  Clock::time_point last_progress[kRoles];
  double max_stall_ms[kRoles] = {};
  const auto start = Clock::now();
  for (int r = 0; r < kRoles; ++r) last_progress[r] = start;
  auto next_kill = start + std::chrono::milliseconds(30);
  while (Clock::now() < start + std::chrono::seconds(seconds)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    const auto now = Clock::now();
    for (int r = 0; r < kRoles; ++r) {
      int st = 0;
      if (waitpid(pid[r], &st, WNOHANG) == pid[r]) spawn(r);
      const uint64_t v = g->progress[r].load();
      if (v != last[r]) {
        last[r] = v;
        last_progress[r] = now;
      }
      // A role counts as stalled only while its current process is alive.
      // Time spent dead, between the kill and the replacement's first
      // message, is not a stall of a live reader.
      const double stall =
          std::chrono::duration<double, std::milli>(now - last_progress[r]).count();
      if (stall > max_stall_ms[r]) max_stall_ms[r] = stall;
    }
    if (now >= next_kill) {
      const int victim = static_cast<int>(rng() % (pubs_only ? kPubs : kRoles));
      kill(pid[victim], SIGKILL);
      waitpid(pid[victim], nullptr, 0);
      ++kills;
      if (std::string(maintenance) != "none") topic.collect_garbage();
      if (std::string(maintenance) == "recover") topic.recover();
      spawn(victim);
      // The stall clock is not reset: a replacement that cannot read either
      // keeps the stall growing, which is the stuck-queue case.
      next_kill = now + std::chrono::milliseconds(5 + static_cast<int>(rng() % 20));
    }
  }
  for (int r = 0; r < kRoles; ++r) kill(pid[r], SIGKILL);
  for (int r = 0; r < kRoles; ++r) waitpid(pid[r], nullptr, 0);

  shm_pubsub::RecoveryReport rep;
  Status final_status = Status::kBusy;
  for (int i = 0; i < 100 && final_status != Status::kOk; ++i) {
    final_status = topic.recover(&rep);
    if (final_status != Status::kOk) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  bool works = true;
  {
    Topic::Publisher pub(topic);
    Topic::Subscriber priv(topic);
    SubscriberOptions so;
    so.group = 1;
    Topic::Subscriber member(topic, so);
    Topic::ReadLoan stale;
    while (member.take(stale) == Status::kOk) stale.reset();
    for (int i = 0; i < 8; ++i) {
      works &= pub.publish_copy("ok", 3).status == Status::kOk;
      Topic::ReadLoan a;
      Topic::ReadLoan b;
      works &= priv.take(a) == Status::kOk && member.take(b) == Status::kOk;
    }
  }
  const uint32_t free_after = free_blocks(topic);
  double worst_reader = 0;
  for (int r = kPubs; r < kRoles; ++r) {
    if (max_stall_ms[r] > worst_reader) worst_reader = max_stall_ms[r];
  }
  std::printf(
      "readers=%s maintenance=%s: %ds %ld kills corrupt=%lu held=%lu exceptions=%lu "
      "worst_reader_stall=%.0fms free=%u/%u recover=%s works=%d\n",
      poll ? "poll" : "wait", maintenance, seconds, kills,
      static_cast<unsigned long>(g->corrupt.load()),
      static_cast<unsigned long>(g->changed_while_held.load()),
      static_cast<unsigned long>(g->exceptions.load()), worst_reader, free_after,
      Topic::kBlockCount, shm_pubsub::to_string(final_status), works);
  CHECK(g->corrupt.load() == 0);
  CHECK(g->changed_while_held.load() == 0);
  CHECK(g->exceptions.load() == 0);
  CHECK(final_status == Status::kOk);
  CHECK(rep.inconsistencies == 0);
  CHECK(free_after == Topic::kBlockCount);
  CHECK(works);
  CHECK(worst_reader <= kStallBoundMs);
  munmap(mem, sizeof(Shared));
  g = nullptr;
  return g_failures == 0;
}

}  // namespace

int main(int argc, char** argv) {
  int seconds = 2;
  if (const char* env = std::getenv("SHM_PUBSUB_CHAOS_SECONDS")) seconds = std::atoi(env);
  if (argc > 1) seconds = std::atoi(argv[1]);
  if (seconds < 1) seconds = 1;
  if (argc > 2) {
    const bool pubs_only = std::string(argv[2]) == "pubs";
    const bool poll = argc > 3 && std::string(argv[3]) == "poll";
    const char* maintenance = argc > 4 ? argv[4] : "gc";
    run_mode(seconds, pubs_only, poll, maintenance);
  } else {
    // Busy-poll readers with no supervisor, then blocking readers with
    // collect_garbage(). Both must keep every live reader moving.
    // Publishers are the victims. Readers stay up, so a stuck queue shows up
    // as a reader stall instead of being hidden by the reader's own death.
    run_mode(seconds, true, true, "none");
    run_mode(seconds, true, false, "gc");
  }
  return finish("chaos_test");
}
