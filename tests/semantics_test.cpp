#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "test_util.hpp"

using shm_pubsub::Overflow;
using shm_pubsub::Status;
using shm_pubsub::SubscriberOptions;
using Topic = shm_pubsub::Topic<SmallConfig>;

static void check_copy_api(Topic& topic) {
  Topic::Publisher pub(topic);
  CHECK(pub.publish_copy("x", 1).status == Status::kNoSubscribers);
  Topic::Subscriber sub(topic);
  std::string large(Topic::max_payload_size() + 1, 'x');
  CHECK(pub.publish_copy(large.data(), large.size()).status == Status::kMessageTooLarge);
  CHECK(pub.publish_copy(nullptr, 1).status == Status::kInvalidArgument);
  const auto r = pub.publish_copy("hello", 6);
  CHECK(r.status == Status::kOk && r.targets == 1 && r.delivered == 1);
  char small[2];
  const auto too_small = sub.receive(small, sizeof(small));
  CHECK(too_small.status == Status::kBufferTooSmall && too_small.size == 6);
  CHECK(sub.receive(small, sizeof(small)).status == Status::kWouldBlock);
  char buf[16];
  CHECK(pub.publish_copy("world", 6).ok());
  const auto got = sub.receive(buf, sizeof(buf));
  CHECK(got.status == Status::kOk && got.size == 6 && std::strcmp(buf, "world") == 0);
}

static void check_loans(Topic& topic) {
  Topic::Publisher pub(topic);
  Topic::Publisher other(topic);
  Topic::Subscriber a(topic);
  Topic::Subscriber b(topic);

  Topic::WriteLoan write;
  const size_t size = Topic::max_payload_size();
  CHECK(pub.loan(0, write) == Status::kInvalidArgument);
  CHECK(pub.loan(size + 1, write) == Status::kMessageTooLarge);
  CHECK(pub.loan(size, write) == Status::kOk && write.size() == size);
  CHECK(pub.loan(1, write) == Status::kInvalidArgument);
  Topic::WriteLoan moved = std::move(write);
  CHECK(!write && moved);
  CHECK(other.publish(std::move(moved)).status == Status::kInvalidArgument && moved);
  std::memset(moved.data(), 'a', moved.size());
  CHECK(moved.resize(size - 1) && moved.size() == size - 1);
  CHECK(!moved.resize(size + 1));
  const auto r = pub.publish(std::move(moved));
  CHECK(r.status == Status::kOk && r.delivered == 2 && !moved);

  Topic::ReadLoan first, second;
  CHECK(a.take(first) == Status::kOk && b.take(second) == Status::kOk);
  CHECK(first.data() == second.data());
  CHECK(first.size() == size - 1 && first.data()[size - 2] == 'a');
  CHECK(first.publisher_id() == pub.slot());
  CHECK(first.timestamp_ns() > 0);
  const uint64_t seq = first.sequence();
  first.reset();
  second.reset();

  // Per-publisher sequence numbers let readers detect gaps.
  CHECK(pub.publish_copy("1", 1).ok() && pub.publish_copy("2", 1).ok());
  CHECK(a.take(first) == Status::kOk && first.sequence() == seq + 1);
  first.reset();
  CHECK(a.take(first) == Status::kOk && first.sequence() == seq + 2);
  first.reset();
  Topic::ReadLoan drain;
  while (b.take(drain) == Status::kOk) drain.reset();

  // Loan limits are per endpoint and bounded by the configuration.
  std::vector<Topic::WriteLoan> loans(Topic::kMaxLoansPerPublisher + 1);
  for (uint32_t i = 0; i < Topic::kMaxLoansPerPublisher; ++i) {
    CHECK(pub.loan(1, loans[i]) == Status::kOk);
  }
  CHECK(pub.loan(1, loans.back()) == Status::kLoanLimit);
  loans.clear();

  CHECK(pub.publish_copy("a", 1).ok() && pub.publish_copy("b", 1).ok() &&
        pub.publish_copy("c", 1).ok());
  std::vector<Topic::ReadLoan> reads(Topic::kMaxLoansPerSubscriber + 1);
  for (uint32_t i = 0; i < Topic::kMaxLoansPerSubscriber; ++i)
    CHECK(a.take(reads[i]) == Status::kOk);
  CHECK(a.take(reads.back()) == Status::kLoanLimit);
  reads.clear();
  while (a.take(drain) == Status::kOk) drain.reset();
  while (b.take(drain) == Status::kOk) drain.reset();

  // Unsubscribed endpoints neither receive nor take.
  b.unsubscribe();
  CHECK(pub.publish_copy("u", 1).delivered == 1);
  CHECK(b.take(drain) == Status::kNotSubscribed);
  b.subscribe();
  CHECK(b.take(drain) == Status::kWouldBlock);
  CHECK(a.take(drain) == Status::kOk);
  drain.reset();

  CHECK(a.wait_take(first, std::chrono::milliseconds(10)) == Status::kTimedOut);
  CHECK(a.wait_take(first, std::chrono::milliseconds(-1)) == Status::kInvalidArgument);
  Status waiter = Status::kNotSupported;
  std::thread waiting([&] { waiter = b.wait_take(second, std::chrono::seconds(3)); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  b.stop();
  waiting.join();
  CHECK(waiter == Status::kStopped);
  CHECK(b.wait_take(second, std::chrono::seconds(1)) == Status::kStopped);
}

static void check_groups(Topic& topic) {
  Topic::Publisher pub(topic);
  SubscriberOptions in_group;
  in_group.group = 1;
  Topic::Subscriber w1(topic, in_group);
  Topic::Subscriber w2(topic, in_group);
  Topic::Subscriber observer(topic);
  bool threw = false;
  try {
    SubscriberOptions bad;
    bad.group = Topic::kMaxGroups + 1;
    Topic::Subscriber invalid(topic, bad);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  CHECK(threw);

  const auto r = pub.publish_copy("job", 4);
  CHECK(r.status == Status::kOk && r.targets == 2 && r.delivered == 2);
  Topic::ReadLoan x, y, z;
  const Status s1 = w1.take(x);
  const Status s2 = w2.take(y);
  CHECK((s1 == Status::kOk) != (s2 == Status::kOk));
  CHECK(observer.take(z) == Status::kOk && z.size() == 4);
}

static void check_overflow(Topic& topic) {
  Topic::Publisher pub(topic);
  SubscriberOptions keep_newest;
  keep_newest.overflow = Overflow::kDropOldest;
  Topic::Subscriber newest(topic, keep_newest);
  Topic::Subscriber oldest(topic);
  for (int i = 0; i < 10; ++i) {
    const char c = static_cast<char>('0' + i);
    const auto r = pub.publish_copy(&c, 1);
    CHECK(r.targets == 2);
    if (i >= static_cast<int>(Topic::kQueueCapacity)) {
      CHECK(r.status == Status::kPartial && r.delivered == 1 && r.dropped_oldest == 1);
    }
  }
  Topic::ReadLoan loan;
  CHECK(newest.take(loan) == Status::kOk && loan.data()[0] == '6');
  loan.reset();
  CHECK(oldest.take(loan) == Status::kOk && loan.data()[0] == '0');
  loan.reset();
  const auto snap = topic.snapshot();
  CHECK(snap.slots[newest.slot()].dropped_oldest == 6);
  CHECK(snap.slots[oldest.slot()].dropped_newest == 6);
}

static void check_batch(Topic& topic) {
  Topic::Publisher pub(topic);
  Topic::Subscriber a(topic);
  Topic::Subscriber b(topic);
  Topic::WriteLoan loans[2];
  CHECK(pub.publish(loans, 2).status == Status::kInvalidArgument);
  for (int i = 0; i < 2; ++i) {
    CHECK(pub.loan(1, loans[i]) == Status::kOk);
    loans[i].data()[0] = static_cast<char>('a' + i);
  }
  CHECK(pub.publish(loans, 3).status == Status::kInvalidArgument);
  CHECK(loans[0] && loans[1]);
  const auto r = pub.publish(loans, 2);
  CHECK(r.status == Status::kOk && r.targets == 4 && r.delivered == 4);
  CHECK(!loans[0] && !loans[1]);
  Topic::ReadLoan got[3];
  CHECK(a.take(got, 3) == 2);
  CHECK(got[0].data()[0] == 'a' && got[1].data()[0] == 'b');
  CHECK(got[1].sequence() == got[0].sequence() + 1);
  CHECK(got[0].timestamp_ns() != 0 && got[0].timestamp_ns() == got[1].timestamp_ns());
  std::string seen;
  CHECK(b.drain([&](const Topic::ReadLoan& m) { seen += m.data()[0]; }) == 2);
  CHECK(seen == "ab");
}

struct NoTimestampConfig : SmallConfig {
  static constexpr bool kTimestamps = false;
};

static void check_no_timestamps() {
  ScopedName name("semantics_nots");
  shm_pubsub::Options o;
  o.name = name.name;
  shm_pubsub::Topic<NoTimestampConfig> topic(o);
  shm_pubsub::Topic<NoTimestampConfig>::Publisher pub(topic);
  shm_pubsub::Topic<NoTimestampConfig>::Subscriber sub(topic);
  CHECK(pub.publish_copy("x", 1).ok());
  shm_pubsub::Topic<NoTimestampConfig>::ReadLoan loan;
  CHECK(sub.take(loan) == Status::kOk && loan.timestamp_ns() == 0);
}

struct LargeConfig : SmallConfig {
  static constexpr uint32_t kLargeBlockCount = 4;
  static constexpr uint32_t kLargeBlockSize = 16384;
};

// Loans take the smallest class that fits; the classes have separate pools.
static void check_size_classes() {
  using Large = shm_pubsub::Topic<LargeConfig>;
  ScopedName name("semantics_large");
  shm_pubsub::Options o;
  o.name = name.name;
  Large topic(o);
  Large::Publisher pub(topic);
  Large::Subscriber sub(topic);
  CHECK(Large::max_payload_size() == Large::kMaxLargePayload);
  CHECK(Large::kMaxLargePayload > Large::kMaxPayload);
  Large::WriteLoan small, big;
  CHECK(pub.loan(Large::kMaxPayload, small) == Status::kOk);
  CHECK(small.capacity() == Large::kMaxPayload);
  CHECK(pub.loan(Large::kMaxPayload + 1, big) == Status::kOk);
  CHECK(big.capacity() == Large::kMaxLargePayload);
  CHECK(big.resize(Large::kMaxLargePayload) && !small.resize(Large::kMaxPayload + 1));
  std::memset(big.data(), 'L', big.size());
  small.reset();
  CHECK(pub.publish(std::move(big)).ok());
  Large::ReadLoan loan;
  CHECK(sub.take(loan) == Status::kOk);
  CHECK(loan.size() == Large::kMaxLargePayload && loan.data()[loan.size() - 1] == 'L');
  loan.reset();

  std::vector<char> payload(Large::kMaxLargePayload, 'p');
  uint32_t published = 0;
  for (uint32_t i = 0; i < Large::kLargeBlockCount + 2; ++i) {
    published += pub.publish_copy(payload.data(), payload.size()).ok();
  }
  CHECK(published == Large::kLargeBlockCount);
  CHECK(topic.snapshot().large_free_blocks == 0);
  CHECK(pub.publish_copy("x", 1).status == Status::kQueueFull);
  CHECK(pub.publish_copy(nullptr, Large::kMaxLargePayload + 1).status == Status::kInvalidArgument);
  uint32_t taken = 0;
  while (sub.take(loan) == Status::kOk) {
    taken += loan.size() == payload.size();
    loan.reset();
  }
  CHECK(taken == Large::kLargeBlockCount);
  shm_pubsub::RecoveryReport report;
  CHECK(topic.recover(&report) == Status::kOk && report.inconsistencies == 0);
  const auto snap = topic.snapshot();
  CHECK(snap.free_blocks == Large::kBlockCount &&
        snap.large_free_blocks == Large::kLargeBlockCount);
}

// An idle reader that busy-polls must not keep recover() from running.
static void check_recover_with_busy_poller(Topic& topic) {
  Topic::Subscriber sub(topic);
  std::atomic<bool> run{true};
  std::thread poller([&] {
    while (run.load(std::memory_order_relaxed)) {
      Topic::ReadLoan loan;
      sub.take(loan);
    }
  });
  int ok = 0;
  for (int i = 0; i < 200; ++i) ok += topic.recover() == Status::kOk;
  run = false;
  poller.join();
  CHECK(ok >= 190);
}

int main() {
  ScopedName name("semantics");
  shm_pubsub::Options options;
  options.name = name.name;
  {
    Topic topic(options);
    check_copy_api(topic);
    check_loans(topic);
    check_groups(topic);
    check_overflow(topic);
    check_batch(topic);
    check_recover_with_busy_poller(topic);
    CHECK(free_blocks(topic) == Topic::kBlockCount);
    bool threw = false;
    try {
      shm_pubsub::Topic<> wrong_config(options);
    } catch (const std::runtime_error&) {
      threw = true;
    }
    CHECK(threw);
  }
  check_no_timestamps();
  check_size_classes();
  bool threw = false;
  try {
    shm_pubsub::Options bad;
    bad.name = "no_slash";
    Topic topic(bad);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  CHECK(threw);
  return finish("semantics_test");
}
