#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "shm_pubsub.h"
static int check(bool condition, const char* message) {
  if (!condition) std::cerr << message << '\n';
  return condition ? 0 : 1;
}
static int check_loans(const ShmPubSub::Options& o) {
  using Status = ShmPubSub::Status;
  ShmPubSub pub(ShmPubSub::PUBLISHER, o);
  ShmPubSub other_pub(ShmPubSub::PUBLISHER, o);
  ShmPubSub a(ShmPubSub::SUBSCRIBER, o);
  ShmPubSub b(ShmPubSub::SUBSCRIBER, o);
  int errors = 0;

  ShmPubSub::WriteLoan write;
  const size_t size = ShmPubSub::max_payload_size();
  errors += check(pub.loan(size, write) == Status::OK && write.size() == size, "maximum loan");
  if (!write) return errors;
  auto moved = std::move(write);
  errors += check(!write && moved, "move transfers the write loan");
  errors +=
      check(other_pub.publish_loan(std::move(moved)).status == Status::INVALID_ARGUMENT && moved,
            "wrong publisher leaves loan with its owner");
  errors += check(pub.publish_loan(std::move(moved)).status == Status::NO_SUBSCRIBERS && !moved,
                  "no-reader publish releases the loan");

  a.subscribe();
  b.subscribe();
  if (pub.loan(size, write) != Status::OK) return errors + check(false, "loan for broadcast");
  std::memset(write.data(), 'a', write.size());
  const auto result = pub.publish_loan(std::move(write));
  errors += check(result.status == Status::OK && result.delivered == 2 && !write,
                  "broadcast transfers write ownership");
  ShmPubSub::ReadLoan first, second;
  errors += check(a.take(first) == Status::OK && b.take(second) == Status::OK,
                  "both readers receive the broadcast");
  if (!first || !second) return errors;
  errors += check(first.size() == size && second.size() == size &&
                      std::memcmp(first.data(), second.data(), size) == 0 &&
                      second.data()[size - 1] == 'a',
                  "readers see the entire shared payload");

  // One retained broadcast reference keeps the block out of the pool.
  first.reset();
  std::vector<ShmPubSub::WriteLoan> pool(BLOCK_COUNT);
  for (size_t i = 0; i < BLOCK_COUNT - 1; ++i)
    errors += check(pub.loan(1, pool[i]) == Status::OK, "loan remaining pool capacity");
  errors += check(pub.loan(1, pool.back()) == Status::QUEUE_FULL, "held sample pins its block");
  second.reset();
  errors += check(pub.loan(1, pool.back()) == Status::OK, "last reference returns its block");
  pool.clear();

  errors += check(pub.publish_result("z", 1, ShmPubSub::COMPETING).status == Status::OK,
                  "legacy publish can feed a loan consumer");
  errors += check(a.take(first) == Status::OK && first.size() == 1 && first.data()[0] == 'z' &&
                      b.take(second) == Status::WOULD_BLOCK,
                  "competing has one consumer");
  first.reset();
  errors += check(a.wait_take(first, std::chrono::milliseconds(10)) == Status::TIMED_OUT,
                  "blocking receive times out when empty");
  Status waiter = Status::NOT_SUPPORTED;
  std::thread waiting([&] { waiter = b.wait_take(second, std::chrono::seconds(3)); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  b.stop();
  waiting.join();
  errors += check(waiter == Status::STOPPED, "stop cancels the blocked receiver");
  return errors;
}

int main() {
  ShmPubSub::Options o;
  o.name = "/shm_semantics_" + std::to_string(getpid());
  ShmPubSub::destroy(o.name);
  int errors = 0;
  {
    ShmPubSub pub(ShmPubSub::PUBLISHER, o);
    errors += check(pub.publish_result("x", 1).status == ShmPubSub::Status::NO_SUBSCRIBERS,
                    "no-subscriber status");
    ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
    sub.subscribe();
    std::string large(BLOCK_SIZE, 'x');
    auto too_large = pub.publish_result(large.data(), large.size());
    errors += check(too_large.status == ShmPubSub::Status::MESSAGE_TOO_LARGE, "oversize status");
    errors +=
        check(pub.publish_result("hello", 6).status == ShmPubSub::Status::OK, "publish status");
    char small[2];
    size_t actual = 0;
    auto receive = sub.receive_result(small, sizeof(small), actual);
    errors += check(receive.status == ShmPubSub::Status::BUFFER_TOO_SMALL && actual == 6,
                    "small-buffer status");
  }
  errors += check_loans(o);
  ShmPubSub::destroy(o.name);
  return errors ? 1 : 0;
}
