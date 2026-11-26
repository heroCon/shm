#include <chrono>
#include <thread>

#include "test_util.hpp"

using shm_pubsub::Status;
using shm_pubsub::WaitSet;
using SmallTopic = shm_pubsub::Topic<SmallConfig>;
using Topic = shm_pubsub::Topic<>;

int main() {
  if (!WaitSet::supported()) {
    std::printf("futex_waitv not supported by this kernel; skipping\n");
    return 0;
  }
  ScopedName name_a("waitset_a");
  ScopedName name_b("waitset_b");
  shm_pubsub::Options oa, ob;
  oa.name = name_a.name;
  ob.name = name_b.name;
  // Two topics with different configurations share one wait set.
  Topic a(oa);
  SmallTopic b(ob);
  Topic::Subscriber sa(a);
  SmallTopic::Subscriber sb(b);
  Topic::Publisher pa(a);
  SmallTopic::Publisher pb(b);
  WaitSet set;
  CHECK(set.add(sa) && set.add(sb) && set.size() == 2);

  size_t index = 99;
  CHECK(set.wait(std::chrono::milliseconds(-1), &index) == Status::kInvalidArgument);
  CHECK(set.wait(std::chrono::milliseconds(20), &index) == Status::kTimedOut);

  std::thread later([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    pb.publish_copy("b", 1);
  });
  const auto start = std::chrono::steady_clock::now();
  CHECK(set.wait(std::chrono::seconds(5), &index) == Status::kOk && index == 1);
  CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds(4));
  later.join();
  SmallTopic::ReadLoan lb;
  CHECK(sb.take(lb) == Status::kOk && lb.data()[0] == 'b');

  pa.publish_copy("a", 1);
  CHECK(set.wait(std::chrono::seconds(1), &index) == Status::kOk && index == 0);
  Topic::ReadLoan la;
  CHECK(sa.take(la) == Status::kOk);

  std::thread stopper([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    sa.stop();
  });
  CHECK(set.wait(std::chrono::seconds(5), &index) == Status::kStopped && index == 0);
  stopper.join();
  return finish("wait_set_test");
}
