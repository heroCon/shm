// Regression for the 0.2 leak: a process killed while holding a loan lost
// its block forever. Holder bits make reclamation exact without recover().
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>

#include "test_util.hpp"

using shm_pubsub::Status;
using Topic = shm_pubsub::Topic<SmallConfig>;

enum class Holder { kWriter, kReader, kReaderWithBacklog };

static void crash_holding(Topic& topic, Topic::Publisher& pub, const shm_pubsub::Options& o,
                          Holder holder) {
  int ready[2];
  CHECK(pipe(ready) == 0);
  const pid_t child = fork();
  if (child == 0) {
    Topic local(o);
    char x = 1;
    if (holder == Holder::kWriter) {
      Topic::Publisher writer(local);
      Topic::WriteLoan a, b;
      x = writer.loan(1, a) == Status::kOk && writer.loan(1, b) == Status::kOk ? 1 : 2;
      if (write(ready[1], &x, 1) != 1) _exit(5);
      pause();
    }
    Topic::Subscriber sub(local);
    if (write(ready[1], &x, 1) != 1) _exit(5);
    Topic::ReadLoan loan;
    x = sub.wait_take(loan, std::chrono::seconds(5)) == Status::kOk ? 1 : 2;
    if (write(ready[1], &x, 1) != 1) _exit(5);
    pause();
  }
  char x = 0;
  CHECK(read(ready[0], &x, 1) == 1 && x == 1);
  if (holder != Holder::kWriter) {
    CHECK(pub.publish_copy("m", 1).status == Status::kOk);
    CHECK(read(ready[0], &x, 1) == 1 && x == 1);
    if (holder == Holder::kReaderWithBacklog) {
      for (int i = 0; i < 3; ++i) CHECK(pub.publish_copy("b", 1).ok());
    }
  }
  kill(child, SIGKILL);
  waitpid(child, nullptr, 0);
  close(ready[0]);
  close(ready[1]);
  CHECK(topic.collect_garbage() == 1);
}

int main() {
  ScopedName name("crash_leak");
  shm_pubsub::Options o;
  o.name = name.name;
  Topic topic(o);
  Topic::Publisher pub(topic);
  const Holder holders[] = {Holder::kWriter, Holder::kReader, Holder::kReaderWithBacklog};
  for (Holder holder : holders) {
    for (int i = 0; i < 40; ++i) {
      crash_holding(topic, pub, o, holder);
      CHECK(free_blocks(topic) == Topic::kBlockCount);
    }
  }
  return finish("crash_leak_test");
}
