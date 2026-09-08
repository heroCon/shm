#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

#include "shm_pubsub.h"
int main(int argc, char** argv) {
  size_t count = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 100000,
         bytes = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 64;
  if (bytes == 0 || bytes > 4072) return 2;
  ShmPubSub::Options o;
  o.name = "/shm_benchmark_" + std::to_string(getpid());
  o.unlink_on_destroy = true;
  ShmPubSub sub(ShmPubSub::SUBSCRIBER, o);
  sub.subscribe();
  ShmPubSub pub(ShmPubSub::PUBLISHER, o);
  std::string payload(bytes, 'x'), out(bytes, '\0');
  size_t received = 0, n = 0, dropped = 0;
  auto start = std::chrono::steady_clock::now();
  while (received < count) {
    if (pub.publish_result(payload.data(), payload.size(), ShmPubSub::COMPETING).status ==
        ShmPubSub::Status::QUEUE_FULL)
      ++dropped;
    auto r = sub.receive_result(&out[0], out.size(), n);
    if (r.status == ShmPubSub::Status::OK) ++received;
  }
  auto ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
          .count();
  std::cout << "transport,message_bytes,messages,seconds,msg_per_sec,dropped\nshm," << bytes << ','
            << count << ',' << ns / 1e9 << ',' << count / (ns / 1e9) << ',' << dropped << '\n';
}
