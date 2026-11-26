// One thread waits on two topics without polling (Linux 5.16+).
#include <chrono>
#include <iostream>
#include <thread>

#include "shm_pubsub/shm_pubsub.hpp"

int main() {
  using Topic = shm_pubsub::Topic<>;
  if (!shm_pubsub::WaitSet::supported()) {
    std::cout << "futex_waitv is not available on this kernel\n";
    return 0;
  }
  shm_pubsub::Options imu, camera;
  imu.name = "/shm_pubsub_example_imu";
  camera.name = "/shm_pubsub_example_camera";
  imu.unlink_on_destroy = camera.unlink_on_destroy = true;
  Topic imu_topic(imu), camera_topic(camera);
  Topic::Subscriber imu_sub(imu_topic), camera_sub(camera_topic);
  Topic::Publisher imu_pub(imu_topic), camera_pub(camera_topic);

  shm_pubsub::WaitSet set;
  set.add(imu_sub);
  set.add(camera_sub);
  std::thread producer([&] {
    for (int i = 0; i < 3; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      (i % 2 ? camera_pub : imu_pub).publish_copy("frame", 6);
    }
  });
  Topic::Subscriber* subs[] = {&imu_sub, &camera_sub};
  const char* names[] = {"imu", "camera"};
  for (int received = 0; received < 3;) {
    size_t index = 0;
    if (set.wait(std::chrono::seconds(1), &index) != shm_pubsub::Status::kOk) break;
    Topic::ReadLoan loan;
    while (subs[index]->take(loan) == shm_pubsub::Status::kOk) {
      std::cout << names[index] << ": " << loan.data() << '\n';
      loan.reset();
      ++received;
    }
  }
  producer.join();
}
