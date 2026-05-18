#include "shm_pubsub.h"
#include <thread>

#include "TestTopic.h"
#include "DelayTime.h"

int main() {
    ShmPubSub pub(ShmPubSub::PUBLISHER);
    TestTopic *msg = new TestTopic;

    uint64_t count = 0;
    while (true) {
        msg->timestamp = count++;//DelayTime::get_time();
        bool ret = pub.publish(msg, sizeof(TestTopic));
        std::cout << "publish " << count << std::endl;
        if (!ret) {
            std::cerr << "publish failed" << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    return 0;
}