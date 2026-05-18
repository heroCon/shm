#include "shm_pubsub.h"
#include <thread>

#include "TestTopic.h"
#include "DelayTime.h"

int main() {
    ShmPubSub sub(ShmPubSub::SUBSCRIBER);
    sub.subscribe();  // 订阅数据

    DelayTime delay;
    TestTopic *buf = new TestTopic;
    size_t actual_len = 0;

    // 循环接收数据（非阻塞）
    while (true) {
        if (sub.receive(buf, sizeof(*buf), actual_len)) {
            // delay.update(buf->timestamp);
            std::cout << "Received: " << buf->timestamp << std::endl;
            memset(buf, 0, sizeof(*buf));
        }
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    return 0;
}