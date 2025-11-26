#include "shm_pubsub.h"
#include <thread>

int main() {
    ShmPubSub sub(ShmPubSub::SUBSCRIBER);
    sub.subscribe();  // 订阅数据

    char buf[BLOCK_SIZE] = {0};
    size_t actual_len = 0;

    // 循环接收数据（非阻塞）
    while (true) {
        if (sub.receive(buf, sizeof(buf), actual_len)) {
            std::cout << "Subscriber " << getpid() << " received: " << std::string(buf, actual_len) << std::endl;
            memset(buf, 0, sizeof(buf));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    return 0;
}