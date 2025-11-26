#include "shm_pubsub.h"
#include <thread>

int main() {
    ShmPubSub pub(ShmPubSub::PUBLISHER);
    std::string msg = "Hello PubSub! Count: ";
    size_t count = 0;

    // 每隔1秒发布一条数据
    while (true) {
        std::string data = msg + std::to_string(count++);
        bool ret = pub.publish(data.c_str(), data.size());
        if (ret) {
            std::cout << "Publisher " << getpid() << " published: " << data << std::endl;
        } else {
            std::cerr << "Publisher " << getpid() << " publish failed" << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}