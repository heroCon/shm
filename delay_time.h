#ifndef DELAY_TIME_H_
#define DELAY_TIME_H_

#include <chrono>
#include <cstdint>
#include <iostream>

class DelayTime {
 public:
  DelayTime() : count_(0), sum_(0) {}

  // 获取当前时间戳（微秒）
  static uint64_t get_time() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
  }

  // 更新延时统计，接收发送时间戳
  void update(uint64_t send_time) {
    // 获取当前时间
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    uint64_t receive_time = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

    // 计算延时并累加
    uint64_t delay = receive_time - send_time;
    sum_ += delay;
    count_++;
    std::cout << "Average: " << get_average_delay() << " us, cur:" << delay << std::endl;
  }

  // 获取平均延时（微秒）
  double get_average_delay() const {
    if (count_ == 0) {
      return 0.0;
    }
    return sum_ / count_;
  }

  // 获取统计次数
  int get_count() const { return count_; }

  // 重置统计
  void reset() {
    count_ = 0;
    sum_ = 0;
  }

 private:
  int count_;   // 统计次数
  double sum_;  // 延时总和（微秒）
};

#endif  // DELAY_TIME_H_
