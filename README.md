# shm_pubsub

面向 Linux 的 **C++11** 纯头文件共享内存发布/订阅库，核心特性是**崩溃可验证**：数据路径中 13 个具名位置的任意一处 `SIGKILL` 一个进程，存活进程都能收回全部共享块并继续收发。没有守护进程，没有库内后台线程，除 C++ 标准库与 Linux 外无其他依赖。

- **零拷贝**：发布方直接写入借出的共享块，订阅方原地读取。
- **广播与消费组同一机制**：每次发布投递给每个私有订阅者，以及每个消费组中的恰好一个成员。
- **容量有界**：容量为编译期参数，`static_assert` 保证慢速或停滞的订阅者不可能耗尽块池；每个队列可选 `kDropNewest` 或 `kDropOldest`。
- **精确唤醒**：每个队列有独立 futex 字；组发布只唤醒一个成员，私有队列只唤醒其所有者。`WaitSet` 借助 `futex_waitv` 同时等待多个订阅者，可跨 topic。
- **可观测**：`shmtop` 无需注册即可查看运行中共享区的存活状态、队列深度、借用与丢弃计数。

## 快速开始

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/subscriber_example /demo &
./build/publisher_example /demo
./build/shmtop /demo --once
```

```cpp
#include "shm_pubsub/shm_pubsub.hpp"

shm_pubsub::Options options;
options.name = "/demo";
shm_pubsub::Topic<> topic(options);             // 创建或附加
shm_pubsub::Topic<>::Publisher pub(topic);
shm_pubsub::Topic<>::Subscriber sub(topic);     // 私有队列，默认已订阅

shm_pubsub::Topic<>::WriteLoan write;
if (pub.loan(5, write) == shm_pubsub::Status::kOk) {
  std::memcpy(write.data(), "hello", 5);
  pub.publish(std::move(write));                // PublishResult：targets、delivered、丢弃数
}

shm_pubsub::Topic<>::ReadLoan read;
if (sub.wait_take(read, std::chrono::milliseconds(100)) == shm_pubsub::Status::kOk) {
  // read.data()、read.size()、read.sequence()、read.timestamp_ns()、read.publisher_id()
}
```

竞争消费者通过 `SubscriberOptions o; o.group = 1;` 加入消费组。容量由 `Config` 类型给出（`Topic<MyConfig>`）。附加到同一共享区的所有进程必须使用相同的 `Config`，共享区头部保存其哈希并拒绝不一致的附加。

## 在 CMake 中使用

```cmake
find_package(shm_pubsub 0.4 REQUIRED)     # 先执行 cmake --install
# 或：add_subdirectory(shm_pubsub)
target_link_libraries(app PRIVATE shm_pubsub::shm_pubsub)
```

作为子项目引入时只定义库目标；示例、测试和工具仅在顶层构建（`SHM_PUBSUB_BUILD_*` 选项）。

## 差异化定位

| | shm_pubsub | iceoryx2 | eCAL | cpp-ipc |
|---|---|---|---|---|
| 集成方式 | 一个头文件目录，C++11 | Rust 核心，C/C++ 绑定 | 运行时、服务与监控工具 | 需编译的库 |
| 中心守护进程 | 无 | 无 | 无（每进程有注册线程） | 无 |
| 消费组（工作队列）与广播并存 | 是，同一机制 | 否 | 否 | route 与 channel 两种类型 |
| 按故障点验证崩溃恢复 | 是，CI 中执行 | 失效节点清理 | 不适用 | 未见文档说明 |

详细对比及各方案更合适的场景见 [docs/comparison.md](docs/comparison.md)。

## 文档

- [接口语义](docs/api-semantics.md)：状态码、所有权、顺序、溢出策略。
- [生命周期](docs/lifecycle.md)：创建、端点存活、回收、`fork()`。
- [崩溃一致性](docs/crash-consistency.md)：可恢复的内容、故障矩阵与剩余窗口。
- [无锁设计](docs/lock-free-design.md)与平台要求。
- [性能](docs/benchmark.md)、[对比](docs/comparison.md)、[路线图](ROADMAP.md)、[更新记录](CHANGELOG.md)（含 0.2 → 0.3、0.3 → 0.4 迁移）、[贡献指南](CONTRIBUTING.md)。

## 要求与限制

Linux 3.15+（OFD 锁、进程共享 futex）；`WaitSet` 需要 5.16+ 的 `futex_waitv`，否则返回 `kNotSupported`。所有进程须使用相同的体系结构、ABI 与 `Config`。投递仅在内存中且至多一次，不持久化、不回放历史。端点非线程安全，每个线程使用各自的端点。每个 topic 最多 56 个端点。

采用 MIT 许可证。`detail/lock_free_list.hpp` 源自 Eclipse iceoryx（Apache-2.0 OR MIT），见 [NOTICE](NOTICE)。

[English](README.en.md)
