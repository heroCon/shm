# shm_pubsub

一个面向 **Linux 的 C++11 IPC 库**，只需两个头文件，无守护进程和库内后台线程。固定大小共享内存块支持零拷贝广播、竞争消费和阻塞接收。

## 快速开始

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/subscriber /demo &
./build/publisher /demo
```

头文件库目标为 `shm_pubsub::shm_pubsub`。外部项目可 `add_subdirectory` 后执行 `target_link_libraries(app PRIVATE shm_pubsub::shm_pubsub)`。构造参数 `Options` 可配置 POSIX 共享内存名称。关闭时可用 `stop()` 唤醒阻塞的接收方。确认所有参与者都已脱离后，再调用 `ShmPubSub::destroy(name)`。

## 零拷贝与等待

发布方使用 `loan()` 获取共享块，直接写入最多 4072 字节后用 `publish_loan()` 交付。订阅方可以用 `take()` 原地读取，或用 `wait_take()` 阻塞等待。借用句柄在销毁时归还引用。已有的 `publish_result()`、`receive_result()` 可继续使用，拷贝接收也可换用 `receive_wait()`。

```cpp
ShmPubSub::WriteLoan write;
if (pub.loan(1, write) == ShmPubSub::Status::OK) {
  write.data()[0] = 'x';
  pub.publish_loan(std::move(write));
}
ShmPubSub::ReadLoan read;
if (sub.wait_take(read, std::chrono::milliseconds(250)) == ShmPubSub::Status::OK) {
  std::cout << read.data()[0] << '\n';
}
```

## 架构

```text
发布者 -> 固定块池 -> 每订阅者 MPMC 队列 -> 所有广播订阅者
                 `-> 共享 MPMC 队列 ------> 一个竞争消费者
```

`publish_result` 区分无订阅者、队列满、部分广播和超长消息；`receive_result` 区分暂无消息和缓冲区不足。详见[接口语义](docs/api-semantics.md)、[生命周期与恢复](docs/lifecycle.md)及[无锁平台约束](docs/lock-free-design.md)。

## 示例与性能

`broadcast_example` 展示结构化发布结果，`competing_example` 处理真实字节。运行 `./scripts/run-benchmarks.sh` 可生成共享内存与 Unix Domain Datagram 基线 CSV；计时方法与语义差异见[性能文档](docs/benchmark.md)。

## 支持范围与限制

支持 Linux 3.15+、参与进程一致 ABI、CMake 3.10+ 及运行时无锁原子类型。消息至多投递一次。退出订阅者的队列按需回收。持有共享块或正在更新队列时进程被杀，仍可能留下资源。升级到共享内存布局 4 时，请停掉旧进程后重建共享区，或统一使用新名称。另见[路线图](ROADMAP.md)、[更新记录](CHANGELOG.md)和[贡献指南](CONTRIBUTING.md)。项目采用 MIT 许可证；单独标注的自由链表采用 Apache-2.0 OR MIT。

[English](README.en.md)
