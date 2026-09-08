# shm_pubsub

一个面向 **Linux 的实验性 IPC 头文件库**，使用固定大小共享内存块提供广播与竞争消费。0.2 版明确了生命周期和故障边界；它不是持久化消息中间件。

## 快速开始

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/subscriber /demo &
./build/publisher /demo
```

头文件库目标为 `shm_pubsub::shm_pubsub`。外部项目可 `add_subdirectory` 后执行 `target_link_libraries(app PRIVATE shm_pubsub::shm_pubsub)`。构造参数 `Options` 可配置 POSIX 共享内存名称。正常退出调用 `stop()`；确认所有参与者都已脱离后，才调用 `ShmPubSub::destroy(name)`。

## 架构

```text
发布者 -> 固定块池 -> 每订阅者 MPMC 队列 -> 所有广播订阅者
                 `-> 共享 MPMC 队列 ------> 一个竞争消费者
```

`publish_result` 区分无订阅者、队列满、部分广播和超长消息；`receive_result` 区分暂无消息和缓冲区不足。详见[接口语义](docs/api-semantics.md)、[生命周期与恢复](docs/lifecycle.md)及[无锁平台约束](docs/lock-free-design.md)。

## 示例与性能

`broadcast_example` 展示结构化发布结果，`competing_example` 处理真实字节。运行 `./scripts/run-benchmarks.sh` 可生成共享内存与 Unix Domain Datagram 基线 CSV；计时方法与语义差异见[性能文档](docs/benchmark.md)。

## 支持范围与限制

支持 Linux、参与进程一致 ABI、CMake 3.10+，且所需原子类型运行时必须真正 lock-free。投递为至多一次，队列压力下按约定丢弃。不提供持久化、认证、模式协商、发布者崩溃分配日志或初始化者接管。心跳回收假定进程能在五秒内获得调度。另见[路线图](ROADMAP.md)、[更新记录](CHANGELOG.md)和[贡献指南](CONTRIBUTING.md)。项目采用 MIT 许可证；单独标注的自由链表采用 Apache-2.0 OR MIT。

[English](README.en.md)
