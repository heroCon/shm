# lockfree_shm

一个基于 Linux 共享内存的轻量级发布订阅示例，目标是用较少依赖演示多进程通信中的无锁队列、固定块内存池、心跳检测与离线回收。

## 项目内容

- `publisher`：发布进程示例，循环发送 `TestTopic` 数据。
- `subscriber`：订阅进程示例，非阻塞轮询接收数据。
- `shm_pubsub.h`：核心发布订阅实现（共享内存布局、注册、收发、回收逻辑）。
- `lockfree_list.hpp`：无锁空闲块管理结构（自由链表）。
- `TestTopic.h`：示例消息结构。
- `DelayTime.h`：延迟统计工具。

## 环境要求

- Linux（依赖 POSIX 共享内存接口，如 `shm_open` / `mmap`）
- CMake >= 3.10
- 支持 C++11 及以上的编译器

## 构建

```bash
mkdir -p build
cd build
cmake ..
make
```

生成可执行文件：

- `build/publisher`
- `build/subscriber`

## 运行

建议开两个终端：

1. 启动订阅者

```bash
./build/subscriber
```

2. 启动发布者

```bash
./build/publisher
```

你会在订阅者侧看到持续接收的时间戳计数。

## 核心设计（简述）

- 使用固定数量的数据块作为共享内存消息池。
- 发布时先分配块，再写入数据，再把块 ID 投递到在线订阅者队列。
- 每个订阅者有独立接收队列，降低读者之间竞争。
- 通过心跳检测离线发布者/订阅者，并尝试回收其相关资源。

## 当前实现注意事项

- 这是实验性质示例，侧重演示机制，不等同于生产级消息中间件。
- 共享内存清理策略当前较为直接（析构时 `shm_unlink`），多进程并发退出场景下建议进一步完善。
- 建议在压测和异常退出场景下验证回收逻辑与边界行为。

## English Version

See `README.en.md`.
