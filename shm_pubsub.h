#ifndef SHM_PUBSUB_H_
#define SHM_PUBSUB_H_

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "lockfree_list.hpp"

// -------------------------- 可配置参数（根据需求调整）--------------------------
constexpr const char* SHM_NAME = "/shm_pubsub_mq";  // 共享内存名称
constexpr size_t MAX_WRITERS = 8;                   // 最大写进程数
constexpr size_t MAX_READERS = 16;                  // 最大读进程数
constexpr size_t BLOCK_COUNT = 32;                  // 固定数据块数量
constexpr size_t BLOCK_SIZE = 4096;                 // 单个数据块大小（含长度字段）
constexpr size_t QUEUE_CAPACITY = 32;               // 每个消息队列容量
constexpr uint64_t HEARTBEAT_TIMEOUT = 5000;        // 进程心跳超时（5秒）
constexpr uint64_t HEARTBEAT_INTERVAL = 1000;       // 心跳更新间隔（1秒）
constexpr uint64_t RECYCLE_INTERVAL = 2000;         // 资源回收间隔（2秒）
// -----------------------------------------------------------------------------

// Dmitry Vyukov 有界 MPMC 队列（存储数据块ID）
struct AtomicQueue {
  static_assert((QUEUE_CAPACITY & (QUEUE_CAPACITY - 1)) == 0,
                "QUEUE_CAPACITY must be a power of two");

  struct Cell {
    std::atomic<size_t> sequence = {0};
    size_t data = 0;
  };

  Cell queue[QUEUE_CAPACITY];
  std::atomic<size_t> enqueue_pos = {0};
  std::atomic<size_t> dequeue_pos = {0};

  void init() {
    enqueue_pos.store(0, std::memory_order_relaxed);
    dequeue_pos.store(0, std::memory_order_relaxed);
    for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
      queue[i].sequence.store(i, std::memory_order_relaxed);
      queue[i].data = 0;
    }
  }

  // 入队；队列满时返回false，由发布者负责修正引用计数/回收块
  bool enqueue(size_t block_id) {
    Cell* cell = nullptr;
    size_t pos = enqueue_pos.load(std::memory_order_relaxed);

    for (;;) {
      cell = &queue[pos & (QUEUE_CAPACITY - 1)];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (diff == 0) {
        if (enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;
      } else {
        pos = enqueue_pos.load(std::memory_order_relaxed);
      }
    }

    cell->data = block_id;
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  // 出队
  bool dequeue(size_t& block_id) {
    Cell* cell = nullptr;
    size_t pos = dequeue_pos.load(std::memory_order_relaxed);

    for (;;) {
      cell = &queue[pos & (QUEUE_CAPACITY - 1)];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
      if (diff == 0) {
        if (dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;
      } else {
        pos = dequeue_pos.load(std::memory_order_relaxed);
      }
    }

    block_id = cell->data;
    cell->sequence.store(pos + QUEUE_CAPACITY, std::memory_order_release);
    return true;
  }

  // 检查队列中是否包含指定块ID（弱一致性，仅用于回收路径判断）
  bool contains(size_t block_id) const {
    size_t begin = dequeue_pos.load(std::memory_order_acquire);
    size_t end = enqueue_pos.load(std::memory_order_acquire);
    for (size_t pos = begin; pos != end; ++pos) {
      const Cell& cell = queue[pos & (QUEUE_CAPACITY - 1)];
      if (cell.sequence.load(std::memory_order_acquire) == pos + 1 && cell.data == block_id) {
        return true;
      }
    }
    return false;
  }

  // 清空队列（用于进程离线回收后重置队列状态）
  void clear() { init(); }
};

// 数据块结构（POD类型，确保内存布局一致）
struct DataBlock {
  std::atomic<pid_t> owner_pid = {0};   // 占用进程PID（发布者使用）
  std::atomic<size_t> ref_count = {0};  // 引用计数：队列中未消费的块引用数
  size_t data_len = 0;                  // 实际数据长度
  char data[BLOCK_SIZE - sizeof(owner_pid) - sizeof(ref_count) - sizeof(data_len)] = {
      0};  // 调整缓冲区大小
};

// 读进程注册信息
struct ReaderInfo {
  std::atomic<bool> online = {false};     // 在线状态
  std::atomic<uint64_t> heartbeat = {0};  // 心跳时间戳（毫秒）
  bool subscribed = false;                // 订阅状态（非原子：仅读进程自身修改）
  pid_t pid = 0;                          // 进程PID
  AtomicQueue broadcast_queue;            // 广播模式专属接收队列
};

// 写进程注册信息
struct WriterInfo {
  std::atomic<bool> online = {false};     // 在线状态
  std::atomic<uint64_t> heartbeat = {0};  // 心跳时间戳（毫秒）
  pid_t pid = 0;                          // 进程PID
};

// 共享内存元数据（整个共享内存的核心控制结构）
struct SharedMeta {
  std::atomic<bool> inited = {false};  // 初始化标记
  size_t block_count = BLOCK_COUNT;    // 数据块总数
  size_t block_size = BLOCK_SIZE;      // 单个块大小
  LockFreeFreeList<uint32_t, BLOCK_COUNT> free_list;
  WriterInfo writers[MAX_WRITERS] = {0};  // 写进程数组
  ReaderInfo readers[MAX_READERS] = {0};  // 读进程数组
  AtomicQueue shared_queue;               // 竞争消费模式共享消息队列
};

// 共享内存发布订阅中间件类
class ShmPubSub {
 public:
  enum Role { PUBLISHER, SUBSCRIBER };
  enum DeliveryMode { BROADCAST, COMPETING };

  // 构造函数：指定角色，初始化共享内存
  ShmPubSub(Role role)
      : role_(role), shm_fd_(-1), shm_ptr_(nullptr), meta_(nullptr), blocks_(nullptr) {
    init_shm();
    register_process();
    start_heartbeat_thread();
    start_recycle_thread();
  }

  // 析构函数：注销进程，释放资源
  ~ShmPubSub() {
    running_.store(false, std::memory_order_release);
    if (heartbeat_thread_.joinable()) {
      heartbeat_thread_.join();
    }
    if (recycle_thread_.joinable()) {
      recycle_thread_.join();
    }
    unregister_process();
    release_shm();
  }

  // 发布数据（写进程调用）
  bool publish(const void* data, size_t data_len, DeliveryMode mode = BROADCAST) {
    if (role_ != PUBLISHER) return false;
    if (!data || data_len == 0) return false;

    // 1. 检查是否有有效订阅（在线且已订阅的读进程）
    size_t sub_count = get_valid_subscriber_count();  // 新增：获取订阅者数量
    if (sub_count == 0) {
      std::cout << "No valid subscribers, skip publish" << std::endl;
      return true;
    }

    // 2. 无锁申请可用块（CAS操作pop栈顶）
    uint32_t block_id = 0;
    if (!alloc_block(block_id)) {
      std::cerr << "No free blocks for publish" << std::endl;
      return false;
    }

    // 3. 写入数据到块；发布者先持有一个临时引用，避免投递过程中被订阅者提前回收
    DataBlock* block = &blocks_[block_id];
    block->owner_pid.store(getpid(), std::memory_order_release);
    block->ref_count.store(1, std::memory_order_release);
    block->data_len = std::min(data_len, sizeof(block->data));
    memcpy(block->data, data, block->data_len);

    // 4. 数据写完后释放所有权；之后入队的订阅者只能看到完整块
    block->owner_pid.store(0, std::memory_order_release);

    bool enqueued = false;
    if (mode == COMPETING) {
      enqueued = publish_competing(block_id);
    } else {
      enqueued = publish_broadcast(block_id);
    }

    release_block_ref(block_id);  // 释放发布者临时引用
    return enqueued;
  }

  // 新增：获取当前有效订阅者数量（在线且已订阅）
  size_t get_valid_subscriber_count() {
    size_t count = 0;
    for (size_t i = 0; i < MAX_READERS; ++i) {
      ReaderInfo& reader = meta_->readers[i];
      // 用acquire内存序确保读取到最新状态
      if (reader.online.load(std::memory_order_acquire) && reader.subscribed) {
        count++;
      }
    }
    return count;
  }

  // 订阅（读进程调用）
  bool subscribe() {
    if (role_ != SUBSCRIBER || !reader_info_) return false;
    reader_info_->subscribed = true;
    std::cout << "Subscriber " << getpid() << " subscribed" << std::endl;
    return true;
  }

  // 取消订阅（读进程调用）
  bool unsubscribe() {
    if (role_ != SUBSCRIBER || !reader_info_) return false;
    reader_info_->subscribed = false;
    std::cout << "Subscriber " << getpid() << " unsubscribed" << std::endl;
    return true;
  }

  // 接收数据：先读广播队列，再读竞争消费队列；消费后按引用计数回收
  bool receive(void* buf, size_t buf_len, size_t& actual_len) {
    if (role_ != SUBSCRIBER || !reader_info_ || !buf) return false;

    size_t block_id = 0;
    // 优先读取广播队列；没有广播消息时再读取竞争消费共享队列
    if (!reader_info_->broadcast_queue.dequeue(block_id) &&
        !meta_->shared_queue.dequeue(block_id)) {
      actual_len = 0;
      return false;
    }

    // 过滤无效块ID
    if (block_id >= BLOCK_COUNT) {
      actual_len = 0;
      return false;
    }

    DataBlock* block = &blocks_[block_id];
    // 核心判断：owner≠0 → 未完成发布，直接return（不消费、不回收）
    if (block->owner_pid.load(std::memory_order_acquire) != 0) {
      std::cout << "Subscriber " << getpid() << " skip incomplete block (ID: " << block_id << ")"
                << std::endl;
      actual_len = 0;
      return false;
    }

    // owner=0 → 正常消费
    actual_len = std::min(block->data_len, buf_len);
    memcpy(buf, block->data, actual_len);

    // 消费后递减引用计数；最后一个订阅者消费后才回收块
    release_block_ref(block_id);
    return true;
  }

 private:
  // 初始化共享内存
  void init_shm() {
    // 1. 计算共享内存总大小：元数据大小 + 所有数据块大小
    size_t shm_total_size = sizeof(SharedMeta) + BLOCK_COUNT * BLOCK_SIZE;

    // 2. 创建/打开共享内存
    shm_fd_ = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (shm_fd_ == -1) {
      perror("shm_open failed");
      exit(EXIT_FAILURE);
    }

    // 3. 设置共享内存大小
    if (ftruncate(shm_fd_, shm_total_size) == -1) {
      perror("ftruncate failed");
      exit(EXIT_FAILURE);
    }

    // 4. 映射共享内存到虚拟地址空间
    shm_ptr_ = mmap(nullptr, shm_total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (shm_ptr_ == MAP_FAILED) {
      perror("mmap failed");
      exit(EXIT_FAILURE);
    }

    // 5. 拆分元数据区和数据块区
    meta_ = static_cast<SharedMeta*>(shm_ptr_);
    blocks_ = reinterpret_cast<DataBlock*>(reinterpret_cast<char*>(shm_ptr_) + sizeof(SharedMeta));

    // 6. 初始化共享内存（仅第一个进程执行）
    if (!meta_->inited.load(std::memory_order_acquire)) {
      bool expected = false;  // 关键：用普通变量存储预期值
      if (meta_->inited.compare_exchange_strong(
              expected, true,  // expected是普通变量，存储预期的false
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        meta_->free_list.init();
        meta_->shared_queue.init();
        for (size_t i = 0; i < MAX_READERS; ++i) {
          meta_->readers[i].broadcast_queue.init();
        }
      }
    }
    meta_->free_list.print_list();
  }

  // 注册当前进程到共享内存元数据
  void register_process() {
    pid_t pid = getpid();
    if (role_ == PUBLISHER) {
      // 抢占写进程空闲槽位
      for (size_t i = 0; i < MAX_WRITERS; ++i) {
        if (!meta_->writers[i].online.load(std::memory_order_acquire)) {
          bool expected = false;
          if (meta_->writers[i].online.compare_exchange_strong(
                  expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            meta_->writers[i].pid = pid;
            meta_->writers[i].heartbeat.store(get_timestamp_ms(), std::memory_order_release);
            writer_info_ = &meta_->writers[i];
            std::cout << "Publisher " << pid << " registered (ID: " << i << ")" << std::endl;
            return;
          }
        }
      }
      std::cerr << "Max publishers reached (" << MAX_WRITERS << ")" << std::endl;
      exit(EXIT_FAILURE);
    } else {
      // 抢占读进程空闲槽位
      for (size_t i = 0; i < MAX_READERS; ++i) {
        if (!meta_->readers[i].online.load(std::memory_order_acquire)) {
          bool expected = false;
          if (meta_->readers[i].online.compare_exchange_strong(
                  expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            meta_->readers[i].pid = pid;
            meta_->readers[i].heartbeat.store(get_timestamp_ms(), std::memory_order_release);
            reader_info_ = &meta_->readers[i];
            std::cout << "Subscriber " << pid << " registered (ID: " << i << ")" << std::endl;
            return;
          }
        }
      }
      std::cerr << "Max subscribers reached (" << MAX_READERS << ")" << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  // 注销当前进程
  void unregister_process() {
    pid_t pid = getpid();
    if (role_ == PUBLISHER && writer_info_) {
      writer_info_->online.store(false, std::memory_order_release);
      std::cout << "Publisher " << pid << " unregistered" << std::endl;
    } else if (role_ == SUBSCRIBER && reader_info_) {
      drain_reader_broadcast_queue(*reader_info_);
      reader_info_->online.store(false, std::memory_order_release);
      reader_info_->subscribed = false;
      std::cout << "Subscriber " << pid << " unregistered" << std::endl;
    }
  }

  // 释放共享内存映射
  void release_shm() {
    if (shm_ptr_ != MAP_FAILED) {
      munmap(shm_ptr_, sizeof(SharedMeta) + BLOCK_COUNT * BLOCK_SIZE);
    }
    if (shm_fd_ != -1) {
      close(shm_fd_);
    }
    // 仅最后一个进程删除共享内存文件（简化：实际可通过引用计数优化）
    shm_unlink(SHM_NAME);
  }

  // 启动心跳线程（定期更新时间戳）
  void start_heartbeat_thread() {
    heartbeat_thread_ = std::thread([this]() {
      while (running_) {
        uint64_t now = get_timestamp_ms();
        if (role_ == PUBLISHER && writer_info_) {
          writer_info_->heartbeat.store(now, std::memory_order_release);
        } else if (role_ == SUBSCRIBER && reader_info_) {
          reader_info_->heartbeat.store(now, std::memory_order_release);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL));
      }
    });
  }

  // 启动资源回收线程（检测离线进程，回收块和队列）
  void start_recycle_thread() {
    recycle_thread_ = std::thread([this]() {
      while (running_) {
        uint64_t now = get_timestamp_ms();
        recycle_offline_readers(now);
        recycle_offline_writers(now);
        std::this_thread::sleep_for(std::chrono::milliseconds(RECYCLE_INTERVAL));
      }
    });
  }

  // 回收离线写进程（仅处理未完成发布的块）
  void recycle_offline_writers(uint64_t now) {
    for (size_t i = 0; i < MAX_WRITERS; ++i) {
      WriterInfo& w = meta_->writers[i];
      if (!w.online.load(std::memory_order_acquire)) continue;
      if ((now - w.heartbeat.load(std::memory_order_acquire)) <= HEARTBEAT_TIMEOUT) continue;

      // 标记写进程离线
      w.online.store(false, std::memory_order_release);
      std::cout << "Recycle offline publisher (PID: " << w.pid << ")" << std::endl;

      // 遍历该发布者未完成发布的块（owner=该发布者PID）
      for (size_t j = 0; j < BLOCK_COUNT; ++j) {
        DataBlock* block = &blocks_[j];
        if (block->owner_pid.load(std::memory_order_acquire) != w.pid) continue;

        // 检查共享消息队列或广播队列：是否还有该块？
        if (has_block_in_queues(j)) {
          std::cout << "Block " << j << " still in message queues, wait next cycle" << std::endl;
          continue;
        }

        // 关键：短睡眠，避免订阅者刚出队就回收
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // 睡眠后再检查：仍无队列引用 → 回收
        if (!has_block_in_queues(j)) {
          free_block(j);
          block->owner_pid.store(0, std::memory_order_release);
          std::cout << "Block " << j << " recycled (offline publisher, no queue references)"
                    << std::endl;
        } else {
          std::cout << "Block " << j << " still in queues after sleep, skip" << std::endl;
        }
      }
    }
  }

  // 回收离线读进程资源
  void recycle_offline_readers(uint64_t now) {
    for (size_t i = 0; i < MAX_READERS; ++i) {
      ReaderInfo& reader = meta_->readers[i];
      if (now < reader.heartbeat.load(std::memory_order_acquire))
        throw std::runtime_error("Invalid heartbeat time");
      if (reader.online.load(std::memory_order_acquire) &&
          (now - reader.heartbeat.load(std::memory_order_acquire)) > HEARTBEAT_TIMEOUT) {
        // 标记为离线
        reader.online.store(false, std::memory_order_release);
        std::cout << "Recycle offline subscriber (PID: " << reader.pid << ")" << std::endl;

        // 回收该读进程广播队列中的块（递减引用计数，计数为0则归还）
        drain_reader_broadcast_queue(reader);
      }
    }
  }

  void drain_reader_broadcast_queue(ReaderInfo& reader) {
    size_t block_id = 0;
    while (reader.broadcast_queue.dequeue(block_id)) {
      if (block_id < BLOCK_COUNT) {
        release_block_ref(block_id);
      }
    }
    reader.broadcast_queue.clear();
  }

  void release_block_ref(size_t block_id) {
    DataBlock* block = &blocks_[block_id];
    size_t current_ref = block->ref_count.load(std::memory_order_acquire);
    while (current_ref > 0) {
      if (block->ref_count.compare_exchange_weak(
              current_ref, current_ref - 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        if (current_ref - 1 == 0) {
          free_block(block_id);
        }
        return;
      }
    }
  }

  // 无锁分配块（pop可用块栈）
  bool alloc_block(uint32_t& block_id) { return meta_->free_list.pop(block_id); }

  // 无锁释放块（push到可用块栈）
  bool free_block(size_t block_id) { return meta_->free_list.push(block_id); }

  // 回收已完成发布且队列无引用的块
  void recycle_unowned_blocks() {
    for (size_t j = 0; j < BLOCK_COUNT; ++j) {
      DataBlock* block = &blocks_[j];
      // 已完成发布（owner=0）且队列无该块 → 回收
      if (block->owner_pid.load(std::memory_order_acquire) == 0 && !has_block_in_queues(j)) {
        free_block(j);
        std::cout << "Block " << j << " recycled (no queue references)" << std::endl;
      }
    }
  }

  bool publish_competing(size_t block_id) {
    DataBlock* block = &blocks_[block_id];
    block->ref_count.fetch_add(1, std::memory_order_acq_rel);
    if (meta_->shared_queue.enqueue(block_id)) {
      return true;
    }
    release_block_ref(block_id);
    std::cerr << "Shared message queue full, skip publish" << std::endl;
    return false;
  }

  bool publish_broadcast(size_t block_id) {
    bool enqueued = false;
    DataBlock* block = &blocks_[block_id];
    for (size_t i = 0; i < MAX_READERS; ++i) {
      ReaderInfo& reader = meta_->readers[i];
      if (reader.online.load(std::memory_order_acquire) && reader.subscribed) {
        block->ref_count.fetch_add(1, std::memory_order_acq_rel);
        if (reader.broadcast_queue.enqueue(block_id)) {
          enqueued = true;
        } else {
          release_block_ref(block_id);
          std::cerr << "Subscriber broadcast queue full, skip subscriber (ID: " << i << ")"
                    << std::endl;
        }
      }
    }
    return enqueued;
  }

  // 检查共享消息队列或广播队列是否包含该块
  bool has_block_in_queues(size_t block_id) {
    if (meta_->shared_queue.contains(block_id)) {
      return true;
    }
    for (size_t i = 0; i < MAX_READERS; ++i) {
      ReaderInfo& r = meta_->readers[i];
      if (r.online.load(std::memory_order_acquire) && r.subscribed &&
          r.broadcast_queue.contains(block_id)) {
        return true;
      }
    }
    return false;
  }

  // 获取当前时间戳（毫秒）
  static uint64_t get_timestamp_ms() {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
  }

 private:
  Role role_;                           // 进程角色（发布者/订阅者）
  int shm_fd_;                          // 共享内存文件描述符
  void* shm_ptr_;                       // 共享内存映射指针
  SharedMeta* meta_;                    // 共享元数据指针
  DataBlock* blocks_;                   // 数据块数组指针
  WriterInfo* writer_info_ = nullptr;   // 当前写进程信息（仅发布者有效）
  ReaderInfo* reader_info_ = nullptr;   // 当前读进程信息（仅订阅者有效）
  std::thread heartbeat_thread_;        // 心跳线程
  std::thread recycle_thread_;          // 资源回收线程
  std::atomic<bool> running_ = {true};  // 线程运行标记
};

#endif  // SHM_PUBSUB_H_