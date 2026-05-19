#ifndef SHM_PUBSUB_H
#define SHM_PUBSUB_H

#include <iostream>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <thread>
#include <chrono>
#include <vector>
#include <cerrno>
#include <new>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>

#include "lockfree_list.hpp"

// -------------------------- 可配置参数（根据需求调整）--------------------------
constexpr const char* SHM_NAME = "/shm_pubsub_mq";       // 共享内存名称
constexpr size_t MAX_WRITERS = 8;                        // 最大写进程数
constexpr size_t MAX_READERS = 16;                       // 最大读进程数
constexpr size_t BLOCK_COUNT = 32;                       // 固定数据块数量
constexpr size_t BLOCK_SIZE = 4096;                      // 单个数据块大小（含长度字段）
constexpr size_t QUEUE_CAPACITY = 32;                    // 每个读进程接收队列容量
constexpr uint64_t HEARTBEAT_TIMEOUT = 5000;             // 进程心跳超时（5秒）
constexpr uint64_t HEARTBEAT_INTERVAL = 1000;            // 心跳更新间隔（1秒）
constexpr uint64_t RECYCLE_INTERVAL = 2000;               // 资源回收间隔（2秒）
constexpr uint32_t SHM_MAGIC = 0x53484D50;               // 'SHMP'
constexpr uint32_t SHM_ABI_VERSION = 2;                  // bump when SharedMeta layout changes
// -----------------------------------------------------------------------------

// 无锁循环队列（每个读进程专属接收队列，存储数据块ID）
struct AtomicQueue {
    std::atomic<size_t> head = {0};  // 出队指针（读）
    std::atomic<size_t> tail = {0};  // 入队指针（写）
    size_t queue[QUEUE_CAPACITY] = {0};  // 存储块ID
    std::atomic<uint8_t> ready[QUEUE_CAPACITY] = {}; // slot 就绪标记：避免读到未写入的 queue 槽位

    // 入队（无锁；队列满则返回 false，由上层决定如何处理避免泄漏）
    bool enqueue(size_t block_id) {
        for (;;) {
            size_t current_tail = tail.load(std::memory_order_acquire);
            size_t next_tail = (current_tail + 1) % QUEUE_CAPACITY;

            // 队列满：返回失败（避免无声覆盖导致块 ID 丢失/泄漏）
            if (next_tail == head.load(std::memory_order_acquire)) {
                return false;
            }

            // 先通过 CAS 预留一个槽位；消费者可能会先看到 tail 变化，所以需要 ready 标记保护
            if (tail.compare_exchange_weak(
                    current_tail, next_tail,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                queue[current_tail] = block_id;
                ready[current_tail].store(1, std::memory_order_release);
                return true;
            }
        }
    }

    // 出队（无锁）
    bool dequeue(size_t& block_id) {
        size_t current_head = head.load(std::memory_order_acquire);
        if (current_head == tail.load(std::memory_order_acquire)) {
            return false;  // 队列为空
        }

        // 生产者可能已推进 tail 但尚未写入 queue 槽位，ready 用于避免读到旧值
        if (!ready[current_head].load(std::memory_order_acquire)) {
            return false;
        }

        size_t next_head = (current_head + 1) % QUEUE_CAPACITY;
        // CAS更新head：确保原子性（单消费者场景下基本不会失败）
        if (!head.compare_exchange_weak(
                current_head, next_head,
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return false;
        }

        block_id = queue[current_head];
        ready[current_head].store(0, std::memory_order_release);
        return true;
    }
    
    // 检查队列中是否包含指定块ID
    bool contains(size_t block_id) {
        size_t head = this->head.load(std::memory_order_acquire);
        size_t tail = this->tail.load(std::memory_order_acquire);
        for (size_t i = head; i != tail; i = (i + 1) % QUEUE_CAPACITY) {
            if (ready[i].load(std::memory_order_acquire) && queue[i] == block_id) {
                return true;
            }
        }
        return false;
    }

    // 清空队列（用于进程离线回收）
    void clear() {
        for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
            ready[i].store(0, std::memory_order_release);
        }
        head.store(0, std::memory_order_release);
        tail.store(0, std::memory_order_release);
    }
};

// 数据块结构（POD类型，确保内存布局一致）
struct DataBlock {
    std::atomic<pid_t> owner_pid = {0};  // 占用进程PID（发布者使用）
    std::atomic<size_t> ref_count = {0}; // 引用计数：未消费该块的读者数
    size_t data_len = 0;                 // 实际数据长度
    char data[BLOCK_SIZE - sizeof(owner_pid) - sizeof(ref_count) - sizeof(data_len)] = {0};  // 调整缓冲区大小
};

// 读进程注册信息
struct ReaderInfo {
    std::atomic<bool> online = {false};       // 在线状态
    std::atomic<uint64_t> heartbeat = {0};    // 心跳时间戳（毫秒）
    bool subscribed = false;                  // 订阅状态（非原子：仅读进程自身修改）
    pid_t pid = 0;                            // 进程PID
    AtomicQueue recv_queue;                   // 专属接收队列
};

// 写进程注册信息
struct WriterInfo {
    std::atomic<bool> online = {false};       // 在线状态
    std::atomic<uint64_t> heartbeat = {0};    // 心跳时间戳（毫秒）
    pid_t pid = 0;                            // 进程PID
};

// 共享内存元数据（整个共享内存的核心控制结构）
struct SharedMeta {
    uint32_t magic = 0;
    uint32_t abi_version = 0;
    std::atomic<bool> inited = {false};                          // 初始化标记
    size_t block_count = BLOCK_COUNT;                            // 数据块总数
    size_t block_size = BLOCK_SIZE;                              // 单个块大小
    LockFreeFreeList<uint32_t, BLOCK_COUNT> free_list;
    WriterInfo writers[MAX_WRITERS] = {0};                       // 写进程数组
    ReaderInfo readers[MAX_READERS] = {0};                       // 读进程数组
};

// 共享内存发布订阅中间件类
class ShmPubSub {
public:
    enum Role { PUBLISHER, SUBSCRIBER };

    // 构造函数：指定角色，初始化共享内存
    ShmPubSub(Role role) : role_(role), shm_fd_(-1), shm_ptr_(nullptr), meta_(nullptr), blocks_(nullptr) {
        init_shm();
        register_process();
        start_heartbeat_thread();
        start_recycle_thread();
    }

    // 析构函数：注销进程，释放资源
    ~ShmPubSub() {
        unregister_process();
        release_shm();
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join();
        }
        if (recycle_thread_.joinable()) {
            recycle_thread_.join();
        }
    }

    // 发布数据（写进程调用）
    bool publish(const void* data, size_t data_len) {
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

        // 3. 写入数据到块（初始化引用计数=订阅者数量）
        DataBlock* block = &blocks_[block_id];
        block->owner_pid.store(getpid(), std::memory_order_release);
        block->ref_count.store(sub_count, std::memory_order_release);  // 关键：初始化引用计数
        block->data_len = std::min(data_len, sizeof(block->data));
        memcpy(block->data, data, block->data_len);

        // 4. 发布者释放块所有权（不再直接归还块，由引用计数控制）
        block->owner_pid.store(0, std::memory_order_release);

        // 5. 遍历所有订阅者，将块ID入队到其接收队列（无锁）
        for (size_t i = 0; i < MAX_READERS; ++i) {
            ReaderInfo& reader = meta_->readers[i];
            if (reader.online.load(std::memory_order_acquire) && reader.subscribed) {
                if (!reader.recv_queue.enqueue(block_id)) {
                    // 该订阅者未能入队，释放一次引用，避免 ref_count 永久无法归零导致泄漏
                    release_block_ref(block_id);
                }
            }
        }

        // 如果没有任何订阅者成功入队（例如队列满），此处选择“丢弃但不报错”，保持发布端非阻塞特性。
        // 相关块已在 release_block_ref 中归还 free_list，不会造成泄漏或耗尽。
        return true;
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

    // 接收数据（完全按你的要求：owner≠0直接return，owner=0消费后回收）
    bool receive(void* buf, size_t buf_len, size_t& actual_len) {
        if (role_ != SUBSCRIBER || !reader_info_ || !buf) return false;

        size_t block_id = 0;
        // 出队一个块ID（只处理自己队列的块）
        if (!reader_info_->recv_queue.dequeue(block_id)) {
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
            std::cout << "Subscriber " << getpid() << " skip incomplete block (ID: " << block_id << ")" << std::endl;
            actual_len = 0;
            // 避免丢失块 ID：尝试重新入队；若失败则释放引用避免泄漏
            if (!reader_info_->recv_queue.enqueue(block_id)) {
                release_block_ref(block_id);
            }
            return false;
        }

        // owner=0 → 正常消费
        actual_len = std::min(block->data_len, buf_len);
        memcpy(buf, block->data, actual_len);

        // 消费后释放引用：仅最后一个消费者归还块
        release_block_ref(block_id);
        return true;
    }

private:
    void release_block_ref(size_t block_id) {
        DataBlock* block = &blocks_[block_id];
        size_t current_ref = block->ref_count.load(std::memory_order_acquire);
        while (current_ref > 0) {
            if (block->ref_count.compare_exchange_weak(
                    current_ref, current_ref - 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                if (current_ref - 1 == 0) {
                    free_block(block_id);
                }
                return;
            }
        }
    }

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

        // 5.0 ABI 检查：SharedMeta/队列布局升级后，如果复用旧共享内存会产生错误行为
        if (meta_->magic != SHM_MAGIC || meta_->abi_version != SHM_ABI_VERSION) {
            // 仅示例工程：发现不兼容时直接重建元数据，避免继续使用旧布局造成崩溃/泄漏。
            // 如果需要在生产环境中支持滚动升级，应使用更严格的多版本兼容策略。
            new (meta_) SharedMeta();
            meta_->magic = SHM_MAGIC;
            meta_->abi_version = SHM_ABI_VERSION;
            meta_->inited.store(false, std::memory_order_release);
            meta_->block_count = BLOCK_COUNT;
            meta_->block_size = BLOCK_SIZE;
        }

        // 5.1 清理可能残留的注册信息（异常退出时 online 标记可能遗留在共享内存中）
        // 只做“pid 不存在则离线”的保守回收，避免影响仍在运行的进程。
        for (size_t i = 0; i < MAX_WRITERS; ++i) {
            WriterInfo& w = meta_->writers[i];
            if (w.online.load(std::memory_order_acquire) && !pid_alive(w.pid)) {
                w.online.store(false, std::memory_order_release);
            }
        }
        for (size_t i = 0; i < MAX_READERS; ++i) {
            ReaderInfo& r = meta_->readers[i];
            if (r.online.load(std::memory_order_acquire) && !pid_alive(r.pid)) {
                r.online.store(false, std::memory_order_release);
                r.subscribed = false;
                r.recv_queue.clear();
            }
        }

        const bool any_online = has_any_online_process();

        // 6. 初始化共享内存（仅第一个进程执行）
        if (!meta_->inited.load(std::memory_order_acquire)) {
            bool expected = false;  // 关键：用普通变量存储预期值
            if (meta_->inited.compare_exchange_strong(
                expected, true,  // expected是普通变量，存储预期的false
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
                meta_->free_list.init();
            }
        }

        // 如果共享内存残留了“inited=true 但 free_list 未完成初始化”的状态（例如初始化进程异常退出），
        // 且当前确认没有存活进程，则直接完成初始化以避免永久等待。
        if (!any_online && !meta_->free_list.is_initialized()) {
            meta_->free_list.init();
        }

        // 兼容旧版本/异常退出导致的 free_list 破坏：当确认没有存活进程时，允许重置并重新初始化
        if (!any_online && meta_->free_list.is_initialized() && !meta_->free_list.is_sane()) {
            meta_->free_list.force_reset_for_reinit();
            meta_->free_list.init();
        }

        // 其他进程可能在 meta_->inited 变为 true 之后才进入，此时 free_list 仍可能初始化中
        while (!meta_->free_list.is_initialized()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        meta_->free_list.print_list();
    }

    static bool pid_alive(pid_t pid) noexcept {
        if (pid <= 0) return false;
        if (kill(pid, 0) == 0) return true;
        return errno == EPERM;
    }

    bool has_any_online_process() const noexcept {
        for (size_t i = 0; i < MAX_WRITERS; ++i) {
            if (meta_->writers[i].online.load(std::memory_order_acquire)) return true;
        }
        for (size_t i = 0; i < MAX_READERS; ++i) {
            if (meta_->readers[i].online.load(std::memory_order_acquire)) return true;
        }
        return false;
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
                        expected, true,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
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
                        expected, true,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
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

                // 检查所有在线订阅者队列：是否还有该块？
                if (has_block_in_queues(j)) {
                    std::cout << "Block " << j << " still in subscribers' queues, wait next cycle" << std::endl;
                    continue;
                }

                // 关键：短睡眠，避免订阅者刚出队就回收
                std::this_thread::sleep_for(std::chrono::milliseconds(100));

                // 睡眠后再检查：仍无队列引用 → 回收
                if (!has_block_in_queues(j)) {
                    free_block(j);
                    block->owner_pid.store(0, std::memory_order_release);
                    std::cout << "Block " << j << " recycled (offline publisher, no queue references)" << std::endl;
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

                // 关键：回收队列中的所有块（递减引用计数，计数为0则归还）
                size_t block_id = 0;
                while (reader.recv_queue.dequeue(block_id)) {
                    DataBlock* block = &blocks_[block_id];
                    size_t current_ref = block->ref_count.load(std::memory_order_acquire);
                    while (current_ref > 0) {
                        if (block->ref_count.compare_exchange_weak(
                            current_ref, current_ref - 1,
                            std::memory_order_acq_rel,
                            std::memory_order_relaxed)) {
                            if (current_ref - 1 == 0) {
                                free_block(block_id);
                            }
                            break;
                        }
                        current_ref = block->ref_count.load(std::memory_order_acquire);
                    }
                }
                reader.recv_queue.clear();  // 清空队列
            }
        }
    }

    // 无锁分配块（pop可用块栈）
    bool alloc_block(uint32_t& block_id) {
        return meta_->free_list.pop(block_id);
    }

    // 无锁释放块（push到可用块栈）
    bool free_block(size_t block_id) {
        return meta_->free_list.push(block_id);
    }

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

    // 检查所有在线订阅者队列是否包含该块
    bool has_block_in_queues(size_t block_id) {
        for (size_t i = 0; i < MAX_READERS; ++i) {
            ReaderInfo& r = meta_->readers[i];
            if (r.online.load(std::memory_order_acquire) && r.subscribed && r.recv_queue.contains(block_id)) {
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
    Role role_;                                  // 进程角色（发布者/订阅者）
    int shm_fd_;                                 // 共享内存文件描述符
    void* shm_ptr_;                              // 共享内存映射指针
    SharedMeta* meta_;                           // 共享元数据指针
    DataBlock* blocks_;                          // 数据块数组指针
    WriterInfo* writer_info_ = nullptr;          // 当前写进程信息（仅发布者有效）
    ReaderInfo* reader_info_ = nullptr;          // 当前读进程信息（仅订阅者有效）
    std::thread heartbeat_thread_;               // 心跳线程
    std::thread recycle_thread_;                 // 资源回收线程
    std::atomic<bool> running_ = {true};         // 线程运行标记
};

#endif // SHM_PUBSUB_H
