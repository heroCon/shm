#include <atomic>
#include <cstdint>
#include <type_traits>
#include <stdexcept>
#include <thread>
#include <vector>
#include <cassert>
#include <iostream>

// 无锁自由列表：MPMC支持，管理固定容量的索引分配/回收
// IndexType：索引类型（必须是无符号整数，默认uint32_t）
template <typename IndexType = uint32_t>
class LockFreeFreeList {
private:
    // 链表头节点：索引+版本号（解决ABA问题）
    struct Node {
        IndexType next_free_index; // 指向链表下一个空闲索引
        uint64_t aba_counter;      // 版本号，每次修改递增
    };

    std::atomic<Node> m_head;                // 原子化链表头（索引+版本号）
    IndexType* m_next_free_index;            // 空闲索引链表存储（外部传入，固定长度）
    IndexType m_size;                        // 最大容量（支持的有效索引：0 ~ m_size-1）
    IndexType m_invalid_index;               // 无效索引标记（标记已分配的索引）
    std::atomic<bool> m_is_initialized{false};// 初始化状态标记

    // 静态断言：确保索引类型是无符号整数（避免负数索引）
    static_assert(std::is_unsigned<IndexType>::value, 
                  "IndexType must be an unsigned integer type");

public:
    // 构造函数：默认未初始化，需调用init()完成初始化
    LockFreeFreeList() noexcept 
        : m_head({0, 0}), m_next_free_index(nullptr), m_size(0), m_invalid_index(0) {}

    // 禁止拷贝和移动（无锁结构拷贝风险高，避免误用）
    LockFreeFreeList(const LockFreeFreeList&) = delete;
    LockFreeFreeList& operator=(const LockFreeFreeList&) = delete;
    LockFreeFreeList(LockFreeFreeList&&) = delete;
    LockFreeFreeList& operator=(LockFreeFreeList&&) = delete;

    // 析构函数：不管理外部传入的m_next_free_index内存（由用户负责）
    ~LockFreeFreeList() noexcept {
        m_is_initialized.store(false, std::memory_order_relaxed);
    }

    // 初始化：传入外部索引存储缓冲区和容量
    // 注意：buffer需保证生命周期长于自由列表，且容量>0
    void init(IndexType* buffer, IndexType capacity) {
        if (buffer == nullptr) {
            throw std::invalid_argument("Buffer cannot be null!");
        }
        if (capacity == 0) {
            throw std::invalid_argument("Capacity must be greater than 0!");
        }
        if (m_is_initialized.load(std::memory_order_acquire)) {
            throw std::logic_error("Free list has already been initialized!");
        }

        m_next_free_index = buffer;
        m_size = capacity;
        m_invalid_index = capacity + 1; // 无效索引 = 容量+1（超出有效索引范围）

        // 初始化空闲链表：buffer[i] = i+1（形成 0→1→2→...→capacity 的连续链表）
        for (IndexType i = 0; i < capacity; ++i) {
            m_next_free_index[i] = i + 1;
        }
        // 链表尾节点指向无效索引（标记链表结束）
        m_next_free_index[capacity] = m_invalid_index;

        // 初始化链表头：指向第一个空闲索引（0），版本号0
        m_head.store({0, 0}, std::memory_order_release);
        m_is_initialized.store(true, std::memory_order_release);
    }

    // 分配索引（pop）：成功返回true，通过index传出分配的索引；失败（无空闲索引/未初始化）返回false
    bool pop(IndexType& index) noexcept {
        // 未初始化直接返回失败
        if (!m_is_initialized.load(std::memory_order_acquire)) {
            return false;
        }

        Node old_head = m_head.load(std::memory_order_acquire);
        Node new_head = old_head;

        do {
            // 无空闲索引：链表头指向的索引 >= 容量（到达链表尾）
            if (old_head.next_free_index >= m_size) {
                return false;
            }

            // 计算新链表头：新头 = 原头指向的下一个空闲索引，版本号+1
            new_head.next_free_index = m_next_free_index[old_head.next_free_index];
            new_head.aba_counter = old_head.aba_counter + 1;

            // CAS原子更新链表头：成功则分配完成，失败则重试（自动更新old_head为最新值）
        } while (!m_head.compare_exchange_weak(
            old_head, new_head,
            std::memory_order_acq_rel,  // 成功：写操作释放语义，读操作获取语义
            std::memory_order_acquire   // 失败：仅读取，获取语义
        ));

        // 传出分配的索引（原头指向的空闲索引）
        index = old_head.next_free_index;
        // 标记该索引为已分配（避免double free）
        m_next_free_index[index] = m_invalid_index;

        // 释放栅栏：确保"标记索引为已分配"的写操作对其他线程（push）可见
        std::atomic_thread_fence(std::memory_order_release);

        return true;
    }

    // 回收索引（push）：成功返回true，失败（索引无效/重复回收/未初始化）返回false
    bool push(const IndexType index) noexcept {
        // 未初始化直接返回失败
        if (!m_is_initialized.load(std::memory_order_acquire)) {
            return false;
        }

        // 获取栅栏：确保能看到其他线程（pop）标记的"已分配"状态，避免double free
        std::atomic_thread_fence(std::memory_order_acquire);

        // 校验索引有效性：1. 索引在有效范围（0~m_size-1）；2. 索引已分配（标记为无效索引）
        if (index >= m_size || m_next_free_index[index] != m_invalid_index) {
            return false;
        }

        Node old_head = m_head.load(std::memory_order_acquire);
        Node new_head = old_head;

        do {
            // 回收的索引指向原链表头（头插法：新索引成为新的链表头前驱）
            m_next_free_index[index] = old_head.next_free_index;
            // 新链表头：指向回收的索引，版本号+1
            new_head.next_free_index = index;
            new_head.aba_counter = old_head.aba_counter + 1;

            // CAS原子更新链表头：成功则回收完成，失败则重试
        } while (!m_head.compare_exchange_weak(
            old_head, new_head,
            std::memory_order_acq_rel,
            std::memory_order_acquire
        ));

        return true;
    }

    // 辅助接口：检查是否已初始化
    bool is_initialized() const noexcept {
        return m_is_initialized.load(std::memory_order_acquire);
    }

    // 辅助接口：获取容量
    IndexType capacity() const noexcept {
        return m_size;
    }

    // 辅助接口：检查是否无空闲索引（弱一致性，仅作参考）
    bool is_empty() const noexcept {
        if (!is_initialized()) {
            return true;
        }
        return m_head.load(std::memory_order_acquire).next_free_index >= m_size;
    }
};
