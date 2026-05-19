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
template <typename IndexType = uint32_t, size_t CAPACITY = 32>
class LockFreeFreeList {
private:
    // 链表头节点：索引+版本号（解决ABA问题）
    struct Node {
        IndexType next_free_index; // 指向链表下一个空闲索引
        uint64_t aba_counter;      // 版本号，每次修改递增
    };
// 暂定直接使用数组，不能使用指针，因为指针在第一个进程初始化后的地址在第二个进程中无法访问

    std::atomic<Node> m_head;                // 原子化链表头（索引+版本号）
    IndexType m_next_free_index[CAPACITY];            // 空闲索引链表存储（外部传入，固定长度）
    IndexType m_size;                        // 最大容量（支持的有效索引：0 ~ m_size-1）
    IndexType m_invalid_index;               // 无效索引标记（标记已分配的索引）
    std::atomic<bool> m_is_initialized{false};// 初始化状态标记

    // 静态断言：确保索引类型是无符号整数（避免负数索引）
    static_assert(std::is_unsigned<IndexType>::value, 
                  "IndexType must be an unsigned integer type");

public:
    // 轻量一致性检查：用于判断共享内存中的 free_list 是否可能来自旧版本/被破坏
    bool is_sane() const noexcept {
        if (!m_is_initialized.load(std::memory_order_acquire)) {
            return true;
        }
        return m_size == CAPACITY && m_invalid_index == CAPACITY + 1;
    }

    // 强制重置初始化标记（仅应在确认没有并发使用者时调用）
    void force_reset_for_reinit() noexcept {
        m_is_initialized.store(false, std::memory_order_release);
    }

    void init() {
        if (m_is_initialized.load(std::memory_order_acquire)) {
            throw std::logic_error("Free list has already been initialized!");
        }

        m_size = CAPACITY;
        // m_next_free_index 用于“空闲链表 next”，链表结束用 m_size（CAPACITY）作为哨兵；
        // 而“已分配标记”必须与链表结束哨兵区分开，否则尾节点会被误判为已分配/可重复回收。
        m_invalid_index = static_cast<IndexType>(m_size + 1);

        // 初始化空闲链表：0→1→2→...→(m_size-1)→invalid
        // 注意：m_next_free_index 只有 CAPACITY 个元素（0..m_size-1），不能写入 m_size
        for (IndexType i = 0; i + 1 < m_size; ++i) {
            m_next_free_index[i] = static_cast<IndexType>(i + 1);
        }
        m_next_free_index[m_size - 1] = static_cast<IndexType>(m_size); // end-of-list sentinel

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
        std::cout << "Allocated index: " << index << std::endl;
        // 标记该索引为已分配（避免double free）
        m_next_free_index[index] = m_invalid_index;

        // 释放栅栏：确保"标记索引为已分配"的写操作对其他线程（push）可见
        std::atomic_thread_fence(std::memory_order_release);

        return true;
    }

    // 回收索引（push）：成功返回true，失败（索引无效/重复回收/未初始化）返回false
    bool push(const IndexType index) noexcept {
        std::cout << "Pushing index: " << index << std::endl;
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

    // 辅助接口：检查内存分配
    void print_list() const{
        Node head = m_head.load(std::memory_order_acquire);
        std::cout << "m_head: " << head.aba_counter << "," << head.next_free_index << std::endl;
        for (IndexType i = 0; i < m_size; ++i) {
            std::cout << "buffer[" << i << "]: " << m_next_free_index[i] << std::endl;
        }
    }
};
