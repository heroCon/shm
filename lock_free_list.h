// Copyright (c) 2019 by Robert Bosch GmbH. All rights reserved.
// Copyright (c) 2021 - 2022 by Apex.AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// This implementation is adapted from iceoryx MpmcLoFFLi.  The caller owns an
// index after pop() and must transfer that ownership with synchronization before
// another thread calls push(index).  An index must be pushed exactly once.

#ifndef LOCK_FREE_LIST_H_
#define LOCK_FREE_LIST_H_

// The lock-free free-list algorithm in this file is adapted from iceoryx
// MpmcLoFFLi.
//
// Original copyright:
//   Copyright (c) 2019 by Robert Bosch GmbH. All rights reserved.
//   Copyright (c) 2021 - 2022 by Apex.AI Inc. All rights reserved.
//
// Original license: Apache-2.0 OR MIT
//   Apache-2.0: https://www.apache.org/licenses/LICENSE-2.0
//   MIT:        https://opensource.org/licenses/MIT
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Source:
//   https://github.com/eclipse-iceoryx/iceoryx
//
// Important correctness contract inherited from iceoryx:
//   * pop() transfers unique ownership of the returned index to the caller.
//   * push(index) may only be called by the current unique owner of index.
//   * If the index ownership is transferred to another thread/process before
//     push(), that transfer must provide synchronization.
//   * The invalid-index check in push() is only a defensive check for completed
//     double frees; it is not a complete protection against concurrent
//     duplicate push(index) calls or guessed indices.
//
// Violating this ownership contract can corrupt the free-list even though the
// head uses an ABA counter.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <type_traits>

// 无锁自由列表：MPMC支持，管理固定容量的索引分配/回收。
// 该实现继承自 iceoryx MpmcLoFFLi 的所有权前提：
// 1. pop() 返回的索引具有唯一所有权；
// 2. 只有当前唯一 owner 可以 push(index)；
// 3. 跨线程/跨进程转移索引所有权时，调用方必须提供同步；
// 4. push() 中的 invalid-index 检查只能防御已经完成的重复释放，
//    不能防御两个线程/进程并发 push 同一个 index，也不能防御猜测 index。
// IndexType：索引类型（必须是无符号整数，默认uint32_t）
template <typename IndexType = uint32_t, size_t CAPACITY = 32>
class LockFreeFreeList {
 private:
  // 链表头节点：索引+版本号（解决ABA问题）
  struct Node {
    uint32_t next_free_index;
    uint32_t aba_counter;
  };
  static uint64_t pack(Node n) noexcept {
    return static_cast<uint64_t>(n.aba_counter) << 32 | n.next_free_index;
  }
  static Node unpack(uint64_t value) noexcept {
    return Node{static_cast<uint32_t>(value), static_cast<uint32_t>(value >> 32)};
  }
  // 暂定直接使用数组，不能使用指针，因为指针在第一个进程初始化后的地址在第二个进程中无法访问

  std::atomic<uint64_t> m_head;               // packed 32-bit index + 32-bit ABA counter
  IndexType m_next_free_index[CAPACITY + 1];  // 空闲索引链表存储（包含一个尾哨兵槽位）
  IndexType m_size;                           // 最大容量（支持的有效索引：0 ~ m_size-1）
  IndexType m_invalid_index;                  // 无效索引标记（标记已分配的索引）
  std::atomic<bool> m_is_initialized{false};  // 初始化状态标记

  // 静态断言：确保索引类型是无符号整数（避免负数索引）
  static_assert(std::is_unsigned<IndexType>::value, "IndexType must be an unsigned integer type");
  static_assert(sizeof(IndexType) <= sizeof(uint32_t), "packed head supports up to 32-bit indices");
  static_assert(CAPACITY > 0, "A capacity of 0 is not supported");
  static_assert(CAPACITY < std::numeric_limits<IndexType>::max() - 1,
                "Capacity leaves no room for reserved sentinel indices");

 public:
  void init() {
    if (m_is_initialized.load(std::memory_order_acquire)) {
      throw std::logic_error("Free list has already been initialized!");
    }

    m_size = CAPACITY;
    m_invalid_index = m_size + 1;  // 无效索引 = 容量+1（超出有效索引范围）

    // 初始化空闲链表：buffer[i] = i+1（形成 0→1→2→...→capacity-1 的连续链表）
    for (IndexType i = 0; i < m_size; ++i) {
      m_next_free_index[i] = i + 1;
    }
    // 链表尾节点指向无效索引（标记链表结束）
    m_next_free_index[m_size] = m_invalid_index;

    // 初始化链表头：指向第一个空闲索引（0），版本号0
    m_head.store(pack({0, 0}), std::memory_order_release);
    m_is_initialized.store(true, std::memory_order_release);
  }

  // 分配索引（pop）：成功返回true，通过index传出分配的索引；失败（无空闲索引/未初始化）返回false
  bool pop(IndexType& index) noexcept {
    // 未初始化直接返回失败
    if (!m_is_initialized.load(std::memory_order_acquire)) {
      return false;
    }

    uint64_t old_value = m_head.load(std::memory_order_acquire);
    Node old_head = unpack(old_value);
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
                 old_value, pack(new_head),
                 std::memory_order_acq_rel,  // 成功：写操作释放语义，读操作获取语义
                 std::memory_order_acquire   // 失败：仅读取，获取语义
                 ) &&
             (old_head = unpack(old_value), true));

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

    uint64_t old_value = m_head.load(std::memory_order_acquire);
    Node old_head = unpack(old_value);
    Node new_head = old_head;

    do {
      // 回收的索引指向原链表头（头插法：新索引成为新的链表头前驱）
      m_next_free_index[index] = old_head.next_free_index;
      // 新链表头：指向回收的索引，版本号+1
      new_head.next_free_index = index;
      new_head.aba_counter = old_head.aba_counter + 1;

      // CAS原子更新链表头：成功则回收完成，失败则重试
    } while (!m_head.compare_exchange_weak(old_value, pack(new_head), std::memory_order_acq_rel,
                                           std::memory_order_acquire) &&
             (old_head = unpack(old_value), true));

    return true;
  }

  // 辅助接口：检查是否已初始化
  bool is_initialized() const noexcept { return m_is_initialized.load(std::memory_order_acquire); }

  // 辅助接口：获取容量
  IndexType capacity() const noexcept { return m_size; }

  // 辅助接口：检查是否无空闲索引（弱一致性，仅作参考）
  bool is_empty() const noexcept {
    if (!is_initialized()) {
      return true;
    }
    return unpack(m_head.load(std::memory_order_acquire)).next_free_index >= m_size;
  }

  // Inter-process use is supported only when the composite ABA-protected head
  // is implemented without a process-local library lock.
  bool is_lock_free() const noexcept { return m_head.is_lock_free(); }
};

#endif  // LOCK_FREE_LIST_H_
