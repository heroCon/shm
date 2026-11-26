// Copyright (c) 2019 by Robert Bosch GmbH. All rights reserved.
// Copyright (c) 2021 - 2022 by Apex.AI Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// The lock-free free-list algorithm in this file is adapted from iceoryx
// MpmcLoFFLi (https://github.com/eclipse-iceoryx/iceoryx).
//   Apache-2.0: https://www.apache.org/licenses/LICENSE-2.0
//   MIT:        https://opensource.org/licenses/MIT
//
// Ownership contract inherited from iceoryx:
//   * pop() transfers unique ownership of the returned index to the caller.
//   * push(index) may only be called by the current unique owner of index.
//   * Transferring an index to another thread or process before push() must
//     provide synchronization.
//   * The invalid-index check in push() only rejects completed double frees;
//     it does not protect against concurrent duplicate pushes of one index.
// Violating this contract can corrupt the list even though the head carries an
// ABA counter.

#ifndef SHM_PUBSUB_DETAIL_LOCK_FREE_LIST_HPP_
#define SHM_PUBSUB_DETAIL_LOCK_FREE_LIST_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace shm_pubsub {
namespace detail {

// Indices replace pointers because every process maps the list at a
// different address.
template <typename IndexType = uint32_t, size_t kCapacity = 32>
class LockFreeFreeList {
 private:
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

  std::atomic<uint64_t> m_head{0};  // packed 32-bit index + 32-bit ABA counter
  std::atomic<IndexType> m_next_free_index[kCapacity + 1];  // includes a tail sentinel
  IndexType m_size;
  IndexType m_invalid_index;  // marks an allocated index
  std::atomic<bool> m_is_initialized{false};

  static_assert(std::is_unsigned<IndexType>::value, "IndexType must be an unsigned integer type");
  static_assert(sizeof(IndexType) <= sizeof(uint32_t), "packed head supports up to 32-bit indices");
  static_assert(kCapacity > 0, "A capacity of 0 is not supported");
  static_assert(kCapacity < std::numeric_limits<IndexType>::max() - 1,
                "Capacity leaves no room for reserved sentinel indices");

 public:
  void init() {
    if (m_is_initialized.load(std::memory_order_acquire)) {
      throw std::logic_error("Free list has already been initialized!");
    }
    m_size = kCapacity;
    m_invalid_index = m_size + 1;
    for (IndexType i = 0; i < m_size; ++i) {
      std::atomic_init(&m_next_free_index[i], static_cast<IndexType>(i + 1));
    }
    std::atomic_init(&m_next_free_index[m_size], m_invalid_index);
    m_head.store(pack({0, 0}), std::memory_order_release);
    m_is_initialized.store(true, std::memory_order_release);
  }

  bool pop(IndexType& index) noexcept {
    if (!m_is_initialized.load(std::memory_order_acquire)) return false;
    uint64_t old_value = m_head.load(std::memory_order_acquire);
    Node old_head = unpack(old_value);
    Node new_head = old_head;
    do {
      if (old_head.next_free_index >= m_size) return false;
      // A competing pop may already own and modify this node. The CAS rejects
      // a stale head; the speculative link read must still be atomic.
      new_head.next_free_index =
          m_next_free_index[old_head.next_free_index].load(std::memory_order_relaxed);
      new_head.aba_counter = old_head.aba_counter + 1;
    } while (!m_head.compare_exchange_weak(old_value, pack(new_head), std::memory_order_acq_rel,
                                           std::memory_order_acquire) &&
             (old_head = unpack(old_value), true));
    index = static_cast<IndexType>(old_head.next_free_index);
    m_next_free_index[index].store(m_invalid_index, std::memory_order_relaxed);
    return true;
  }

  bool push(const IndexType index) noexcept {
    if (!m_is_initialized.load(std::memory_order_acquire)) return false;
    if (index >= m_size ||
        m_next_free_index[index].load(std::memory_order_relaxed) != m_invalid_index) {
      return false;
    }
    uint64_t old_value = m_head.load(std::memory_order_acquire);
    Node old_head = unpack(old_value);
    Node new_head = old_head;
    do {
      m_next_free_index[index].store(static_cast<IndexType>(old_head.next_free_index),
                                     std::memory_order_relaxed);
      new_head.next_free_index = index;
      new_head.aba_counter = old_head.aba_counter + 1;
    } while (!m_head.compare_exchange_weak(old_value, pack(new_head), std::memory_order_acq_rel,
                                           std::memory_order_acquire) &&
             (old_head = unpack(old_value), true));
    return true;
  }

  // Returns an index whose owner died between the pop CAS and the allocation
  // mark. Only recovery may call this, after proving no live owner exists.
  bool force_push(const IndexType index) noexcept {
    if (index >= m_size) return false;
    m_next_free_index[index].store(m_invalid_index, std::memory_order_relaxed);
    return push(index);
  }

  // Visits the list from its head. Only meaningful while no participant
  // modifies the list; returns false if the walk does not terminate.
  template <typename F>
  bool for_each_free(F visit) const {
    if (!is_initialized()) return false;
    uint32_t index = unpack(m_head.load(std::memory_order_acquire)).next_free_index;
    for (size_t steps = 0; index < m_size; ++steps) {
      if (steps >= kCapacity) return false;
      visit(static_cast<IndexType>(index));
      index = m_next_free_index[index].load(std::memory_order_relaxed);
    }
    return true;
  }

  bool is_initialized() const noexcept { return m_is_initialized.load(std::memory_order_acquire); }
  IndexType capacity() const noexcept { return m_size; }
  bool is_empty() const noexcept {
    if (!is_initialized()) return true;
    return unpack(m_head.load(std::memory_order_acquire)).next_free_index >= m_size;
  }
};

}  // namespace detail
}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_DETAIL_LOCK_FREE_LIST_HPP_
