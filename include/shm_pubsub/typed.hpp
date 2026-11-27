#ifndef SHM_PUBSUB_TYPED_HPP_
#define SHM_PUBSUB_TYPED_HPP_

#include <cstddef>
#include <cstring>
#include <type_traits>

#include "shm_pubsub/shm_pubsub.hpp"

namespace shm_pubsub {

// Publisher and subscriber for one trivially copyable type. The payload is
// exactly sizeof(T); messages of any other size are reported as kCorrupted
// and skipped.
template <class T, class Config = DefaultConfig>
class TypedPublisher {
 public:
  static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
  static_assert(sizeof(T) > 0, "T must not be empty");
  static_assert(sizeof(T) <= Topic<Config>::max_payload_size(), "T does not fit in a block");

  explicit TypedPublisher(Topic<Config>& topic) : publisher_(topic) {}

  Status publish(const T& value) { return publisher_.publish_copy(&value, sizeof(T)).status; }

  typename Topic<Config>::Publisher& publisher() { return publisher_; }

 private:
  typename Topic<Config>::Publisher publisher_;
};

template <class T, class Config = DefaultConfig>
class TypedSubscriber {
 public:
  static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
  static_assert(sizeof(T) > 0, "T must not be empty");
  static_assert(sizeof(T) <= Topic<Config>::max_payload_size(), "T does not fit in a block");

  explicit TypedSubscriber(Topic<Config>& topic,
                           const SubscriberOptions& options = SubscriberOptions())
      : subscriber_(topic, options) {}

  Status take(T& out) { return copy(subscriber_.take(loan_), out); }
  Status wait_take(T& out, std::chrono::milliseconds timeout) {
    return copy(subscriber_.wait_take(loan_, timeout), out);
  }

  typename Topic<Config>::Subscriber& subscriber() { return subscriber_; }

 private:
  Status copy(Status status, T& out) {
    if (status != Status::kOk) return status;
    if (loan_.size() != sizeof(T)) {
      loan_.reset();
      return Status::kCorrupted;
    }
    std::memcpy(&out, loan_.data(), sizeof(T));
    loan_.reset();
    return Status::kOk;
  }

  typename Topic<Config>::Subscriber subscriber_;
  typename Topic<Config>::ReadLoan loan_;
};

}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_TYPED_HPP_
