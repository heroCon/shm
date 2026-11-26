#ifndef SHM_PUBSUB_STATUS_HPP_
#define SHM_PUBSUB_STATUS_HPP_

#include <cstddef>
#include <cstdint>

namespace shm_pubsub {

enum class Status {
  kOk,
  kNoSubscribers,    // publish: nobody eligible; the message is dropped on purpose
  kPartial,          // publish: some targets were full under kDropNewest
  kQueueFull,        // publish: every target was full under kDropNewest
  kLoanLimit,        // the endpoint already holds its maximum number of loans
  kPoolExhausted,    // no free block even after reclamation (only after leaks)
  kMessageTooLarge,  // the payload exceeds max_payload_size()
  kInvalidArgument,  // bad size, non-empty loan, foreign loan, negative timeout
  kWouldBlock,       // take: nothing queued
  kBufferTooSmall,   // receive: the message was consumed but not copied
  kNotSubscribed,    // take on an unsubscribed endpoint
  kCorrupted,        // shared state failed validation; the message was skipped
  kNotSupported,     // the kernel lacks a required feature
  kTimedOut,         // blocking call reached its deadline
  kStopped,          // stop() cancelled a blocking call
  kBusy,             // recover(): no quiescent window observed; retry later
};

inline const char* to_string(Status status) {
  switch (status) {
    case Status::kOk:
      return "ok";
    case Status::kNoSubscribers:
      return "no_subscribers";
    case Status::kPartial:
      return "partial";
    case Status::kQueueFull:
      return "queue_full";
    case Status::kLoanLimit:
      return "loan_limit";
    case Status::kPoolExhausted:
      return "pool_exhausted";
    case Status::kMessageTooLarge:
      return "message_too_large";
    case Status::kInvalidArgument:
      return "invalid_argument";
    case Status::kWouldBlock:
      return "would_block";
    case Status::kBufferTooSmall:
      return "buffer_too_small";
    case Status::kNotSubscribed:
      return "not_subscribed";
    case Status::kCorrupted:
      return "corrupted";
    case Status::kNotSupported:
      return "not_supported";
    case Status::kTimedOut:
      return "timed_out";
    case Status::kStopped:
      return "stopped";
    case Status::kBusy:
      return "busy";
  }
  return "unknown";
}

enum class Overflow : uint32_t {
  kDropNewest = 0,  // a full queue rejects the new message
  kDropOldest = 1,  // the publisher discards the oldest queued message
};

struct PublishResult {
  Status status;
  uint32_t targets;         // private subscribers plus groups with members
  uint32_t delivered;       // targets that queued the message
  uint32_t dropped_oldest;  // older messages discarded to make room
  bool ok() const {
    return status == Status::kOk || status == Status::kNoSubscribers || status == Status::kPartial;
  }
};

struct ReceiveResult {
  Status status;
  size_t size;  // message size; set for kOk and kBufferTooSmall
};

}  // namespace shm_pubsub

#endif  // SHM_PUBSUB_STATUS_HPP_
