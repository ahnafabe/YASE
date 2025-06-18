/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#pragma once

#include <atomic>
#include <list>

#include <glog/logging.h>

namespace yase {

// Page ID - a 64-bit integer
struct PageId {
  // Represents an Invalid ID
  static const uint64_t kInvalidValue = ~uint64_t{0};

  // Structure of the Page ID:
  // |---32 bits--|--16 bits--|--16 bits--|
  // |  File ID   | Page Num  |  Unused  |
  uint64_t value;

  // Constructors
  PageId() : value(kInvalidValue) {}
  PageId(int file_id, uint32_t page_num) {
    value = (((uint64_t)file_id) << 32UL) | (((uint64_t)page_num) << 16UL);
  }
  PageId(uint64_t value) : value(value) {
    LOG_IF(FATAL, !IsValid()) << "Invalid value";
  }

  inline bool IsValid() { return value != kInvalidValue; }
  inline uint32_t GetPageNum() const { return (value & 0xffffffff) >> 16; }
  inline uint32_t GetFileId() const { return value >> 32UL; }

  // Custom operator
  bool operator()(const PageId& lhs, const PageId& rhs) const {
    return lhs.value < rhs.value;
  }
  bool operator<(const PageId& temp) {
    return value < temp.value;
  }
};

// Record ID - Same as PageId, but uses the 8 LSBs: 
// |---32 bits--|--16 bits--|--16 bits--|
struct RID : PageId {
  // Constructors
  RID(uint64_t v) { value = v;}
  RID() { value = kInvalidValue; }
  RID(PageId page_id, uint16_t slot_id) { value = page_id.value | slot_id; }

  inline uint32_t GetSlotId() { return value & 0xffff; }
  inline bool IsValid() { return value != kInvalidValue; }
};

struct Transaction {
  // Timestamp counter
  static std::atomic<uint64_t> ts_counter;

  // Active transactions list
  static std::list<Transaction *> active_transactions;

  const static uint32_t kStateCommitted = 1;
  const static uint32_t kStateInProgress = 2;
  const static uint32_t kStateAborted = 3;
  const static uint64_t kInvalidTimestamp = ~uint64_t{0};

  // Transaction timestamp. Smaller == older.
  std::atomic<uint64_t> timestamp;

  // List of the RID of records which the transaction currently locked.
  // Other implementations may maintain a list of LockRequest references;
  // for this project we use RIDs.
  std::list<RID> locks;

  // State of the transaction, must be one of the kState* values
  uint32_t state;

  Transaction() : timestamp(ts_counter.fetch_add(1)), state(kStateInProgress) {active_transactions.push_back(this);}
  ~Transaction() {}

  // Abort the transaction
  // Returns the timestamp of this transaction.
  // If the operation fails, return invalid timestamp (kInvalidTimestamp)
  uint64_t Abort();

  // Commit the transaction
  // Returns true/false if the operation succeeded/failed.
  bool Commit();

  inline uint64_t GetTimestamp() { return timestamp; }
  inline bool IsAborted() { return state == kStateAborted; }
  inline bool IsCommitted() { return state == kStateCommitted; }
  inline bool IsInProgress() { return state == kStateInProgress; }
};
}  // namespace yase
