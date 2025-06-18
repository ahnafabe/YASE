/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include <Lock/lock_manager.h>
#include <Log/log_manager.h>

namespace yase {

// Timestamp counter
std::atomic<uint64_t> Transaction::ts_counter(0);

// Active transactions list
std::list<Transaction *> Transaction::active_transactions;

std::mutex active_txn_mutex;

// Static global lock manager instance
LockManager *LockManager::instance = nullptr;

LockManager::~LockManager() {
  // TODO: Your implementation
  for (auto &it : lock_table) {
    delete it.second;
  }
  lock_table.clear();
}

bool LockManager::AcquireLock(Transaction *tx, RID &rid, LockRequest::Mode mode, bool try_lock) {
  if(!rid.IsValid()){
    return false;
  }
  
  uint64_t rvalue = rid.value;
  std::unique_lock<std::mutex> table_latch(latch);
  auto temp = lock_table.find(rvalue);
  if (temp == lock_table.end()) {
    temp = lock_table.emplace(rvalue, new LockHead()).first;
  }
  LockHead *lock_head = temp->second;
  table_latch.unlock();

  std::unique_lock<std::mutex> head_latch(lock_head->latch);

  if (try_lock && !lock_head->requests.empty()) {
    return false;
  }

  for (auto &req : lock_head->requests) {
    if (req.requester == tx && req.mode == mode) {
      return true;
    }
  }

  bool granted_lock = (lock_head->requests.empty() ||
                   (mode == LockRequest::SH &&
                   std::all_of(lock_head->requests.begin(), lock_head->requests.end(),
                      [] (const LockRequest &r) { return r.mode == LockRequest::SH; })));
  for (const auto &req : lock_head->requests) {
    if (req.granted && ((mode == LockRequest::XL) || (mode == LockRequest::SH && req.mode == LockRequest::XL))) {
      if (tx->GetTimestamp() > req.requester->GetTimestamp()) {
        tx->Abort();
        return false;
      }
    }
  }

  lock_head->requests.emplace_back(tx, mode, granted_lock);
  if (granted_lock) {
    tx->locks.push_back(rid);
    lock_head->current_mode = mode;
    return true;
  }
  
  lock_head->cv.wait(head_latch, [tx, lock_head]() -> bool {
    auto it = std::find_if(lock_head->requests.begin(), lock_head->requests.end(),
                           [tx](const LockRequest &r){ return r.requester == tx; });
    return (it != lock_head->requests.end() && it->granted);
  });
  return true;
}

bool LockManager::ReleaseLock(Transaction *tx, RID &rid) {
  uint64_t rvalue = rid.value;
  std::unique_lock<std::mutex> table_latch(latch);
  auto temp = lock_table.find(rvalue);
  if (temp == lock_table.end()) {
    return false;
  }
  LockHead *lock_head = temp->second;
  table_latch.unlock();

  std::unique_lock<std::mutex> head_latch(lock_head->latch);
  auto it = std::find_if(lock_head->requests.begin(), lock_head->requests.end(), [tx](const LockRequest &r){ return r.requester == tx; });
  if (it == lock_head->requests.end()) {
    return false;
  }
  lock_head->requests.erase(it);

  if (!lock_head->requests.empty()) {
    auto first = lock_head->requests.begin();
    if (first->mode == LockRequest::XL) {
      first->granted = true;
      first->requester->locks.push_back(rid);
      lock_head->current_mode = static_cast<LockRequest::Mode>(first->mode);
    } else {
      for (auto req_it = lock_head->requests.begin();
           req_it != lock_head->requests.end(); ++req_it) {
        if (req_it->mode == LockRequest::SH) {
          req_it->granted = true;
          req_it->requester->locks.push_back(rid);
        } else {
          break;
        }
      }
      lock_head->current_mode = LockRequest::SH;
    }
  } else {
    lock_head->current_mode = LockRequest::NL;
  }
  
  lock_head->cv.notify_all();
  return true;
}

bool Transaction::Commit() {
  // TODO: Your implementation
  if (!IsInProgress()) {
    return false;
  }
  if (!LogManager::Get()->LogCommit(this->timestamp)) {
    state = kStateAborted;
    return false;
  }
  if (!LogManager::Get()->Flush()) {
    state = kStateAborted;
    return false;
  }
  if (!LogManager::Get()->LogEnd(this->timestamp)) {
    state = kStateAborted;
    return false;
  }
  bool all_locks_released = true;
  for (auto &rid : locks) {
    if (!LockManager::Get()->ReleaseLock(this, rid)) {
      all_locks_released = false;
    } 
  }
  {
    std::lock_guard<std::mutex> lock(active_txn_mutex);
    auto temp = std::find(active_transactions.begin(), active_transactions.end(), this);
    if (temp != active_transactions.end()) {
      active_transactions.erase(temp);
    }
  }
  state = all_locks_released ? kStateCommitted : kStateAborted;
  return all_locks_released;
}

uint64_t Transaction::Abort() {
  if (!IsInProgress()) {
    return kInvalidTimestamp;
  }

  if (!LogManager::Get()->LogAbort(this->timestamp)) {
    state = kStateAborted;
    return kInvalidTimestamp;
  }
  if (!LogManager::Get()->Flush()) {
    state = kStateAborted;
    return kInvalidTimestamp;
  }
  if (!LogManager::Get()->LogEnd(this->timestamp)) {
    state = kStateAborted;
    return kInvalidTimestamp;
  }

  for (auto &rid : locks) {
    LockManager::Get()->ReleaseLock(this, rid);
  }

  {
    std::lock_guard<std::mutex> lock(active_txn_mutex);
    auto temp = std::find(active_transactions.begin(), active_transactions.end(), this);
    if (temp != active_transactions.end()) {
      active_transactions.erase(temp);
    }
  }

  state = kStateAborted;
  return this->timestamp;
}

}  // namespace yase
