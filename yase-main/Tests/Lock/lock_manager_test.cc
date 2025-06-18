/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for lock manager.
 */
#include <assert.h>
#include <thread>

#include <Lock/lock_manager.h>
#include <Log/log_manager.h>
#include <Storage/table.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace yase {

// A single requester for each lock in X mode
GTEST_TEST(LockManager, SingleRequesterXL) {
  LockManager::Initialize();
  auto *lockmgr = yase::LockManager::Get();

  // Create transactions to acquire and release locks
  yase::Transaction t;

  // Fake RIDs
  std::vector<yase::RID> rids;
  uint64_t kRids = 1000;
  for (uint64_t i = 0; i < kRids; ++i) {
    rids.emplace_back(i + 1);
  }

  // Acquire
  for (uint32_t i = 0; i < rids.size(); ++i) {
    bool acquired = lockmgr->AcquireLock(&t, rids[i], yase::LockRequest::Mode::XL, false);
    ASSERT_TRUE(acquired);

    // Check lock head and queue status
    auto &lh = *lockmgr->GetLockTableRef()[rids[i].value];
    ASSERT_EQ(lh.current_mode, LockRequest::XL);
    ASSERT_EQ(lh.requests.size(), 1);
    ASSERT_EQ(lh.requests.front().mode, LockRequest::XL);
    ASSERT_EQ(lh.requests.front().requester, &t);
    ASSERT_TRUE(lh.requests.front().granted);
  }

  // Release
  for (uint32_t i = 0; i < rids.size(); ++i) {
    bool acquired = lockmgr->AcquireLock(&t, rids[i], yase::LockRequest::Mode::XL, false);
    ASSERT_TRUE(acquired);
    lockmgr->ReleaseLock(&t, rids[i]);

    // Check lock head and queue status
    auto &lh = *lockmgr->GetLockTableRef()[rids[i].value];
    ASSERT_EQ(lh.current_mode, LockRequest::NL);
    ASSERT_EQ(lh.requests.size(), 0);
  }

  yase::LockManager::Uninitialize();
}

// A single requester for each lock in S mode
GTEST_TEST(LockManager, SingleRequesterSH) {
  LockManager::Initialize();
  auto *lockmgr = yase::LockManager::Get();

  // Create transactions to acquire and release locks
  yase::Transaction t;

  // Fake RIDs
  std::vector<yase::RID> rids;
  uint64_t kRids = 1000;
  for (uint64_t i = 0; i < kRids; ++i) {
    rids.emplace_back(i + 1);
  }

  // Acquire
  for (uint32_t i = 0; i < rids.size(); ++i) {
    bool acquired = lockmgr->AcquireLock(&t, rids[i], yase::LockRequest::Mode::SH, false);
    ASSERT_TRUE(acquired);

    // Check lock head and queue status
    auto &lh = *lockmgr->GetLockTableRef()[rids[i].value];
    ASSERT_EQ(lh.current_mode, LockRequest::SH);
    ASSERT_EQ(lh.requests.size(), 1);
    ASSERT_EQ(lh.requests.front().mode, LockRequest::SH);
    ASSERT_EQ(lh.requests.front().requester, &t);
    ASSERT_TRUE(lh.requests.front().granted);
  }

  // Release
  for (uint32_t i = 0; i < rids.size(); ++i) {
    bool acquired = lockmgr->AcquireLock(&t, rids[i], yase::LockRequest::Mode::SH, false);
    ASSERT_TRUE(acquired);
    lockmgr->ReleaseLock(&t, rids[i]);

    // Check lock head and queue status
    auto &lh = *lockmgr->GetLockTableRef()[rids[i].value];
    ASSERT_EQ(lh.current_mode, LockRequest::NL);
    ASSERT_EQ(lh.requests.size(), 0);
  }

  yase::LockManager::Uninitialize();
}

// Multiple readers per lock, should all succeed
GTEST_TEST(LockManager, MultipleSH) {
  LockManager::Initialize();
  auto *lockmgr = yase::LockManager::Get();

  // Create 10 transactions
  yase::Transaction tx[10];

  // Fake RIDs
  std::vector<yase::RID> rids;
  uint64_t kRids = 2;
  for (uint64_t i = 0; i < kRids; ++i) {
    rids.emplace_back(i + 1);
  }

  // Acquire
  for (auto &rid : rids) {
    for (auto &t : tx) {
      bool acquired = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::SH, false);
      ASSERT_TRUE(acquired);
    }
  }

  // Check lock head and queue status
  auto &lt = lockmgr->GetLockTableRef();
  for (auto &rid : rids) {
    auto *lh = lt[rid.value];
    ASSERT_NE(lh, nullptr);
    ASSERT_EQ(lh->current_mode, LockRequest::SH);
    ASSERT_EQ(lh->requests.size(), 10);
    for (auto it = lh->requests.begin(); it != lh->requests.end(); it++) {
      ASSERT_TRUE(it->granted);
      ASSERT_EQ(it->mode, LockRequest::SH);
    }
  }

  // Release
  for (auto &rid : rids) {
    for (auto &t : tx) {
      bool success = lockmgr->ReleaseLock(&t, rid);
      ASSERT_TRUE(success);
    }
  }

  yase::LockManager::Uninitialize();
}

GTEST_TEST(LockManager, AlreadyAcquired) {
  LockManager::Initialize();
  auto *lockmgr = yase::LockManager::Get();
  
  Transaction t;

  // Fake RID
  yase::RID rid(0xbeef);

  // T1 acquires in S mode
  bool acquired = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::SH, false);
  ASSERT_TRUE(acquired);

  // Do it again
  acquired = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::SH, false);
  ASSERT_TRUE(acquired);

  LockManager::Uninitialize();
}

GTEST_TEST(LockManager, AlreadyReleased) {
  LockManager::Initialize();
  auto *lockmgr = yase::LockManager::Get();
  
  Transaction t;

  // Fake RID
  yase::RID rid(0xbeef);

  // T1 acquires in X mode
  bool success = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::XL, false);
  ASSERT_TRUE(success);

  // Release it
  success = lockmgr->ReleaseLock(&t, rid);
  ASSERT_TRUE(success);

  // Do it again
  success = lockmgr->ReleaseLock(&t, rid);
  ASSERT_FALSE(success);

  LockManager::Uninitialize();
}

// Test FIFO ordering: SH -> XL -> SH
GTEST_TEST(LockManager, FIFO) {
  LockManager::Initialize();
  auto *lockmgr = yase::LockManager::Get();

  yase::Transaction t3;
  yase::Transaction t2;
  yase::Transaction t1;

  // Fake RID
  yase::RID rid(0xbeef);

  // T1 acquires in S mode
  bool acquired = lockmgr->AcquireLock(&t1, rid, LockRequest::Mode::SH, false);
  ASSERT_TRUE(acquired);

  // T2 acquires in X mode - will wait
  auto t2_thd = [&]() {
    bool success = LockManager::Get()->AcquireLock(&t2, rid, LockRequest::Mode::XL, false);
    ASSERT_TRUE(success);
  };
  std::thread t2_thread(t2_thd);

  // Wait for t2 to line up in the queue
  auto &lt = lockmgr->GetLockTableRef();
  auto *lh = lt[rid.value];
  uint32_t count = 0;
  while (count != 2) {
    lh->latch.lock();
    count = lh->requests.size();
    lh->latch.unlock();
  }

  // T3 acquires in S mode - should wait too
  auto t3_thd = [&]() {
    bool success = LockManager::Get()->AcquireLock(&t3, rid, LockRequest::Mode::SH, false);
    ASSERT_TRUE(success);
  };
  std::thread t3_thread(t3_thd);


  // Similarly wait for t3 to line up in the queue
  lt = lockmgr->GetLockTableRef();
  lh = lt[rid.value];

  while (count != 3) {
    lh->latch.lock();
    count = lh->requests.size();
    lh->latch.unlock();
  }

  // Check lock head and queue status
  lt = lockmgr->GetLockTableRef();
  lh = lt[rid.value];
  ASSERT_NE(lh, nullptr);
  ASSERT_EQ(lh->current_mode, LockRequest::SH);
  LOG_IF(FATAL, lh->requests.size() != 3);
  ASSERT_EQ(lh->requests.size(), 3);
  uint32_t n = 0;
  for (auto it = lh->requests.begin(); it != lh->requests.end(); it++) {
    if (n == 0) {
      ASSERT_TRUE(it->granted);
      ASSERT_EQ(it->requester, &t1);
      ASSERT_EQ(it->mode, LockRequest::SH);
    } else {
      ASSERT_FALSE(it->granted);
      if (n == 1) {
        ASSERT_EQ(it->mode, LockRequest::XL);
        ASSERT_EQ(it->requester, &t2);
      } else {
        ASSERT_EQ(it->mode, LockRequest::SH);
        ASSERT_EQ(it->requester, &t3);
      }
    }
    ++n;
  }

  // T1 releases lock so T2 can grab the lock
  bool success = lockmgr->ReleaseLock(&t1, rid);
  ASSERT_TRUE(success);

  // Wait for T2 to finish acquiring the lock
  t2_thread.join();

  // Release for T2 so T3 can be granted the lock
  success = lockmgr->ReleaseLock(&t2, rid);
  ASSERT_TRUE(success);

  // Wait for T3 to finish acquiring the lock
  t3_thread.join();

  // Release for T3
  success = lockmgr->ReleaseLock(&t3, rid);
  ASSERT_TRUE(success);

  yase::LockManager::Uninitialize();
}

// Transaction should maintain a list of all acquired locks
GTEST_TEST(LockManager, TransactionLockList) {
  yase::LockManager::Initialize();

  // Fake RID
  yase::RID rid1(0xbeef);
  yase::RID rid2(0xfeed);
  yase::RID rid3(0xdeed);

  // Acquire some locks by a tx
  Transaction t;
  auto *lockmgr = LockManager::Get();
  bool success = lockmgr->AcquireLock(&t, rid1, yase::LockRequest::Mode::SH, false);
  ASSERT_TRUE(success);

  success = lockmgr->AcquireLock(&t, rid2, yase::LockRequest::Mode::XL, false);
  ASSERT_TRUE(success);

  success = lockmgr->AcquireLock(&t, rid3, yase::LockRequest::Mode::SH, false);
  ASSERT_TRUE(success);

  ASSERT_EQ(t.locks.size(), 3);
  auto iter = t.locks.begin();
  ASSERT_EQ(iter->value, rid1.value);
  ASSERT_EQ((++iter)->value, rid2.value);
  ASSERT_EQ((++iter)->value, rid3.value);

  yase::LockManager::Uninitialize();
}

// Success commit case
GTEST_TEST(LockManager, TransactionCommit) {
  yase::LockManager::Initialize();

  // Creation, should be in progress
  Transaction t;
  ASSERT_TRUE(t.IsInProgress());

  // Acquire lock
  yase::RID rid1(0xbeef);
  auto *lockmgr = LockManager::Get();
  bool success = lockmgr->AcquireLock(&t, rid1, yase::LockRequest::Mode::SH, false);
  ASSERT_TRUE(success);

  // Should still be in progress
  ASSERT_TRUE(t.IsInProgress());

  // Commit
  success = t.Commit();
  ASSERT_TRUE(success);
  ASSERT_TRUE(t.IsCommitted());

  LockManager::Uninitialize();
}

// Active transactions list
GTEST_TEST(LockManager, ActiveTransactions) {
  yase::LockManager::Initialize();

  // Creation, should be in progress
  Transaction *t = new Transaction;
  ASSERT_TRUE(t->IsInProgress());

  // Should exist in the active transactions list
  bool found = false;
  for (auto &at : yase::Transaction::active_transactions) {
    if (at == t) {
      found = true;
    }
  }
  ASSERT_TRUE(found);

  // Acquire lock
  yase::RID rid1(0xbeef);
  auto *lockmgr = LockManager::Get();
  bool success = lockmgr->AcquireLock(t, rid1, yase::LockRequest::Mode::SH, false);
  ASSERT_TRUE(success);

  // Should still be in progress
  ASSERT_TRUE(t->IsInProgress());

  // Commit
  success = t->Commit();
  ASSERT_TRUE(success);
  ASSERT_TRUE(t->IsCommitted());

  // Should not exist in the active transactions list
  found = false;
  for (auto &at : yase::Transaction::active_transactions) {
    if (at == t) {
      found = true;
    }
  }
  ASSERT_FALSE(found);
  delete t;

  LockManager::Uninitialize();
}

// Testing deadlock
GTEST_TEST(LockManager, DeadlockWaitDie) {
  yase::LockManager::Initialize();

  // Create two transaction to acquire and release locks
  yase::Transaction t1;
  yase::Transaction t2;

  ASSERT_TRUE(t2.GetTimestamp() > t1.GetTimestamp());

  // Fake RID
  yase::RID rid1(0xbeef);
  yase::RID rid2(0xdead);

  auto *lockmgr = yase::LockManager::Get();

  // T1 locks rid1 in SH mode
  bool acquired = lockmgr->AcquireLock(&t1, rid1, yase::LockRequest::Mode::SH, false);
  ASSERT_TRUE(acquired);

  // T2 locks rid2 in XL mode
  acquired = lockmgr->AcquireLock(&t2, rid2, yase::LockRequest::Mode::XL, false);
  ASSERT_TRUE(acquired);

  ASSERT_TRUE(t1.IsInProgress());
  ASSERT_TRUE(t2.IsInProgress());

  auto ddl_thread = [&](yase::LockManager *lm,
                        yase::Transaction *t,
                        yase::RID *rid,
                        yase::LockRequest::Mode mode,
                        bool try_lock) {
    if (!lm->AcquireLock(t, *rid, mode, try_lock)) {
      bool success = t->Abort();
      ASSERT_TRUE(success);
    }
  };

  // Start two threads to create a deadlock
  // T1 is older (smaller timestamp), so it will wait
  std::thread thread1(ddl_thread, lockmgr, &t1, &rid2, yase::LockRequest::Mode::SH, false);

  // T2 is younger, so it will not wait
  std::thread thread2(ddl_thread, lockmgr, &t2, &rid1, yase::LockRequest::Mode::XL, false);

  thread1.join();
  thread2.join();

  // T2 should have been aborted
  ASSERT_TRUE(t2.IsAborted());
  t1.Commit();
  ASSERT_TRUE(t1.IsCommitted());

  yase::LockManager::Uninitialize();
}

}  // namespace yase

int main(int argc, char **argv) {
  yase::LogManager::Initialize("log_file", 1);
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
