/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for log manager.
 */
#include <fcntl.h>
#include <assert.h>
#include <iostream>
#include <memory>
#include <cstdio>
#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Log/log_manager.h>

static const uint32_t kMaxRecordSize = 4000;
static thread_local char tls_rec_arena[kMaxRecordSize];

namespace yase {

GTEST_TEST(LogManager, Init) {
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  ASSERT_EQ(log->GetDurableLSN(), 0);
  ASSERT_EQ(log->GetCurrentLSN(), 0);
  ASSERT_EQ(log->logbuf_offset, 0);
  ASSERT_EQ(log->logbuf_size, 1024 * 1024);

  // Check log file
  struct stat s;
  int ret = stat("log_file", &s);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s.st_size, 0);

  yase::LogManager::Uninitialize();
  ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, Insert) {
  static const uint32_t kRecordSize = 128;

  // Small 1KB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Fake RID
  yase::RID rid(0xbeef);
  bool success = log->LogInsert(rid, tls_rec_arena, kRecordSize);
  ASSERT_TRUE(success);

  yase::LogRecord *rec = (yase::LogRecord *)log->logbuf;
  ASSERT_EQ(rec->type, yase::LogRecord::Insert);
  ASSERT_EQ(0, memcmp(tls_rec_arena, rec->payload, kRecordSize));
  ASSERT_EQ(rec->payload_size, kRecordSize);
  ASSERT_EQ(rec->id, rid.value);
  ASSERT_EQ(rec->GetChecksum(), 0);
  ASSERT_EQ((uint64_t)log->current_lsn, log->logbuf_offset);
  ASSERT_EQ(kRecordSize + sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN), log->logbuf_offset);

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, Update) {
  static const uint32_t kRecordSize = 128;

  // Small 1KB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Fake RID
  yase::RID rid(0xbeef);
  bool success = log->LogUpdate(rid, tls_rec_arena, kRecordSize);
  ASSERT_TRUE(success);

  yase::LogRecord *rec = (yase::LogRecord *)log->logbuf;
  ASSERT_EQ(rec->type, yase::LogRecord::Update);
  ASSERT_EQ(0, memcmp(tls_rec_arena, rec->payload, kRecordSize));
  ASSERT_EQ(rec->payload_size, kRecordSize);
  ASSERT_EQ(rec->id, rid.value);
  ASSERT_EQ(rec->GetChecksum(), 0);
  ASSERT_EQ((uint64_t)log->current_lsn, log->logbuf_offset);
  ASSERT_EQ(kRecordSize + sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN), log->logbuf_offset);

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, Delete) {
  // Small 1KB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Fake RID
  yase::RID rid(0xbeef);
  bool success = log->LogDelete(rid);
  ASSERT_TRUE(success);

  yase::LogRecord *rec = (yase::LogRecord *)log->logbuf;
  ASSERT_EQ(rec->type, yase::LogRecord::Delete);
  ASSERT_EQ(rec->payload_size, 0);
  ASSERT_EQ(rec->id, rid.value);
  ASSERT_EQ(rec->GetChecksum(), 0);

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, CommitAbortEnd) {
  // Small 1KB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Fake TID
  uint64_t tid1 = 999;
  bool success = log->LogCommit(tid1);
  ASSERT_TRUE(success);

  uint64_t tid2 = 998;
  success = log->LogAbort(tid2);
  ASSERT_TRUE(success);

  uint64_t tid3 = 997;
  success = log->LogEnd(tid3);
  ASSERT_TRUE(success);

  // Verify content
  yase::LogRecord *rec = (yase::LogRecord *)log->logbuf;
  ASSERT_EQ(rec->type, yase::LogRecord::Commit);
  ASSERT_EQ(rec->payload_size, 0);
  ASSERT_EQ(rec->id, tid1);
  ASSERT_EQ(rec->GetChecksum(), 0);

  rec = (yase::LogRecord *)((char *)rec + sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN));
  ASSERT_EQ(rec->type, yase::LogRecord::Abort);
  ASSERT_EQ(rec->payload_size, 0);
  ASSERT_EQ(rec->id, tid2);
  ASSERT_EQ(rec->GetChecksum(), sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN));

  rec = (yase::LogRecord *)((char *)rec + (sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN)));
  ASSERT_EQ(rec->type, yase::LogRecord::End);
  ASSERT_EQ(rec->payload_size, 0);
  ASSERT_EQ(rec->id, tid3);
  ASSERT_EQ(rec->GetChecksum(), 2 * (sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN)));

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, InsertOneFlush) {
  static const uint32_t kRecordSize = 128;

  // Small 1KB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Fake RID
  yase::RID rid(100);
  bool success = log->LogInsert(rid, tls_rec_arena, kRecordSize);
  ASSERT_TRUE(success);

  // Durable LSN should remain 0 - no flush yet
  ASSERT_EQ(log->GetDurableLSN(), 0);

  bool succeed = log->Flush();
  ASSERT_TRUE(succeed);
  ASSERT_EQ(log->GetDurableLSN(),
            (sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN) + kRecordSize));
  ASSERT_EQ(log->GetDurableLSN(), log->GetCurrentLSN());

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, FlushDuringInsert) {
  static const uint32_t kRecordSize = 128;

  // Small 1KB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Insert 6898 records, shouldn't trigger flush
  for (uint32_t i = 0; i < 6898; ++i) {
    // Fake RID
    yase::RID rid(i);
    bool success = log->LogInsert(rid, tls_rec_arena, kRecordSize);
    ASSERT_TRUE(success);
  }
  ASSERT_EQ(log->GetDurableLSN(), 0);

  // Insert one more, should trigger a flush
  yase::RID rid(500);
  bool success = log->LogInsert(rid, tls_rec_arena, kRecordSize);
  ASSERT_TRUE(success);

  uint64_t nrecs_on_flush = 1024 * 1024 / 
    (sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN) + kRecordSize);
  uint64_t size_per_rec = sizeof(yase::LogRecord) + sizeof(yase::LogManager::LSN) + kRecordSize;
  ASSERT_EQ(log->GetDurableLSN(), nrecs_on_flush * size_per_rec);

  // Issue a final flush
  bool succeed = log->Flush();
  ASSERT_TRUE(succeed);
  ASSERT_EQ(log->GetDurableLSN(), log->GetCurrentLSN());

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, ExitFlush) {
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Write something to the log buffer
  std::string str("test text in logbuf");
  memcpy(log->logbuf, str.c_str(), str.size());
  log->logbuf_offset = str.size();

  yase::LogManager::Uninitialize();

  // Check log file size and content
  struct stat s;
  int ret = stat("log_file", &s);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s.st_size, str.size());

  int fd = open("log_file", O_RDONLY);
  char buf[128];
  ret = read(fd, buf, str.size());
  ASSERT_EQ(ret, str.size());
  ASSERT_EQ(0, memcmp(str.c_str(), buf, str.size()));
  close(fd);
  ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

static const uint32_t kOps = 1000;
void insert_log(uint32_t thread_id, yase::LogManager *log) {
  static const uint32_t kRecordSize = 128;
  for (uint32_t i = 0; i < kOps; ++i) {
    // Fake RID
    yase::RID rid(i);
    int op = i % 3;
    if (op == 0) {
      bool success = log->LogInsert(rid, tls_rec_arena, kRecordSize);
      ASSERT_TRUE(success);
    } else if (op == 1) {
      bool success = log->LogUpdate(rid, tls_rec_arena, kRecordSize);
      ASSERT_TRUE(success);
    } else {
      bool success = log->LogDelete(rid);
      ASSERT_TRUE(success);
    }
  }
}

GTEST_TEST(LogManager, MultiThread) {
  static const uint32_t kThreads = 4;

  // 1MB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  std::vector<std::thread *> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(new std::thread(insert_log, i, log));
  }

  // Wait for all threads to finish
  for (auto &t : threads) {
    t->join();
    delete t;
  }

  ASSERT_EQ(log->GetDurableLSN(), 0);
  bool success = log->Flush();
  ASSERT_TRUE(success);
  ASSERT_EQ(log->GetDurableLSN(), log->GetCurrentLSN());

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

GTEST_TEST(LogManager, FlushUponCommit) {
  static const uint32_t kRecordSize = 57;

  // 1MB log buffer
  yase::LogManager::Initialize("log_file", 1);
  auto *log = yase::LogManager::Get();

  // Get a transaction to insert something
  Transaction t;
  yase::RID rid(100);
  bool success = log->LogUpdate(rid, tls_rec_arena, kRecordSize);
  ASSERT_TRUE(success);
  ASSERT_EQ(log->durable_lsn, 0);
  uint32_t rec_size = sizeof(LogRecord) + sizeof(LogManager::LSN) + kRecordSize;
  ASSERT_EQ(log->current_lsn, rec_size);
  t.Commit();
  uint32_t commit_rec_size = sizeof(LogManager::LSN) + sizeof(LogRecord);
  ASSERT_EQ(log->durable_lsn, rec_size + commit_rec_size);

  yase::LogManager::Uninitialize();
  int ret = system("rm -rf log_file");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

}  // namespace yase

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
