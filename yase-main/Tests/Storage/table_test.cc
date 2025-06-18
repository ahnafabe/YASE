/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for Table operations.
 */

#include <assert.h>
#include <iostream>
#include <memory>
#include <cstdio>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Storage/buffer_manager.h>
#include <Storage/table.h>
#include <Log/log_manager.h>

// Single-threaded test with a single table
GTEST_TEST(Table, SimpleTableTest) {
  static const uint32_t kRecordSize = 8;
  static const uint32_t MB = 1024 * 1024;
  static const uint32_t kDataSize = 4 * MB;
  static const uint32_t kPageCount = 50;

  // Initialize the buffer pool with 50 page frames
  yase::BufferManager::Initialize(kPageCount);

  // Create a table
  yase::Table table("mytable", kRecordSize);
  uint16_t max_nrecs_per_page = yase::DataPage::GetCapacity(kRecordSize);

  for (uint32_t p = 0; p < kDataSize / PAGE_SIZE; ++p) {
    // Insert some records
    for (uint64_t i = 0; i < max_nrecs_per_page; ++i) {
      char *record = (char *)&i;
      yase::RID rid = table.Insert(record);
      ASSERT_TRUE(rid.IsValid());
      ASSERT_EQ(rid.GetSlotId(), i % max_nrecs_per_page);
      ASSERT_EQ(rid.GetPageNum(), p);
    }
  }

  // Read back the values inserted, and delete the ones with even slot numbers
  uint64_t value = 0; 
  uint32_t deleted = 0;
  for (uint32_t p = 0; p < kDataSize / PAGE_SIZE; ++p) {
    for (uint64_t i = 0; i < max_nrecs_per_page; ++i) {
      yase::RID rid(yase::PageId(table.GetFileId(), p), i);

      bool success = table.Read(rid, (void *)&value);
      ASSERT_TRUE(success);
      ASSERT_EQ(value, i); 

      if (i % 2 == 0) {
        success = table.Delete(rid);
        ASSERT_TRUE(success);
        ++deleted;
      }
    }
  }

  // Do some updates with odd-numbered records
  uint32_t p = 0;
  for (uint64_t i = 1; i < max_nrecs_per_page; i += 2) {
    uint64_t v = i + 27;
    char *record = (char *)&v;
    yase::RID rid(yase::PageId(table.GetFileId(), p), i);
    bool success = table.Update(rid, record);
    ASSERT_TRUE(success);

    // Read it back
    success = table.Read(rid, record);
    ASSERT_EQ(*(uint64_t*)record, i + 27);
  }

  // Now insert more records, reusing the deleted slots
  for (uint64_t i = 0; i < deleted; ++i) {
    char *record = (char *)&i;
    yase::RID r = table.Insert(record);
    ASSERT_TRUE(r.IsValid());
  }
  yase::BufferManager::Uninitialize();
}

int main(int argc, char **argv) {
  yase::LogManager::Initialize("log_file", 1);
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
