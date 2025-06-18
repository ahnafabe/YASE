/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for BufferManager.
 */

#include <assert.h>
#include <iostream>
#include <memory>
#include <cstdio>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Storage/buffer_manager.h>

namespace yase {

static const uint32_t kPageCount = 10;
class BufferManagerTests : public ::testing::Test {
 protected:
  yase::BufferManager *bm;

  void SetUp() override {
    bm = nullptr;
  }
  void TearDown() override {
    yase::BufferManager::Uninitialize();
  }

  void NewBufferManager() {
    yase::BufferManager::Initialize(kPageCount);
    bm = yase::BufferManager::Get();
    ASSERT_TRUE(bm);
  }
};

// Initialization and check basic states
TEST_F(BufferManagerTests, Init) {
  NewBufferManager();

  ASSERT_EQ(kPageCount, bm->page_count);
  ASSERT_EQ(bm->file_map.size(), 0);

  // Page frame initialization
  for (uint32_t i = 0 ; i < kPageCount; ++i) {
    yase::Page *p = &bm->page_frames[i];
    ASSERT_FALSE(p->is_dirty);
    ASSERT_EQ(p->pin_count, 0);
    ASSERT_EQ(p->page_id.value, yase::PageId().value);
  }
}

// Register a BaseFile to the buffer manager
TEST_F(BufferManagerTests, Register) {
  yase::BaseFile bf("test_reg");
  NewBufferManager();
  bm->RegisterFile(&bf);

  ASSERT_EQ(bm->file_map.size(), 1);
  ASSERT_EQ(bm->file_map[bf.GetId()], &bf);
  int ret = system("rm -rf test_reg");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

// Pin an invalid page, should fail
TEST_F(BufferManagerTests, PinInvalid) {
  yase::BaseFile bf("test_reg");
  NewBufferManager();
  bm->RegisterFile(&bf);

  yase::Page *p = bm->PinPage(yase::PageId());
  ASSERT_EQ(p, nullptr);

  int ret = system("rm -rf test_reg");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

// Pin an existing page
TEST_F(BufferManagerTests, PinExisting) {
  yase::BaseFile bf("test_reg");
  NewBufferManager();
  bm->RegisterFile(&bf);

  yase::PageId pid = bf.CreatePage();
  yase::Page *p = bm->PinPage(pid);
  ASSERT_NE(p, nullptr);

  ASSERT_EQ(p->pin_count, 1);
  ASSERT_EQ(p->page_id.value, pid.value);
  ASSERT_FALSE(p->is_dirty);

  int ret = system("rm -rf test_reg");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

// Pin a already-buffered page
TEST_F(BufferManagerTests, PinHit) {
  yase::BaseFile bf("test_reg");
  NewBufferManager();
  bm->RegisterFile(&bf);

  yase::PageId pid = bf.CreatePage();
  yase::Page *p = bm->PinPage(pid);
  ASSERT_NE(p, nullptr);

  ASSERT_EQ(p->pin_count, 1);
  ASSERT_EQ(p->page_id.value, pid.value);
  ASSERT_FALSE(p->is_dirty);

  yase::Page *pp = bm->PinPage(pid);
  ASSERT_EQ(pp->pin_count, 2);
  ASSERT_EQ(pp->page_id.value, pid.value);
  ASSERT_FALSE(pp->is_dirty);

  int ret = system("rm -rf test_reg");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

// Unpin a page
TEST_F(BufferManagerTests, Unpin) {
  yase::BaseFile bf("test_reg");
  NewBufferManager();
  bm->RegisterFile(&bf);

  yase::PageId pid = bf.CreatePage();
  yase::Page *p = bm->PinPage(pid);

  bm->UnpinPage(p);
  ASSERT_EQ(p->pin_count, 0);
  ASSERT_FALSE(p->is_dirty);

  int ret = system("rm -rf test_reg");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

// Test page eviction
TEST_F(BufferManagerTests, Evict) {
  yase::BaseFile bf("test_reg");
  NewBufferManager();
  bm->RegisterFile(&bf);

  // Fully occupy the buffer pool
  yase::Page *p = nullptr;
  for (uint32_t i = 0; i < kPageCount; ++i) {
    yase::PageId pid = bf.CreatePage();
    p = bm->PinPage(pid);
    ASSERT_NE(p, nullptr);

    ASSERT_EQ(p->pin_count, 1);
    ASSERT_EQ(p->page_id.value, pid.value);
    ASSERT_FALSE(p->is_dirty);
  }

  // Dirty and unpin a page
  p->SetDirty(true);
  bm->UnpinPage(p);

  // Pin another, [p] will be forced out
  yase::PageId pid = bf.CreatePage();
  yase::Page *pp = bm->PinPage(pid);
  ASSERT_NE(pp, nullptr);
  ASSERT_EQ(pp->page_id.value, pid.value);
  ASSERT_FALSE(pp->is_dirty);

  int ret = system("rm -rf test_reg");
  LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
}

}  // namespace yase

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

