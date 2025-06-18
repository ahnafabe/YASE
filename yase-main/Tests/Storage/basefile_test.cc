/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for BaseFile abstraction.
 */

#include <fcntl.h>
#include <thread>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Storage/basefile.h>

class BaseFileTests : public ::testing::Test {
 protected:
  std::string bfile_name;
  yase::BaseFile *bfile;

  void SetUp() override {
    bfile = nullptr;
    bfile_name = "test_base";
    int ret = system(("rm -rf " + bfile_name).c_str());
    LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
  }
  void TearDown() override {
    if (bfile) {
      delete bfile;
    }
    int ret = system(("rm -rf " + bfile_name).c_str());
    LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
  }

  void NewBaseFile() {
    bfile = new yase::BaseFile(bfile_name.c_str());
    ASSERT_TRUE(bfile);
  }
};

// Creation test to check empty file parameters
TEST_F(BaseFileTests, NewBaseFileID) {
  NewBaseFile();
  ASSERT_TRUE(bfile);

  // Remember the file ID
  int first_fd = bfile->GetId();
  ASSERT_GT(first_fd, 0);

  // Another file, ID should be greater than first_fd
  yase::BaseFile *another_bfile = new yase::BaseFile(bfile_name.c_str());
  ASSERT_TRUE(another_bfile);
  ASSERT_GT(another_bfile->GetId(), first_fd);
  delete another_bfile;
}

// Page count should be 0
TEST_F(BaseFileTests, InitPageCount) {
  NewBaseFile();
  ASSERT_EQ(bfile->GetPageCount(), 0);
}


// The file's raw initial size should be 0
TEST_F(BaseFileTests, InitSize) {
  NewBaseFile();
  struct stat s;
  stat(bfile_name.c_str(), &s);
  ASSERT_EQ(s.st_size, 0);
}

// Create, read and write some pages
void AccessFile(yase::BaseFile *bf, uint32_t npages) {
  void *p = malloc(PAGE_SIZE);
  ASSERT_NE(p, nullptr);
  for (uint32_t i = 0; i < npages; ++i) {
    yase::PageId pid = bf->CreatePage();
    ASSERT_TRUE(pid.IsValid());

    // Load the page, should succeed
    bool success = bf->LoadPage(pid, p);
    ASSERT_TRUE(success);

    // Write the page back
    for (uint32_t j = 0; j < PAGE_SIZE / sizeof(uint32_t); ++j) {
      ((uint32_t *)p)[j] = j;
    }
    bf->FlushPage(pid, p);

    // Load again and verify result
    success = bf->LoadPage(pid, p);
    ASSERT_TRUE(success);
    for (uint32_t j = 0; j < PAGE_SIZE / sizeof(uint32_t); ++j) {
      ASSERT_EQ(((uint32_t *)p)[j], j);
    }
  }
  free(p);
}

// Load a page from a new empty file, should fail
TEST_F(BaseFileTests, LoadEmpty) {
  NewBaseFile();

  void *p = malloc(PAGE_SIZE);
  ASSERT_NE(p, nullptr);
  bool success = bfile->LoadPage(yase::PageId(0), p);
  free(p);
  ASSERT_FALSE(success);
}

// Create a page in a new file
TEST_F(BaseFileTests, CreatePage) {
  NewBaseFile();
  yase::PageId pid = bfile->CreatePage();
  ASSERT_TRUE(pid.IsValid());
}

// Single-threaded test with a single file
TEST_F(BaseFileTests, SingleThreadAccess) {
  static const uint32_t kPages = 10;

  NewBaseFile();

  AccessFile(bfile, kPages);

  // Check raw file size
  struct stat s;
  stat(bfile_name.c_str(), &s);
  ASSERT_EQ(s.st_size, kPages * PAGE_SIZE);
}

// Multi-threaded test of BaseFile
TEST_F(BaseFileTests, MultiThreadAccess) {
  // Your code here
  static const uint32_t kPages = 10;
  static const uint32_t kThreads = 4;
  NewBaseFile();
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([this]() { AccessFile(bfile, kPages); });
  }
  for (auto& t : threads) {
    t.join();
  }

  struct stat s;
  stat(bfile_name.c_str(), &s);
  ASSERT_EQ(s.st_size, kThreads * kPages * PAGE_SIZE);
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

