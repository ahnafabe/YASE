/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for File abstraction.
 */

#include <thread>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <Storage/buffer_manager.h>
#include <Storage/file.h>
#include <Storage/page.h>

static const uint32_t kRecordSize = 8;
class FileTests : public ::testing::Test {
 protected:
  std::string file_name;
  yase::File *file;

  void SetUp() override {
    // Initialize the buffer pool
    yase::BufferManager::Initialize(10);

    file = nullptr;
    file_name = "test_file";
    int ret = system(("rm -rf " + file_name).c_str());
    LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
  }
  void TearDown() override {
    yase::BufferManager::Uninitialize();
    if (file) {
      delete file;
    }
    int ret = system(("rm -rf " + file_name).c_str());
    LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
  }

  void NewFile() {
    file = new yase::File(file_name.c_str(), kRecordSize);
    ASSERT_TRUE(file);
  }
};

// Creation test to check empty file parameters
TEST_F(FileTests, NewFile) {
  NewFile();
  ASSERT_TRUE(file);

  // The file should occupu two file IDs, one for dir, one for data
  ASSERT_NE(file->GetId(), file->GetDir()->GetId());
}

// Create another file, now should get more IDs allocated
TEST_F(FileTests, ID) {
  NewFile();
  ASSERT_TRUE(file);

  // The file should occupu two file IDs, one for dir, one for data
  ASSERT_NE(file->GetId(), file->GetDir()->GetId());
}

// Check dir and data file names
TEST_F(FileTests, FileName) {
  NewFile();
  struct stat s;
  int ret = stat((file_name + ".dir").c_str(), &s);
  ASSERT_EQ(ret, 0);
}

// Helper function to read a data page to a provided buffer
void LoadDataPage(yase::PageId pid, yase::Page *out_page) {
  auto *bm = yase::BufferManager::Get();
  yase::Page *p = bm->PinPage(pid);
  ASSERT_NE(p, nullptr);
  memcpy(out_page->GetDataPage(), p->GetDataPage(), sizeof(yase::DataPage));
  bm->UnpinPage(p);
}

// Allocate a data page
TEST_F(FileTests, AllocatePage) {
  NewFile();
  yase::PageId pid = file->AllocatePage();
  ASSERT_TRUE(pid.IsValid());

  // The data and dir files should each have one page allocated
  ASSERT_EQ(file->GetPageCount(), 1);
  ASSERT_EQ(file->GetDir()->GetPageCount(), 1);

  // Check page parameters
  auto page_space = std::unique_ptr<char[]>(new char[sizeof(yase::Page)]);
  yase::Page *page = (yase::Page *)page_space.get();
  LoadDataPage(pid, page);
  ASSERT_EQ(page->GetDataPage()->GetRecordCount(), 0);
  ASSERT_EQ(page->GetDataPage()->GetRecordSize(), kRecordSize);
}

// Deallocate a non-existant data page
TEST_F(FileTests, DeallocateInvalidPage) {
  NewFile();
  yase::PageId pid(file->GetId(), 1000);

  // Deallocate the page
  bool success = file->DeallocatePage(pid);
  ASSERT_FALSE(success);

  yase::PageId pid2(1000, 1000);
  success = file->DeallocatePage(pid2);
  ASSERT_FALSE(success);
}

// Check physical page count after deallocation
TEST_F(FileTests, DeallocatePageCount) {
  NewFile();
  yase::PageId pid = file->AllocatePage();
  ASSERT_TRUE(pid.IsValid());

  // Deallocate the page
  bool success = file->DeallocatePage(pid);
  ASSERT_TRUE(success);

  // Raw page count in the underlying base file should still be 1
  // (only mark as deleted in dir page)
  ASSERT_EQ(file->GetPageCount(), 1);
  ASSERT_EQ(file->GetDir()->GetPageCount(), 1);
}

// Deallocate two same pages
TEST_F(FileTests, DeallocateTwoSame) {
  NewFile();
  yase::PageId pid = file->AllocatePage();
  ASSERT_TRUE(pid.IsValid());

  // Deallocate the page
  bool success = file->DeallocatePage(pid);
  ASSERT_TRUE(success);

  // Do it again
  success = file->DeallocatePage(pid);
  ASSERT_FALSE(success);
}

// No scavenge for new file
TEST_F(FileTests, NewFileNoScavenge) {
  NewFile();
  ASSERT_TRUE(file);

  yase::PageId pid = file->ScavengePage();
  ASSERT_FALSE(pid.IsValid());
}

// Scavenge a page
TEST_F(FileTests, Scavenge) {
  NewFile();
  yase::PageId pid = file->AllocatePage();
  ASSERT_TRUE(pid.IsValid());

  // Deallocate the page
  bool success = file->DeallocatePage(pid);
  ASSERT_TRUE(success);

  // Scavenge it
  auto spid = file->ScavengePage();
  ASSERT_EQ(spid.value, pid.value);
}

// Allocate scavenged page
TEST_F(FileTests, ScavengeAlloc) {
  NewFile();
  yase::PageId pid = file->AllocatePage();
  ASSERT_TRUE(pid.IsValid());

  // Deallocate the page
  bool success = file->DeallocatePage(pid);
  ASSERT_TRUE(success);

  auto apid = file->AllocatePage();
  ASSERT_EQ(apid.value, pid.value);
}

static const uint32_t kThreads = 5;
// Multi-threaded test for creating files
GTEST_TEST(File, MultiThreadCreation) {
  yase::BufferManager::Initialize(10);
  std::vector<std::thread> threads;
  std::vector<std::string> names;
  for (uint32_t i = 0; i < kThreads; ++i) {
    std::string name = std::to_string(i) + "testfile";
    threads.push_back(std::thread(
      [=](uint32_t i) {
        auto *f = new yase::File(name.c_str(), 8);
        delete f;
      },
      i));
    names.push_back(name);
  }

  // Wait for all threads to finish and check result
  for (uint32_t i = 0; i < threads.size(); ++i) {
    threads[i].join();

    // Check whether the data and dir files are created
    struct stat s;
    int ret = stat(names[i].c_str(), &s);
    ASSERT_EQ(ret, 0);

    ret = stat((names[i] + ".dir").c_str(), &s);
    ASSERT_EQ(ret, 0);

    ret = system(("rm -rf " + names[i]).c_str());
    LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";

    ret = system(("rm -rf " + names[i] + ".dir").c_str());
    LOG_IF(FATAL, ret == -1) << "Error cleaning up testing files";
  }
  yase::BufferManager::Uninitialize();
}

static const uint32_t kAllocs = 10;
void MultiThreadCreate(yase::File &file) {
  // Allocate some pages and write to them
  for (uint32_t i = 0; i < kAllocs; ++i) {
    auto pid = file.AllocatePage();
    ASSERT_TRUE(pid.IsValid());
  }

  ASSERT_EQ(file.GetPageCount(), kAllocs);

  // Create a bunch of pages
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(std::thread(
      [&](uint32_t thread_idx) {
        for (uint32_t j = 0; j < kAllocs; ++j) {
          auto pid = file.AllocatePage();
          ASSERT_TRUE(pid.IsValid());
        }
      },
      i));
  }

  for (uint32_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }

  auto total_pages = (kThreads + 1) * kAllocs;
  ASSERT_EQ(file.GetPageCount(), total_pages); 

  struct stat s;
  int ret = stat("testfile", &s);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s.st_size, (kThreads + 1) * kAllocs * PAGE_SIZE);
}

GTEST_TEST(File, MultiThreadAlloc) {
  yase::BufferManager::Initialize(10);
  yase::File file("testfile", 8);
  MultiThreadCreate(file);
  yase::BufferManager::Uninitialize();
}

void MultiThreadDealloc(yase::File &file) {
  // Randomly delete pages
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(std::thread(
      [&](uint32_t thread_idx) {
        // Each thread responsible for deallocating some pages
        for (uint32_t j = 0; j < kAllocs; ++j) {
          yase::PageId pid(file.GetId(), thread_idx * kAllocs + j);
          ASSERT_TRUE(pid.IsValid());
          bool dealloc = file.DeallocatePage(pid);
          ASSERT_TRUE(dealloc);
        }
      },
      i));
  }

  for (uint32_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }

  struct stat s;
  int ret = stat("testfile", &s);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s.st_size, (kThreads + 1) * kAllocs * PAGE_SIZE);
}

GTEST_TEST(File, MultiThreadDealloc) {
  yase::BufferManager::Initialize(10);
  yase::File file("testfile", 8);
  MultiThreadCreate(file);
  MultiThreadDealloc(file);
  yase::BufferManager::Uninitialize();
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

