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

#include <algorithm>
#include <list>
#include <memory>
#include <map>
#include <mutex>

#include <gtest/gtest_prod.h>

#include "file.h"
#include "page.h"

namespace yase {

struct BufferManager;

// Representation of a page in memory. The buffer has an array of Pages to
// accommodate DataPages and DirectoryPages.
struct Page {
  // Whether the page is dirty
  bool is_dirty;

  // Pin count - the number of users of this page
  uint16_t pin_count;

  // ID of the page held in page_data
  PageId page_id;

  // Space to hold a real page loaded from storage
  char page_data[PAGE_SIZE];

  //mutex for page protection
  std::mutex page_mutex;

  Page() : is_dirty(false), pin_count(0) {}
  ~Page() {}

  // Helper functions
  inline DataPage *GetDataPage() { return (DataPage *)page_data; }
  inline DirectoryPage *GetDirPage() { return (DirectoryPage *)page_data; }
  inline void SetDirty(bool dirty) { is_dirty = dirty; }
  inline PageId GetPageId() { return page_id; }
  inline void IncPinCount() { pin_count += 1; }
  inline void DecPinCount() { pin_count -= 1; }
  inline bool IsDirty() { return is_dirty; }
  inline uint16_t GetPinCount() { return pin_count; }

  inline void Lock() { page_mutex.lock(); }
  inline void Unlock() { page_mutex.unlock(); }
};

struct BufferManager {
  
  
  static BufferManager *instance;

  //Mutex to pretect buffer operations
  std::mutex buffer_mutex;

  //Protect LRU
  std::mutex lru_mutex;
  std::list<Page *> lru;

  inline static void Initialize(uint32_t page_count) {
    BufferManager::instance = new BufferManager(page_count);
  }
  inline static void Uninitialize() {
    delete BufferManager::instance;
  }
  inline static BufferManager *Get() { return instance; }

  BufferManager(BufferManager const &) = delete;
  void operator=(BufferManager const &) = delete;

  // Pin a page
  // @page_id: ID of the page to pin
  // Returns a page frame containing the pinned page; nullptr if the page cannot
  // be pinned
  Page *PinPage(PageId page_id);

  // Unpin a page
  // @page: Page to unpin
  void UnpinPage(Page *page);

  // Add the file ID - BaseFile* mappings to support multiple tables
  // @file: pointer to the File object
  void RegisterFile(BaseFile *bf);

  // Buffer manager constructor
  // @page_count: number of pages in the buffer pool
  BufferManager(uint32_t page_count);
  ~BufferManager();

  // File ID - BaseFile* mapping
  std::map<int, BaseFile*> file_map;

  // Page ID - Page frame mapping
  std::map<PageId, Page*, PageId> page_map;

  // Number of page frames
  uint32_t page_count;

  // An array of buffer pages
  Page *page_frames;
};
}  // namespace yase
