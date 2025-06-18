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
#include <mutex>
#include "../yase_internal.h"

namespace yase {

// Low-level primitives for reading and writing pages
struct BaseFile {
  BaseFile() {}
  BaseFile(std::string name);
  ~BaseFile();

  // Write a page to storage
  bool FlushPage(PageId pid, void *page);

  // Load a page from storage
  bool LoadPage(PageId pid, void *out_buf);

  // Create a new page in the file; returns the ID of the new page
  PageId CreatePage();

  // Return the ID of this file
  inline int GetId() { return id; }

  // Return the number of created pages
  inline uint32_t GetPageCount() { return page_count; }

  //Return the file descriptor
  inline int GetFd() { return id; }

  // ID of this file, also the file descriptor of the underlying file
  int id;

  // Number of pages the file currently has
  std::atomic<uint32_t> page_count;
};

}  // namespace yase
