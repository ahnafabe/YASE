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

#include "../yase_internal.h"
#include "basefile.h"
#include "page.h"
#include <mutex>

namespace yase {

// Underlying structure of Table to read/write data
struct File : public BaseFile {
  File(std::string name, uint16_t record_size);
  ~File();

  std::mutex file_mutex;

  // Allocate a new data page
  // Returns the Page ID of the allocated page
  // If no page is allocated, return an invalid PageId
  PageId AllocatePage();

  // Deallocate an existing page
  // @pid: ID of the page to be deallocated
  // Returns true/false if the page is deallocated/already deallocated
  bool DeallocatePage(PageId pid);

  // Return a pointer to the dir basefile object
  inline BaseFile *GetDir() { return &dir; }

  // Return true if the specified page is allocated (i.e., "exists")
  bool PageExists(PageId pid);

  // Try to find a previously-deallocated page, and allocate it
  // Returns invalid PageId if no such page is found.
  PageId ScavengePage();

  // BaseFile for managing directory pages
  BaseFile dir;

  // Record size supported by data pages in this file
  uint16_t record_size;

  std::mutex file_latch;
};

}  // namespace yase
