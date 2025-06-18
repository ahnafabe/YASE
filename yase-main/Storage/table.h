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

#include "page.h"
#include "file.h"

namespace yase {

// User-facing table abstraction
struct Table {
 public:
  Table(std::string name, uint32_t record_size);
  ~Table() {}

  // Insert a record to the table, returns the inserted record's RID
  // @record: pointer to the record
  RID Insert(const char *record);

  // Read a record with a given RID
  // @rid: RID of the record to be read
  // @out_buf: memory provided by user to store the read record
  bool Read(RID rid, void *out_buf);

  // Delete a record with the given RID
  // @rid: RID of the record to be deleted
  bool Delete(RID rid);

  // Update a record with the given RID, and the new record
  // @rid: RID of the record to be updated
  // @record: pointer to the new record value
  bool Update(RID rid, const char *record);

  // Return the ID of the underlying File
  inline int GetFileId() { return file.GetId(); }

  // The table's name
  std::string table_name;

  // The underlying file
  File file;

  // ID of the page which has free space
  PageId next_free_pid;

  // Record size supported by this table
  uint32_t record_size;

  // Latch protecting the table structure
  std::mutex latch;
};

}  // namespace yase
