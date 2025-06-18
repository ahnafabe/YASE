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

#include <glog/logging.h>

#include "../yase_internal.h"

namespace yase {

struct DataPage {
  // Data area, including actual data that starts from data[0], and the bit array
  // (excluding record count) that precedes record count and record size
  // information which are at the end of the page
  char data[PAGE_SIZE - sizeof(uint16_t) * 2];

  // Number of valid records (occupied slots), located at the end of the page
  uint16_t record_count;

  // Record size, located at the end of the page
  uint16_t record_size;

  // Set the bit in the bit arrary for the given slot ID
  void SetBitArray(uint32_t slot_id, bool value);

  DataPage() : data{0}, record_count(0), record_size(0) {}
  DataPage(uint16_t record_size) : data{0}, record_count(0), record_size(record_size) {}

  // Returns true if the given slot is occupied
  bool SlotOccupied(uint16_t slot);

  // Retrieve a record with a given RID from the page 
  bool Read(RID rid, void *out_buf);

  // Insert a new record
  bool Insert(const char *record, uint32_t &out_slot_id);

  // Delete a record by a given RID
  bool Delete(RID rid);

  // Update a record with the given RID
  bool Update(RID rid, const char *new_record);

  // Return the maximum number of records that can be stored in this page
  static uint16_t GetCapacity(uint16_t record_size);

  inline uint16_t GetRecordSize() { return record_size; }
  inline uint16_t GetRecordCount() { return record_count; }
};

// Directory page that consists of Entries, each Entry represents a data page
struct DirectoryPage {
  struct Entry {
    uint16_t free_slots;
    bool allocated;
    bool created;
    Entry() : allocated(false), created(false) {}
  };

  Entry entries[PAGE_SIZE / sizeof(Entry)];

  static_assert(PAGE_SIZE % sizeof(Entry) == 0, "Page size not a multiple of Entry size");
};

static_assert(sizeof(DataPage) == PAGE_SIZE, "Wrong data page size");
static_assert(sizeof(DirectoryPage) == PAGE_SIZE, "Wrong dir page size");
}  // namespace yase
