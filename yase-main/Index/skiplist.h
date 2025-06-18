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

#include <gtest/gtest_prod.h>

#include "../yase_internal.h"

namespace yase {

// Skip list node (tower)
struct SkipListNode {
  static const uint8_t kInvalidLevels = 0;

  // Tower height
  uint32_t nlevels;

  // Key size
  uint32_t key_size;

  // Payload size
  uint32_t payload_size;

  // Pointer to the next node. The i-th element represents the (i+1)th level
  SkipListNode *next[SKIP_LIST_MAX_LEVEL];

  // Key and payload (must be the last field of this struct)
  char data[0];

  SkipListNode(uint32_t nlevels, uint32_t ksize, uint32_t vsize) : 
    nlevels(nlevels), key_size(ksize), payload_size(vsize) {}
  SkipListNode() : nlevels(kInvalidLevels), key_size(-1), payload_size(-1) {}
  ~SkipListNode() {}

  char *GetKey() { return data; }
  char *GetPayload() { return data + key_size; }
};

// Skip list that maps keys to data entries
struct SkipList {
  // Constructor - create a skip list
  SkipList(uint32_t key_size, uint32_t payload_size);

  // Destructor
  ~SkipList();

  // Insert a key - data entry mapping into the skip list index
  // @key: pointer to the key
  // @payload: pointer to the payload (the "data entry")
  bool Insert(const char *key, const char *payload);

  // Search for a key in the skip list
  // @key: pointer to the key
  // @out_payload: the data entry corresponding to the search key
  // Returns true/false if the key is found/not found
  bool Search(const char *key, char *out_payload);

  // Delete a key from the index
  // @key: pointer to the key
  // Returns true if the index entry is successfully deleted, false if the key
  // does not exist
  bool Delete(const char *key);

  // Update the data entry with a new data entry
  // @key: target key
  // @payload: new payload
  // Returns true if the update was successful
  bool Update(const char *key, const char *payload);

  // Scan and return multiple keys/payloads
  // @start_key: start (smallest) key of the scan operation
  // @nkeys: number of keys to scan
  // @inclusive: whether the result (nkeys) includes [start_key] (and the payload)
  // @out_records: pointer to a vector to stores the scan result
  void Scan(const char *start_key, uint32_t nkeys, bool inclusive,
                   std::vector<std::pair<char *, char *> > *out_records);

  // Create a new skip list node (tower)
  // @levels: height of this tower
  // @key: pointer to the key
  // @payload: data entry
  SkipListNode *NewNode(uint32_t levels, const char *key, const char *payload);

  // Key size supported - should match the size recorded in node
  uint32_t key_size;

  // Payload size supported - should match the size recorded in node
  uint32_t payload_size;

  // Dummy head tower
  SkipListNode head;

  // Dummy tail tower
  SkipListNode tail;

  // Current height of the skip list
  uint32_t height;

  pthread_rwlock_t latches[SKIP_LIST_MAX_LEVEL];
};

}  // namespace yase
