/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include "page.h"

namespace yase {

bool DataPage::Insert(const char *record, uint32_t &out_slot_id) {
  auto max_slots = GetCapacity(record_size);
  if (record_count + 1 > max_slots) {
    return false;
  }

  // Search through the space to find a free slot
  for (uint32_t i = 0; i < max_slots; ++i) {
    if (!SlotOccupied(i)) {
      SetBitArray(i, true);
      LOG_IF(FATAL, !SlotOccupied(i)) << "Failed setting bit array";

      out_slot_id = i;
      uint32_t off = i * record_size;
      memcpy(&data[off], record, record_size);
      ++record_count;
      return true;
    }
  }
  return false;
}

bool DataPage::Read(RID rid, void *out_buf) {
  if (!SlotOccupied(rid.GetSlotId())) {
    return false;
  }
  uint32_t off = rid.GetSlotId() * record_size;
  memcpy(out_buf, &data[off], record_size);
  return true;
}

bool DataPage::Delete(RID rid) {
  if (!SlotOccupied(rid.GetSlotId())) {
    return false;
  }

  SetBitArray(rid.GetSlotId(), false);
  LOG_IF(FATAL, SlotOccupied(rid.GetSlotId())) << "Error clearing bit array";
  --record_count;
  return true;
}

bool DataPage::Update(RID rid, const char *record) {
  if (!SlotOccupied(rid.GetSlotId())) {
    return false;
  }
  uint32_t off = rid.GetSlotId() * record_size;
  memcpy(&data[off], record, record_size);
  return true;
}

bool DataPage::SlotOccupied(uint16_t slot_id) {
  char &byte = data[PAGE_SIZE - sizeof(record_size) - sizeof(record_count) - slot_id / 8 - 1];
  uint32_t pos = slot_id % 8;
  return byte & (uint8_t{1} << pos);
}

void DataPage::SetBitArray(uint32_t slot_id, bool value) {
  uint32_t idx = PAGE_SIZE - sizeof(record_size) - sizeof(record_count) - slot_id / 8 - 1;
  char &byte = data[idx];
  uint32_t pos = slot_id % 8;
  if (value) {
    LOG_IF(FATAL, byte & (uint8_t{1} << pos));
    byte |= (uint8_t{1} << pos);
  } else {
    LOG_IF(FATAL, !(byte & (uint8_t{1} << pos)));
    byte &= ~((uint8_t{1} << pos));
  }
}

uint16_t DataPage::GetCapacity(uint16_t record_size) {
  uint32_t bits_per_record = record_size * 8 + 1;
  uint16_t nrecs = (PAGE_SIZE - sizeof(record_size) - sizeof(record_count)) * 8 / bits_per_record;
  return nrecs;
}

}  // namespace yase
