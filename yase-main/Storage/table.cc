/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include "table.h"
#include "buffer_manager.h"
#include "Log/log_manager.h"

namespace yase {

Table::Table(std::string name, uint32_t record_size)
  : table_name(name), file(name, record_size), record_size(record_size) {
  // Allocate a new page for the table
  next_free_pid = file.AllocatePage();
}

RID Table::Insert(const char *record) {
  // Obtain buffer manager instance 

  auto *bm = BufferManager::Get();

retry:
  PageId local_free_pid = next_free_pid;
  Page *p = bm->PinPage(local_free_pid);
  if (!p) {
    return RID();
  }
  p->Lock();
  DataPage *dp = p->GetDataPage();
  uint32_t slot = 0;
  if (!dp->Insert(record, slot)) {
    p->Unlock();
    bm->UnpinPage(p);

    std::lock_guard<std::mutex> lock(latch);
    if (next_free_pid.GetFileId() != local_free_pid.GetFileId() ||
    next_free_pid.GetPageNum() != local_free_pid.GetPageNum()) {
      goto retry;
    }

    next_free_pid = file.AllocatePage();
    if (!next_free_pid.IsValid()) {
      return RID();
    }
    goto retry;
  }

  RID new_rid = RID(next_free_pid, slot);
  if (!LogManager::Get()->LogInsert(new_rid, record, record_size)) {
    // Handle logging error (e.g., abort the operation)
    p->Unlock();
    bm->UnpinPage(p);
    return RID();
  }

  p->SetDirty(true);
  p->Unlock();
  bm->UnpinPage(p);

  // Mark the slot allocation in directory page
  static uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
  uint32_t dir_page_num = (next_free_pid.GetPageNum() / entries_per_dir_page);
  PageId dir_pid(file.GetDir()->GetId(), dir_page_num);
  p = bm->PinPage(dir_pid);
  if (!p) {
    return RID();
  }
  p->Lock();
  DirectoryPage *dirp = p->GetDirPage();

  uint32_t idx = next_free_pid.GetPageNum() % entries_per_dir_page;

  if (dirp->entries[idx].free_slots == 0) {
    p->Unlock();
    bm->UnpinPage(p);
    return RID();
  }

  if(slot != UINT32_MAX){
    --dirp->entries[idx].free_slots;
    p->SetDirty(true);
  }

  p->SetDirty(true);
  p->Unlock();
  bm->UnpinPage(p);

  return new_rid;
}

bool Table::Read(RID rid, void *out_buf) {

  if (!rid.IsValid() || !file.PageExists(rid.GetPageNum())) {
    return false;
  }

  auto *bm = BufferManager::Get();

  // Pin the requested page
  Page *p = bm->PinPage(PageId(rid.GetFileId(), rid.GetPageNum()));
  if (!p) {
    return false;
  }
  p->Lock();

  DataPage *dp = p->GetDataPage();
  bool success = dp->Read(rid, out_buf);
  p->Unlock();
  bm->UnpinPage(p);

  return success;
}

bool Table::Delete(RID rid) {
  if (!rid.IsValid()) {
    return false;
  }

  auto *bm = BufferManager::Get();
  Page *p = bm->PinPage(PageId(rid.GetFileId(), rid.GetPageNum()));
  if (!p) {
    return false;
  }
  p->Lock();

  DataPage *dp = p->GetDataPage();

  // Log before delete
  bool success = LogManager::Get()->LogDelete(rid);
  
  if (success) {
    success = dp->Delete(rid);
  }
  if(success){
    p->SetDirty(true);
  }
  
  p->Unlock();
  bm->UnpinPage(p);

  if (success) {
    uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
    uint32_t dir_page_num = rid.GetPageNum() / entries_per_dir_page;
    PageId dir_pid(file.GetDir()->GetId(), dir_page_num);
    p = bm->PinPage(dir_pid);
    if (!p) {
      return false;
    }
    p->Lock();
    DirectoryPage *dirp = p->GetDirPage();
    uint32_t idx = rid.GetPageNum() % entries_per_dir_page;
    if (dirp->entries[idx].free_slots < DataPage::GetCapacity(record_size)) {
      ++dirp->entries[idx].free_slots;
    } else{
    }
    p->SetDirty(true);
    p->Unlock();
    bm->UnpinPage(p);
  }

  return success;
}

bool Table::Update(RID rid, const char *record) {
  if (!rid.IsValid()) {
    return false;
  }

  auto *bm = BufferManager::Get();
  Page *p = bm->PinPage(PageId(rid.GetFileId(), rid.GetPageNum()));
  if (!p) {
    return false;
  }
  p->Lock();

  DataPage *dp = p->GetDataPage();

  // log before update
  bool success = LogManager::Get()->LogUpdate(rid, record, record_size);

  if (success) {
    success = dp->Update(rid, record);
  }
  if(success){
    p->SetDirty(true);
  }

  p->Unlock();
  bm->UnpinPage(p);
  return success;
}
}  // namespace yase