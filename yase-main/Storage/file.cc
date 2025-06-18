/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include "basefile.h"
#include "buffer_manager.h"
#include "page.h"

namespace yase {

File::File(std::string name, uint16_t record_size) {
  // 1. Initialize the structure as needed; in particular the directory BaseFile should be named as
  //    "name.dir".
  // 2. The file's both BaseFiles should be registered using BufferManager::RegisterFile.
  //
  // TODO: Your implementation
  this->record_size = record_size;
  BufferManager *bm = BufferManager::Get();
  new (this) BaseFile(name);
  new (&dir) BaseFile(name + ".dir");
  bm->RegisterFile(this);
  bm->RegisterFile(&dir);

  PageId dir_page_id = dir.CreatePage();
  Page *pinned_page = bm->PinPage(dir_page_id);
  DirectoryPage *dir_page = pinned_page->GetDirPage();
  uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
  for (size_t i = 0; i < entries_per_dir_page; i++) {
    dir_page->entries[i].free_slots = DataPage::GetCapacity(record_size);
    dir_page->entries[i].allocated = false;
    dir_page->entries[i].created = false;
  }
  pinned_page->SetDirty(true);
  bm->UnpinPage(pinned_page);
}

File::~File() {
  // Nothing needed here
}

PageId File::AllocatePage() {
  
  // Notes:
  // 1 .Page access should be done through PinPage provided by the buffer pool, for example:
  //
  //      auto *bm = BufferManager::Get();
  //      Page *p = bm->PinPage(page_id);
  //      ... access page p ...
  //
  //    You should not directly use LoadPage/FlushPage interfaces to access data
  //    pages in the File class; all accesses must go through the buffer manager.
  //    Basically this means to follow the following steps:
  //
  // 2. The "Page" structure represents a "page frame" in the buffer pool and it 
  //    can contain either a data page (DataPage) or a directory page (DirectoryPage).
  //    See the definition of Page in buffer_manager.h for details. See also page.h 
  //    for definitions of DataPage and DirectoryPage.
  //
  // 3. A directory page just contains an array of directory entries, each of which
  //    records the information about a data page. The n-th entry would correspond
  //    to the n-th data page (i.e., page number n - 1). You can locate the directory
  //    entry for a particular page by dividing page size by the number of entries per
  //    directory page. Functions implemented in table.cc use a similar approach, you
  //    may adapt the approach used there.
  //
  // 4. To use the buffer manager, you need to use BufferManager::Get() to obtain the
  //    instance. See BufferManager class definition in buffermanager.h for details.
  //
  // TODO: Your implementation
  // Case 1

  BufferManager *bm = BufferManager::Get();

  PageId scavengedPage = ScavengePage();
  if(scavengedPage.IsValid()){
    Page *data_page = bm->PinPage(scavengedPage);
    data_page->SetDirty(true);
    bm->UnpinPage(data_page);
    return scavengedPage;
  }
  
  // Case 2
  static uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
  uint32_t dir_page_num = (dir.GetPageCount() / entries_per_dir_page);
  
  Page *pinned_page;
  PageId dir_page_pid = PageId(dir.GetId(), dir_page_num);
  PageId data_pid = this->CreatePage();

{
  std::lock_guard<std::mutex> lock(file_latch);
  //  Create new Dir page if not enough
  if(data_pid.GetPageNum() >= (entries_per_dir_page * dir.GetPageCount())){
    PageId new_dir_page_id = dir.CreatePage();
    pinned_page = bm->PinPage(new_dir_page_id);
    if(!pinned_page){
      return PageId();
    }
    pinned_page->Lock();
    DirectoryPage *new_dir_page = pinned_page->GetDirPage();
    for (size_t i = 0; i < entries_per_dir_page; i++) {
      new_dir_page->entries[i].free_slots = DataPage::GetCapacity(record_size);
      new_dir_page->entries[i].allocated = false;
      new_dir_page->entries[i].created = false;
    }
  }
  else{
    pinned_page = bm->PinPage(dir_page_pid);
    if (!pinned_page) {
      return PageId();
    }
    pinned_page->Lock();
  }
}

  DirectoryPage *dir_page = pinned_page->GetDirPage();
  uint32_t index = (data_pid.GetPageNum() % entries_per_dir_page);
  dir_page->entries[index].created = true;
  dir_page->entries[index].allocated = true;
  dir_page->entries[index].free_slots = DataPage::GetCapacity(record_size);
  pinned_page->SetDirty(true);
  pinned_page->Unlock();
  bm->UnpinPage(pinned_page);

  // handle data page
  Page *data_page = bm->PinPage(data_pid);
  if(!data_page){
    bm->UnpinPage(pinned_page);
    return data_pid;
  }
  data_page->Lock();
  new (data_page->GetDataPage()) DataPage(record_size);
  data_page->Unlock();
  data_page->SetDirty(true);
  bm->UnpinPage(data_page);

  return data_pid;
}

bool File::DeallocatePage(PageId data_pid) {
  // Mark the data page as deallocated in its corresponding directory page entry 
  //
  // TODO: Your implementation
  static uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
  uint32_t dir_limit = entries_per_dir_page * dir.GetPageCount();

  if(data_pid.GetPageNum() > dir_limit){
    return false;
  }
  
  BufferManager *bm = BufferManager::Get();
  uint32_t dir_page_num = (data_pid.GetPageNum() / entries_per_dir_page);
  uint32_t index = (data_pid.GetPageNum() % entries_per_dir_page);
  PageId dir_pid(dir.GetId(), dir_page_num);

  Page *pinned_page = bm->PinPage(dir_pid);
  if(!pinned_page){
    return false;
  }
  pinned_page->Lock();
  DirectoryPage *dir_page = pinned_page->GetDirPage();

  if (!dir_page->entries[index].created) {
    pinned_page->Unlock();
    bm->UnpinPage(pinned_page);
    return false;
  }

  bool allocated = dir_page->entries[index].allocated;
  if(allocated){
    dir_page->entries[index].allocated = false;

    Page *data_page = bm->PinPage(data_pid);
    data_page->Lock();
    DataPage *dp = data_page->GetDataPage();
    dp->record_count = 0;
    data_page->SetDirty(true);
    data_page->Unlock();
    bm->UnpinPage(data_page);
  }
  pinned_page->SetDirty(true);
  pinned_page->Unlock();
  bm->UnpinPage(pinned_page);
  return allocated;
}

bool File::PageExists(PageId pid) {
  // Pin the directory page corresponding to the specifid page (pid) and check whether the page is
  // in the "allocated" state.

  BufferManager *bm = BufferManager::Get();

  static uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
  uint32_t dir_page_num = (pid.GetPageNum() / entries_per_dir_page);
  uint32_t index = (pid.GetPageNum() % entries_per_dir_page);
  PageId dir_pid(dir.GetId(), dir_page_num);

  Page *pinned_page = bm->PinPage(dir_pid);
  if(!pinned_page){
    return false;
  }
  pinned_page->Lock();
  DirectoryPage *dir_page = pinned_page->GetDirPage();

  bool exists = dir_page->entries[index].allocated;
  pinned_page->Unlock();
  bm->UnpinPage(pinned_page);
  return exists;
}

PageId File::ScavengePage() {
  // Loop over all directory pages, and for each page, check if any entry represents a "created" but
  // not "allocatd" page. If so, allocate and initialize it. 
  //
  // Return the page ID of the allocated data page; an invalid page ID should be returned if no page
  // is scavenged.
  //
  // TODO: Your implementation

  BufferManager *bm = BufferManager::Get();
  uint32_t entries_per_dir_page = PAGE_SIZE / sizeof(DirectoryPage::Entry);
  PageId current_dir_pid = PageId(dir.GetId(), 0);

  for(uint32_t i=0; i < dir.GetPageCount(); i++){
    Page *pinned_page = bm->PinPage(current_dir_pid);
    if(!pinned_page){
      return PageId();
    }
    pinned_page->Lock();
    DirectoryPage *dir_page = pinned_page->GetDirPage();
    for (size_t j=0; j < entries_per_dir_page; j++) {
      if (dir_page->entries[j].created && !dir_page->entries[j].allocated) {
        dir_page->entries[j].allocated = true;
        dir_page->entries[j].free_slots = DataPage::GetCapacity(record_size);
        PageId allocated_pid = PageId(this->GetId(), i * entries_per_dir_page + j);
        pinned_page->SetDirty(true);
        pinned_page->Unlock();
        bm->UnpinPage(pinned_page);
        return allocated_pid;
      }
    }
    //move to next page
    current_dir_pid = PageId(current_dir_pid.GetFileId(), current_dir_pid.GetPageNum() + 1);
    pinned_page->Unlock();
    bm->UnpinPage(pinned_page);
  }
  return PageId();
}

}  // namespace yase