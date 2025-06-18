/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include "buffer_manager.h"

namespace yase {

BufferManager *BufferManager::instance = nullptr;

// Initialize a new buffer manager
BufferManager::BufferManager(uint32_t page_count) {
  // Allocate and initialize memory for page frames
  // 1. Initialize the page_count member variable
  // 2. Allocate and initialize the desired amount of memory (specified by page_count) 
  //    and store the address in member variable page_frames
  // 3. Clear the allocated memory to 0
  std::lock_guard<std::mutex> lock(buffer_mutex);
  this->page_count = page_count;
  page_frames = new Page[page_count];
  for (uint32_t i = 0; i < page_count; ++i) {
    new (&page_frames[i]) Page();
  }
}

BufferManager::~BufferManager() {
  // Flush all dirty pages and free page frames

  std::lock_guard<std::mutex> lock(buffer_mutex);

  for (uint32_t i = 0; i < page_count; ++i) {
    if (page_frames[i].IsDirty() && page_frames[i].GetPageId().IsValid()) {
      BaseFile *file = file_map[page_frames[i].GetPageId().GetFileId()];
      file->FlushPage(page_frames[i].GetPageId(), page_frames[i].page_data);
    }
  }

  for (auto& entry: page_map) {
    delete entry.second;
  }
  page_map.clear();
  lru.clear();

  delete[] page_frames;
}

Page* BufferManager::PinPage(PageId page_id) {
  if (!page_id.IsValid()) {
    return nullptr;
  }
  std::lock_guard<std::mutex> lock(buffer_mutex);

  auto it = page_map.find(page_id);
  if (it != page_map.end()) {
    it->second->IncPinCount();

    lru.remove(it->second);
    lru.push_back(it->second);
    return it->second;
  }

  if (page_map.size() >= page_count) {
    bool evicted = false;

    while (!lru.empty()) {
      Page* victim = lru.front();
      lru.pop_front();

      if (victim->GetPinCount() == 0) {
        if (victim->IsDirty()) {
          BaseFile* file = file_map[victim->GetPageId().GetFileId()];
          if(file){
            file->FlushPage(victim->GetPageId(), victim->page_data);
          }
        }
        page_map.erase(victim->GetPageId());
        delete victim; 
        evicted = true;
        break;
      }
    }

    if (!evicted && page_map.size() >= page_count) {
      return nullptr;
    }
  }

  // Allocate new page
  Page* new_page = new Page();
  new_page->page_id = page_id;
  new_page->IncPinCount();
  page_map[page_id] = new_page;

  BaseFile* file = file_map[new_page->GetPageId().GetFileId()];
  if (file) {
    if (!file->LoadPage(page_id, new_page->page_data)) {
      lru.remove(new_page);     
      page_map.erase(page_id);
      delete new_page;
      return nullptr;
    }
  } else {
    lru.remove(new_page);
    page_map.erase(page_id);
    delete new_page;
    return nullptr;
  }

  lru.push_back(new_page);
  return new_page;
}

void BufferManager::UnpinPage(Page *page) {
  // 1. Unpin the provided page by decrementing the pin count. The page should stay in the buffer
  //    pool until another operation evicts it later.
  // 2. The page should be added to the LRU queue when its pin count becomes 0.
  if(!page) {
    return;
  }
  std::lock_guard<std::mutex> lock(buffer_mutex);

  page->DecPinCount();
  if (page->GetPinCount() == 0 ) {
    std::lock_guard<std::mutex> lru_lock(lru_mutex);
    
    if (std::find(lru.begin(), lru.end(), page) == lru.end()) {
      lru.push_back(page);
    }
  }
}

void BufferManager::RegisterFile(BaseFile *bf) {
  // Setup a mapping from [bf]'s file ID to [bf]
  if(!bf) {
    return;
  }

  std::lock_guard<std::mutex> lock(buffer_mutex);
  file_map[bf->GetId()] = bf;
}

}  // namespace yase
