/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Spring 2025
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include <fcntl.h>
#include "basefile.h"

namespace yase {

BaseFile::BaseFile(std::string name) {
  // Example error handling code:
  // - Using glog to throw a fatal error if ret is less than 0 with a message "error"
  //   LOG_IF(FATAL, ret < 0) << "error";
  // - Using abort system call:
  //   if (ret < 0) {
  //     abort();
  //   }
  //
  // TODO: Your implementation
  id = open(name.c_str(), O_CREAT|O_RDWR|O_TRUNC, S_IRUSR|S_IWUSR);
  if (id < 0) {
    abort();
  }
  page_count = 0;
}

BaseFile::~BaseFile() {
  // TODO: Your implementation
  int ret = fsync(id);
  if (ret < 0) {
    abort();
  }
  close(id);
}

bool BaseFile::FlushPage(PageId pid, void *page) {
  // TODO: Your implementation
  if (!pid.IsValid()) { return false; }
  off_t offset = pid.GetPageNum() * PAGE_SIZE;
  int ret = fsync(id);
  if (ret < 0) {
    abort();
  }
  return pwrite(id, page, PAGE_SIZE, offset) == PAGE_SIZE;
}

bool BaseFile::LoadPage(PageId pid, void *out_buf) {
  // TODO: Your implementation
  if (!pid.IsValid()) { return false; }
  off_t offset = pid.GetPageNum() * PAGE_SIZE;
  return pread(id, out_buf, PAGE_SIZE, offset) == PAGE_SIZE;
}

PageId BaseFile::CreatePage() {
  // TODO: Your implementation
  PageId pid(id, page_count.fetch_add(1));
  char buffer[PAGE_SIZE] = {0};
  if (!FlushPage(pid, buffer)) {
    page_count--;
    return PageId();
  }
  return pid;
}

}  // namespace yase
