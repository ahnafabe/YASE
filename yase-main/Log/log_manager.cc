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
#include <unistd.h>
#include "log_manager.h"
#include "Storage/file.h"

namespace yase {

LogManager *LogManager::instance = nullptr;

LogManager::LogManager(const char *log_filename, uint32_t logbuf_mb) {
  // TODO: Your implementation.
  logbuf_size = logbuf_mb * 1024 * 1024;
  logbuf = new char[logbuf_size];
  logbuf_offset = 0;
  
  fd = open(log_filename, O_CREAT|O_RDWR|O_TRUNC, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    abort();
  }

  durable_lsn = 0;
  current_lsn = 0;
};

LogManager::~LogManager() {
  // TODO: Your implementation.
  LogManager::Flush();
  delete[] logbuf;
  int ret = fsync(fd);
  if (ret < 0) {
    abort();
  }
  close(fd);
}

bool LogManager::LogInsert(RID rid, const char *record, uint32_t length) {
  // TODO: Your implementation.
  // type, rid, payload size, payload(PID, length, Offset, Image), checksum(LSN)
  if(length <= 0 || !record || !rid.IsValid()){
    return false;
  }

  uint32_t log_size = sizeof(LogRecord) + length + sizeof(LSN);
  if(log_size > logbuf_size) return false;

  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lock(logbuf_latch);
    if (logbuf_offset + log_size > logbuf_size) {
      should_flush = true;
    }
  }

  // flush outside latch if needed
  if (should_flush) {
    if (!Flush()) {
      return false;
    }
  }

  std::lock_guard<std::mutex> lock(logbuf_latch);
  LogRecord *new_log = new (logbuf + logbuf_offset)LogRecord(rid.value, LogRecord::Insert, length);
  memcpy(new_log->payload, record, length);
  memcpy(new_log->payload + length, &current_lsn, sizeof(LSN));

  current_lsn += log_size;
  logbuf_offset += log_size;

  return true;
}

bool LogManager::LogUpdate(RID rid, const char *record, uint32_t length) {
  // TODO: Your implementation.
  if(length <= 0 || !record || !rid.IsValid()){
    return false;
  }

  uint32_t log_size = sizeof(LogRecord) + length + sizeof(LSN);
  if(log_size > logbuf_size) return false;

  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lock(logbuf_latch);
    if (logbuf_offset + log_size > logbuf_size) {
      should_flush = true;
    }
  }
  if (should_flush) {
    if (!Flush()) {
      return false;
    }
  }

  std::lock_guard<std::mutex> lock(logbuf_latch);
  LogRecord *new_log = new (logbuf + logbuf_offset)LogRecord(rid.value, LogRecord::Update, length);
  memcpy(new_log->payload, record, length);
  memcpy(new_log->payload + length, &current_lsn, sizeof(LSN));

  current_lsn += log_size;
  logbuf_offset += log_size;

  return true;
}

bool LogManager::LogDelete(RID rid) {
  // TODO: Your implementation.
  if(!rid.IsValid()){
    return false;
  }
  
  uint32_t log_size = sizeof(LogRecord) + sizeof(LSN);

  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lock(logbuf_latch);
    if (logbuf_offset + log_size > logbuf_size) {
      should_flush = true;
    }
  }
  if (should_flush) {
    if (!Flush()) {
      return false;
    }
  }

  std::lock_guard<std::mutex> lock(logbuf_latch);
  LogRecord *new_log = new (logbuf + logbuf_offset)LogRecord(rid.value, LogRecord::Delete, 0);
  memcpy(new_log->payload, &current_lsn, sizeof(LSN));

  current_lsn += log_size;
  logbuf_offset += log_size;

  return true;
}

bool LogManager::LogCommit(uint64_t tid) {
  // TODO: Your implementation.
  uint32_t log_size = sizeof(LogRecord) + sizeof(LSN);

  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lock(logbuf_latch);
    if (logbuf_offset + log_size > logbuf_size) {
      should_flush = true;
    }
  }
  if (should_flush) {
    if (!Flush()) {
      return false;
    }
  }

  std::lock_guard<std::mutex> lock(logbuf_latch);
  LogRecord *new_log = new (logbuf + logbuf_offset)LogRecord(tid, LogRecord::Commit, 0);
  memcpy(new_log->payload, &current_lsn, sizeof(LSN));

  current_lsn += log_size;
  logbuf_offset += log_size;

  return true;
}

bool LogManager::LogAbort(uint64_t tid) {
  // TODO: Your implementation.
  uint32_t log_size = sizeof(LogRecord) + sizeof(LSN);

  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lock(logbuf_latch);
    if (logbuf_offset + log_size > logbuf_size) {
      should_flush = true;
    }
  }
  if (should_flush) {
    if (!Flush()) {
      return false;
    }
  }

  std::lock_guard<std::mutex> lock(logbuf_latch);
  LogRecord *new_log = new (logbuf + logbuf_offset)LogRecord(tid, LogRecord::Abort, 0);
  memcpy(new_log->payload, &current_lsn, sizeof(LSN));

  current_lsn += log_size;
  logbuf_offset += log_size;

  return true;
}

bool LogManager::LogEnd(uint64_t tid) {
  // TODO: Your implementation.
  uint32_t log_size = sizeof(LogRecord) + sizeof(LSN);

  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lock(logbuf_latch);
    if (logbuf_offset + log_size > logbuf_size) {
      should_flush = true;
    }
  }
  if (should_flush) {
    if (!Flush()) {
      return false;
    }
  }

  std::lock_guard<std::mutex> lock(logbuf_latch);
  LogRecord *new_log = new (logbuf + logbuf_offset)LogRecord(tid, LogRecord::End, 0);
  memcpy(new_log->payload, &current_lsn, sizeof(LSN));

  current_lsn += log_size;
  logbuf_offset += log_size;

  return true;
}

bool LogManager::Flush() {
  // TODO: Your implementation.
  std::lock_guard<std::mutex> lock(logbuf_latch);
  if(logbuf_offset == 0){
    return true;
  }

  // write log buffer to file
  ssize_t bytes_written = pwrite(fd, logbuf, logbuf_offset, (off_t)durable_lsn);
  if (bytes_written < 0 || (size_t)bytes_written != logbuf_offset) {
    return false;
  }

  if (fsync(fd) < 0) {
    return false;
  }
  durable_lsn = current_lsn;
  logbuf_offset = 0;
  return true;
}

}  // namespace yase
