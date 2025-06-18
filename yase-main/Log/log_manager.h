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

#include <mutex>
#include <yase_internal.h>

namespace yase {

struct LogRecord {
  enum Type {
    Insert, // Insert log record
    Update, // Update log record
    Delete, // Delete log record
    Commit, // Commit log record
    Abort,  // Abort log record
    End,    // End log record
  };

  // Will represent a TID if this is a commit/abort/end record; otherwise an RID
  uint64_t id;
  Type type;
  uint32_t payload_size;
  char payload[0];

  LogRecord(uint64_t id, Type type, uint32_t payload_size) :
    id(id), type(type), payload_size(payload_size) {}

  // Checksum (record LSN) follows the payload
  inline uint64_t GetChecksum() { return *(uint64_t *)((char *)payload + payload_size); }
};

// Simple logging facility, recovery not implemented
struct LogManager {
  typedef uint64_t LSN;

  static LogManager *instance;

  // Log buffer size in bytes
  uint32_t logbuf_size;

  // Log buffer
  char *logbuf;

  // Offset of the next free byte in the log buffer
  uint32_t logbuf_offset;

  // Latch protecting the log buffer
  std::mutex logbuf_latch;

  // File descriptor for log file
  int fd;

  // Durable LSN
  LSN durable_lsn;

  // Current LSN
  LSN current_lsn;

  // Constructor
  // @log_filename: file name for the persistent log
  // @logbuf_mb: log buffer size in MB
  LogManager(const char *log_filename, uint32_t logbuf_mb);
  ~LogManager();

  inline static void Initialize(const char *log_filename, uint32_t logbuf_mb) {
    LogManager::instance = new LogManager(log_filename, logbuf_mb);
  }
  inline static void Uninitialize() { delete instance; }
  inline static LogManager *Get() { return instance; }

  // Log an insert operation
  // @rid: RID of the new record
  // @record: new record
  // @length: size of the data record
  // Return true/false if the logging operation succeeded/failed
  bool LogInsert(RID rid, const char *record, uint32_t length);

  // Log an update operation
  // @rid: RID of the updated record
  // @record: pointer to new record value (i.e., after-image)
  // @length: size of the data record
  // Return true/false if the logging operation succeeded/failed
  bool LogUpdate(RID rid, const char *record, uint32_t length);

  // Log a delete operation
  // @rid: RID of the deleted record
  // Return true/false if the logging operation succeeded/failed
  bool LogDelete(RID rid);

  // Log a commit operation
  // @tid: ID of the committing transaction
  // Return true/false if the logging operation succeeded/failed
  bool LogCommit(uint64_t tid);

  // Log an abort operation
  // @tid: ID of the aborting transaction
  // Return true/false if the logging operation succeeded/failed
  bool LogAbort(uint64_t tid);

  // Write an end log record
  // @tid: ID of the completing transaction
  // Return true/false if the logging operation succeeded/failed
  bool LogEnd(uint64_t tid);

  // Return the durable LSN
  inline LSN GetDurableLSN() { return durable_lsn; }

  // Return the current LSN
  inline LSN GetCurrentLSN() { return current_lsn; }

  // Flush the log; return true/false if succeeded/failed
  bool Flush();
};

}  // namespace yase
