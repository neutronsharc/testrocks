//
// Created by shawn.ouyang on 12/07/2023.
//

#ifndef TESTROCKS_KVS_ROCKSDB_H
#define TESTROCKS_KVS_ROCKSDB_H

#include <mutex>
#include <string>
#include <vector>

#include <rocksdb/options.h>
#include <rocksdb/db.h>
#include <rocksdb/trace_reader_writer.h>
#include <rocksdb/trace_record.h>
#include <rocksdb/wal_write_callback.h>

#include "kvs.h"
#include "util/logger.h"
#include "util/util.h"

// rocksdb config used in the test.
struct RocksConfig {
  std::string dbpath; // db path. It's a dir.
  int num_cf;  // number of column families, >= 1.
};


//////////  code taken from pdfs:: kvs_rocksdb.cpp


#define HUGE_PAGE (2 * 1024 * 1024)

using rocksdb::Status;
using rocksdb::Slice;

class KvsComparator : public rocksdb::Comparator {
 public:
  KvsComparator(kvs_compare_t cmp_func, const char *name)
    : cmp_func_(cmp_func) {
    name_ = std::string(name) + "-cmp";
  }

  virtual const char *Name() const override { return name_.c_str(); }

  virtual int Compare(const Slice &a, const Slice &b) const override {
    return cmp_func_(a.size(), a.data(), b.size(), b.data());
  }
  virtual void FindShortestSeparator(std::string *start,
                                     const Slice &limit) const override {}

  virtual void FindShortSuccessor(std::string *key) const override {}
 private:
  kvs_compare_t cmp_func_;
  std::string name_;
};

class DbWrapper {
 public:
  DbWrapper(const char *db_path);

  ~DbWrapper() {
    KvsComparator *cmp;
    while (!comparators_.empty()) {
      cmp = comparators_.back();
      delete cmp;
      comparators_.pop_back();
    }
    delete kvs_options_.env;
    if (db_) {
      delete db_;
    }
  }

  rocksdb::Options      kvs_options_;
  rocksdb::ReadOptions  roptions_;
  rocksdb::WriteOptions woptions_default_;
  rocksdb::WriteOptions woptions_nosync_;

  // Use "env_options" and "Options.env" to create trace-writer and trace-reader.
  std::unique_ptr<rocksdb::TraceWriter> trace_writer_;

  rocksdb::DB *db_ = nullptr;
  std::string db_path_;
  cf_handle_t default_cfh_;

  pthread_mutex_t sync_mtx_;
  std::unique_ptr<PdfsWalWriteCallback> wal_cb_;

  // This array includes all CFs.
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors_;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  std::vector<KvsComparator *> comparators_;
};


class TxnWrapper {
 public:
  DbWrapper *db_ = nullptr;
  rocksdb::WriteBatch batch_;
};

class
PdfsWalWriteCallback : public rocksdb::WalWriteCallback {
 public:
  std::atomic<bool> wal_write_done_{false};

  pthread_mutex_t *sync_mtx = nullptr;
  pthread_t writer_pid;
  uint64_t num_callbacks;

  PdfsWalWriteCallback(pthread_mutex_t *mtx) {
	sync_mtx = mtx;
	num_callbacks = 0;
	writer_pid = pthread_self();
  }
  PdfsWalWriteCallback() = default;
  PdfsWalWriteCallback(const PdfsWalWriteCallback &other) {
	wal_write_done_.store(other.wal_write_done_.load());
  }

  void OnWalWriteFinish(void) {
	num_callbacks++;
	if (sync_mtx) {
	  if (pthread_self() != writer_pid) {
		LOG_ERROR("in wal write callback: callback={} hope writer={}",
				  num_callbacks, GetThreadName(writer_pid));
	  }
//	  LOG_INFO("in wal write callback: callback={}", num_callbacks);
	  pthread_mutex_unlock(sync_mtx);
	}
  }
};

class PdfsWalFile : public rocksdb::WritableFile {
 public:
  explicit PdfsWalFile(const rocksdb::EnvOptions &options, uint64_t write_buffer_sz);

  ~PdfsWalFile();

  Status Append(const Slice &data);
  Status AppendSegment(const char *ptr, size_t nbytes);
  Status PositionedAppend(const Slice &data, uint64_t offset);
  Status Open(const std::string &fname);
  Status Close();
  Status Sync();
  Status Truncate(uint64_t size) { return Status::OK(); }
  Status Flush() { return Status::OK(); }
  Status Allocate(uint64_t offset, uint64_t len) { return Status::OK(); }
  uint64_t GetFileSize() { return size; }

private:
  Status FlushInternal();
  int do_pwrite(char *data, size_t len, off_t offset);
  int clear_and_falloc(size_t nbytes, off_t offset);

  int fd;
  size_t size;
  uint64_t preallocate;
  uint64_t prealloc_end;
  caddr_t buff;
  size_t buflen;
  bool mmapped;
  bool dirty;
  off_t last_sync;
  std::mutex mtx;
};

class TestWalWriteCallback : public rocksdb::WalWriteCallback {
 public:
  TestWalWriteCallback() {}

  void OnWalWriteFinish(void) override {
  }
};

// class rocksdb::Logger;
/*
class ObsStatsMergeOperator : public rocksdb::MergeOperator
{
public:
  ObsStatsMergeOperator(const char *n) {
    name = std::string(n);
  }
  virtual bool FullMergeV2(const MergeOperationInput &merge_in,
                           MergeOperationOutput *merge_out) const {
    return true;
  }

  virtual bool PartialMerge(const Slice &key,
                            const Slice &left_operand,
                            const Slice &right_operand,
                            std::string *new_value,
                            rocksdb::Logger *logger) {
    return true;
  }
  virtual const char *Name() const {
    return name.c_str();
  }
private:
  std::string name;
};
*/


#endif //TESTROCKS_KVS_ROCKSDB_H
