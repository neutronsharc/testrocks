//
// Created by shawn.ouyang on 12/12/2023.
//

#ifndef TESTROCKS_TESTROCKS_H
#define TESTROCKS_TESTROCKS_H

#include <condition_variable>
#include <mutex>
#include <thread>
#include <gflags/gflags.h>

#include "rocks/kvs.h"

#define MAX_KEY_LEN (32)
#define MAX_VALUE_LEN (50000)

struct KvObj;
class Token;
class TokenRing;
class SyncerThread;
class ProducerThread;

enum TestWorkMode {
  kPipeline = 1,
  kAsyncBusy = 2,
  kWalCallback = 3,
};

template<typename T> class MpscQueue;
//template class MpscQueue<KvObj*>;

std::string WorkModeStr(TestWorkMode mode);

// A lock-free queue wrapper to the multi-producer single-consumer queue.
class LfQueue {
 public:
  LfQueue();
  LfQueue(bool blocking, const std::string& name);
  ~LfQueue();

  // Add an obj to the queue.
  bool Enqueue(KvObj* obj);

  // Bulk dequeue. Return number of objs fetched from the queue.
  size_t Dequeue(KvObj* objs[], int num);

  size_t DequeueWait(KvObj* objs[], int num, uint64_t wait_us);

 private:
  void* queue_;  //a generic pointer to the Mpsc queue.
};

class TestRocksConfig {
 public:
  TestRocksConfig() = default;
  ~TestRocksConfig() = default;
  bool Init();
  std::string ToString() const;

  int32_t runtime_sec;  // how long the test should run.
  int32_t num_threads;
  int32_t num_syncers;     // syncer threads.
  int32_t num_producers;   // producer threads, i.e. worker threads in pdfs.
  int32_t key_len;
  int32_t value_len;
  int64_t num_objs;  // write this many objs

  TestWorkMode work_mode;   // 1: pipelined semi-concurrency,  2: async busy loop
  bool do_walwrite; // write customer wal file.
  bool do_fsync;  // call fsync after wal-file write
  bool do_dbwrite;  // write db.

  bool wal_cb;  // do a callback at wal-write completion.

  std::string customer_wal_dir;  // customer wal file dir.

  kvs_handle_t* kvs;  // instance of a kvstore.
  std::unique_ptr<TokenRing> ring;

//  class QueueImp;
//  QueueImp* queue;
  std::unique_ptr<LfQueue> queue;
//  std::unique_ptr<MpscQueue<KvObj*>> queue;
//  void* queue;
};

// a key-value object.
struct KvObj {
  int32_t key_len;
  int32_t value_len;

  bool is_processed;  // if this object has been processed.
  std::mutex mtx;
  std::condition_variable cv;

  // Pre-allocated 32 bytes for key. The beginning part is prefix, format t.b.d.
  union {
    uint8_t key_buf[MAX_KEY_LEN];
    struct {
      uint32_t tid;  // thread id
      uint64_t sn;  //  global sn.
    } prefix;
  } key;

  // pre-allocated buf for value.
  uint8_t value[MAX_VALUE_LEN];

  KvObj() = default;

  KvObj(int32_t klen, int32_t vlen);

  bool OnComplete();
  bool WaitForCompletion();

  // change the key based on worker thread id and msg sn.
  void UpdateKey(uint32_t thread_id, uint64_t cnt);
  std::string ToString() const;
};

enum TokenType : uint8_t {
  kDeqToken = 0,
  kDbwriteToken = 1,
};

enum WaitTokenResult : uint8_t {
  kSuccess = 0,  // Grabbed a token successfully.
  kTimeout = 1,  // Timed out, no token available.
};

struct TokenStats {
  uint64_t num_produced;  // how many times the token is produced
  uint64_t num_consumed;  // how many times it's consumed.

  TokenStats() {
    num_produced = num_consumed = 0;
  }
};

class Token final {
 public:
  Token() {
    deq_token_ = 0;
    dbwrite_token_ = 0;
  }

  // Assign a token to this thread, so it can do some operations: deq or write-db.
  void AddToken(TokenType ttype);

  WaitTokenResult WaitToken(TokenType ttype);

  std::string ToString() const;

private:
  // Must own a token to pull from the shared dirty queue.
  std::mutex deq_lock_;
  std::condition_variable deq_cv_;
  uint64_t deq_token_;  // A worker must own a token to access the queue.
  TokenStats deq_token_stats_;

  // Must own a db-token before a thread is allowed to write to db.
  std::mutex dbwrite_lock_;
  std::condition_variable dbwrite_cv_;
  uint32_t dbwrite_token_;  // A worker must own a token to access the queue.
  TokenStats dbwrite_token_stats_;
};

// Each worker-thread has one element in this token ring.
class TokenRing {
 public:
  explicit TokenRing(int elems);

  ~TokenRing() = default;

  void AddTokenToNext(int32_t my_tid, TokenType ttype);

  void AddToken(int32_t tid, TokenType ttype);

  // Wait for a token available, and grab the token.
  WaitTokenResult WaitToken(int32_t tid, TokenType ttype);

  Token* GetTokenObject(int32_t tid);

private:
  int32_t num_elems;  // how many valid elements in this token ring.
  // Token tokens[MAX_WORKER_THREADS];
  std::unique_ptr<Token[]> tokens;
};


struct ThreadStats {
  uint64_t num_enq;  // number of enqueues.
  uint64_t num_deq;
  uint64_t num_dbwrite;
  uint64_t num_walwrite;

  ThreadStats() {
    Reset();
  }
  void Reset() {
    num_enq = num_deq = num_dbwrite = 0;
    num_walwrite = 0;
  }

  std::string ToString() const;
};

class CustomerWalFile final {
 public:
  explicit CustomerWalFile(std::string& name);
  ~CustomerWalFile();

  int Write(uint8_t *data, int size, bool do_fsync);

 private:
  int fd_;  // file handle.
  size_t buf_size_;
  size_t curr_offset_;
  void* aligned_buf_;
  std::string filename_;
};

/**
 * Context for a worker thread.
 */
class ThreadCtx {
 public:
  ThreadCtx() = default;
  ~ThreadCtx() = default;

  void Init(int32_t tid, int32_t threads, TestRocksConfig* tconfig);

  std::string ToString() const;

  int GetDirtyData();
  int WriteWalFile();
  int WriteDb();

  bool Enqueue(KvObj* obj);

  size_t Dequeue(KvObj* objs[], int num);

  friend class SyncerThread;
  friend class ProducerThread;
 private:
  int32_t thread_id_;   // id of this thread.
  int32_t thread_cnt_;  // total number of threads

  TestRocksConfig* test_config_;

  kvs_handle_t *kvs_;  // point to an instance of kv store

  uint64_t kv_write_cnt_;  // how many writes to db.
  uint64_t wal_write_cnt_;  // number of writes to wal file.

  std::unique_ptr<KvObj> kv_obj_;  // an kv object to write to kvstore.
  std::unique_ptr<CustomerWalFile> walfile_;
};

/**
 * A syncer thread pulls data from a shared queue, write to wal file, then write to db.
 */
class SyncerThread final {
 public:
  SyncerThread(int32_t tid, int32_t tcnt, TestRocksConfig* test_config);

  ~SyncerThread() = default;

  int Start();
  int Stop();

  inline int32_t ThreadId() const { return ctx_.thread_id_; }

  std::string ToString() const;

  void PrintStats(double seconds);

 private:
  int AsyncBusyLoop();
  int PipelinedSemiConcurrent();
  int Work();  // main loop of a worker thread.

  bool is_running_;
  bool should_exit_;  // a flag to tell the thread to exit.

  std::thread thread_;

  TokenRing* tring_;

  TestRocksConfig* test_config_;

  ThreadCtx ctx_;
  ThreadStats stats_;
};

/**
 * A producer thread generates some data, add to a shared queue, and then wait to be waken up
 * by a syncer-thread.
 * The syncer-thread pulls data from the shared-queue, process it, and wake up the producer.
 */
class ProducerThread final {
public:
  ProducerThread(int32_t tid, int32_t tcnt, TestRocksConfig* tconfig);

  ~ProducerThread() = default;

  int Start();
  int Stop();

  inline int32_t ThreadId() const { return ctx_.thread_id_; }

  std::string ToString() const;

  void PrintStats(double seconds);

private:
  int Work();  // main loop of a worker thread.

  bool is_running_;
  bool should_exit_;  // a flag to tell the thread to exit.

  std::thread thread_;

  TestRocksConfig* test_config_;

  ThreadCtx ctx_;
  ThreadStats stats_;
};

void TestWalWriteCallabck(TestRocksConfig* test_config, kvs_config_t* kvconfig, kvs_handle_t* kvs);

#endif //TESTROCKS_TESTROCKS_H
