//
// Created by shawn.ouyang on 12/12/2023.
//

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <bsd/stdlib.h>
#include <sstream>
#include <queue>

#include "lockless_queue/lockless_queue.h"
#include "rocks/kvs.h"
#include "rocks/testrocks.h"
#include "util/logger.h"
#include "util/util.h"
#include "rocks/kvs_rocksdb.h"

DECLARE_string(log);
DECLARE_string(dir);
DECLARE_string(wal_dir);
DECLARE_string(trace_path);

DECLARE_bool(wal_write);
DECLARE_bool(wal_fsync);
DECLARE_bool(db_write);
DECLARE_bool(use_dbwal);
DECLARE_bool(wal_cb);
DECLARE_bool(db_trace);

DECLARE_int32(cf);
DECLARE_int32(workmode);
DECLARE_int32(runtime);
DECLARE_int32(blkcache);
DECLARE_int32(threads);
DECLARE_int32(syncer);
DECLARE_int32(producer);
DECLARE_int64(num_objs);

void RocksBasicRw(kvs_handle_t* kvs, int tid);

bool TestRocksConfig::Init() {
  num_threads = FLAGS_threads;
  num_syncers = FLAGS_syncer;
  num_producers = FLAGS_producer;
  key_len = 24;
  value_len = 3456;
  runtime_sec = FLAGS_runtime;
  customer_wal_dir = FLAGS_wal_dir;
  do_fsync = FLAGS_wal_fsync;
  do_walwrite = FLAGS_wal_write;
  do_dbwrite = FLAGS_db_write;
  work_mode = static_cast<TestWorkMode>(FLAGS_workmode);
  wal_cb = FLAGS_wal_cb;
  num_objs = FLAGS_num_objs;
  // queue = new MpscQueue<KvObj*>(false, "dirty_queue");
  // allocate a non-blocking queue.
  // queue = std::make_unique<MpscQueue<KvObj*>>();
  queue = std::make_unique<LfQueue>();
//  queue = new QueueImp();
  ring = std::make_unique<TokenRing>(FLAGS_threads);

  LOG_INFO("test config={}", ToString());
  return true;
}

std::string KvObj::ToString() const {
  std::stringstream ss;
  ss << "[key_len=" << key_len << "," << key.prefix.tid << "_" << key.prefix.sn
     << ",value_len=" << value_len << "]";
  return ss.str();
}

KvObj::KvObj(int32_t klen, int32_t vlen) {
  key_len = klen;
  value_len = vlen;
  ASSERT(klen <= MAX_KEY_LEN, "invalid key len={}", klen);
  ASSERT(vlen <= MAX_VALUE_LEN, "invalid value len={}", vlen);
//  memset(key.key_buf, 0, 16);
//  memset(value, 0, 16);
}

// change the key based on worker thread id and msg sn.
void KvObj::UpdateKey(uint32_t thread_id, uint64_t cnt) {
  key.prefix.tid = thread_id;
  key.prefix.sn = cnt;
}

bool KvObj::OnComplete() {
  std::unique_lock<std::mutex> lock(mtx);
  is_processed = true;
  cv.notify_one();
  return true;
}

bool KvObj::WaitForCompletion() {
  std::unique_lock<std::mutex> lock(mtx);
  // wait until the condition is true, or timed out.
  cv.wait_for(lock, std::chrono::milliseconds(30 * 1000), [this]() { return is_processed; });
  return true;
}

void ThreadCtx::Init(int32_t tid, int32_t threads, TestRocksConfig* tconfig) {
  thread_id_ = tid;
  thread_cnt_ = threads;
  kvs_ = tconfig->kvs;
  kv_write_cnt_ = 0;
  wal_write_cnt_ = 0;
  test_config_ = tconfig;

  kv_obj_ = std::make_unique<KvObj>(tconfig->key_len, tconfig->value_len);
  kv_obj_->is_processed = true;  // the object is ready to be-reused.
  kv_obj_->key.prefix.tid = tid;
  kv_obj_->key.prefix.sn = tid;
  arc4random_buf(kv_obj_->value, tconfig->value_len);

  std::string walfile_name = tconfig->customer_wal_dir + "/testwal_" + std::to_string(thread_id_);
  walfile_ = std::make_unique<CustomerWalFile>(walfile_name);
};

std::string ThreadCtx::ToString() const {
  return "";
}

int ThreadCtx::GetDirtyData() {
  kv_obj_->key.prefix.tid = thread_id_;
  kv_obj_->key.prefix.sn = (uint64_t)(std::rand() & 0x0fffffff);
  memcpy(kv_obj_->value, &kv_obj_->key, sizeof(kv_obj_->key.prefix));
  return 0;
}

int ThreadCtx::WriteWalFile() {
  wal_write_cnt_++;
  return walfile_->Write((uint8_t *)(kv_obj_.get()), test_config_->value_len + test_config_->key_len, test_config_->do_fsync);
}

int ThreadCtx::WriteDb() {
  kv_write_cnt_++;

  int retv = 0;
  retv = kvs_put(kvs_, kv_obj_->key.key_buf, test_config_->key_len, kv_obj_->value, test_config_->value_len);
  ASSERT(retv == 0,  "failed to write db at key={}", kv_write_cnt_);
  return retv;
}

bool ThreadCtx::Enqueue(KvObj* obj) {
  ASSERT(!obj->is_processed, "expect a dirty obj data");
  return test_config_->queue->Enqueue(obj);
}

size_t ThreadCtx::Dequeue(KvObj *objs[], int num) {
  return test_config_->queue->Dequeue(objs, num);
//  return test_config_->queue->DequeueWait(objs, num, 500 * 1000);
}

int SyncerThread::Start() {
  should_exit_ = false;
  LOG_INFO("try to start syncer thread {}", ThreadId());
  thread_ = std::thread(&SyncerThread::Work, this);
  while (!is_running_) {  // wait for the thread to start.
    usleep(5000);
  }
  return 0;
}

int SyncerThread::Stop() {
  // set the flag, and wait for thread to exit.
  should_exit_ = true;
  LOG_INFO("try to stop thread {}", ThreadId());
  if (thread_.joinable()) {
    thread_.join();
  }
  return 0;
}

int SyncerThread::AsyncBusyLoop() {
  int max_objs = 16;
  KvObj *objs[max_objs];

  while (!should_exit_) {
    // 1. grab dirty data from shared queue.
    size_t retv = ctx_.Dequeue(objs, 1);
    // test_config_->queue->Dequeue(objs, 1); // dequeue 1 obj at a  time.
    if (retv == 0) {
      continue;
    }

    // 2. commit the dirty data to wal-file + db.
    stats_.num_deq += retv;

    for (size_t i = 0; i < retv; i++) {
      auto obj = objs[i];
      std::unique_lock<std::mutex> lock(obj->mtx);
      obj->is_processed = true;
      obj->cv.notify_one();
    }
  }
  return 0;
}

int SyncerThread::PipelinedSemiConcurrent() {
  WaitTokenResult wr;

  while (!should_exit_) {
    // 1. grab a deq token, and deq some data.
    wr = tring_->WaitToken(ThreadId(), TokenType::kDeqToken);
    if (wr == kSuccess) {
      stats_.num_deq++;
      // (todo) Grab some dirty data.
      ctx_.GetDirtyData();

      // Allow the next thread in the ring to grab dirty data.
      tring_->AddTokenToNext(ThreadId(), TokenType::kDeqToken);

      // Write to customer wal file.
      if (test_config_->do_walwrite) {
        stats_.num_walwrite++;
        ctx_.WriteWalFile();
      }
    } else {
      continue;
    }

    // 2. grab a db-write-token, then write to db.
    if (!test_config_->do_dbwrite) {
      continue;
    }
    wr = tring_->WaitToken(ThreadId(), TokenType::kDbwriteToken);
    if (wr == kSuccess) {
      // Write the dirty data to db (have disabled db's internal WAL).
      ctx_.WriteDb();

      tring_->AddTokenToNext(ThreadId(), TokenType::kDbwriteToken);
      stats_.num_dbwrite++;
      if (stats_.num_dbwrite % 100000 == 0) {
        LOG_INFO("thread {} stats={}", ThreadId(), stats_.ToString());
      }
      // (todo) Resume the originating threads.
    } else {
      continue;
    }
  }

  return 0;
}

int SyncerThread::Work() {
  std::string tname = "syncer_" + std::to_string(ThreadId());
  SetThreadName(pthread_self(), tname);

  LOG_INFO("{} started mode={}", tname, WorkModeStr(test_config_->work_mode));
  is_running_ = true;
  uint64_t t1 = NowInUsec();

  if (test_config_->work_mode == TestWorkMode::kPipeline) {
    PipelinedSemiConcurrent();
  } else {
    AsyncBusyLoop();
  }
  double tsec = (NowInUsec() - t1) / 1000000.0;
  PrintStats(tsec);
  is_running_ = false;
  return 0;
}

void SyncerThread::PrintStats(double seconds) {
  LOG_INFO("tid={} finished in {} seconds, wal-write IOPS={}, dbwrite IOP={} , stats={}",
           ThreadId(), seconds,
           stats_.num_walwrite / seconds,
           stats_.num_dbwrite / seconds,
           stats_.ToString());
}

std::string SyncerThread::ToString() const {
  std::stringstream ss;
  auto token = tring_->GetTokenObject(ThreadId());
  ss << "{tid=" << ThreadId()
     << ", ctx=" <<  ctx_.ToString()
     << ", stats=" << stats_.ToString()
     << ", tokens=" << token->ToString()
     << "}";
  return ss.str();
}

SyncerThread::SyncerThread(int32_t tid, int32_t tcnt, TestRocksConfig *test_config) {
  tring_ = test_config->ring.get();
  should_exit_ = false;
  is_running_ = false;
  test_config_ = test_config;
  ctx_.Init(tid, tcnt, test_config);
}

std::string ThreadStats::ToString() const {
  std::stringstream ss;
  ss << "[enq=" << num_enq
     << ", deq=" << num_deq
     << ", dbwrite=" << num_dbwrite
     << "]";
  return ss.str();
}

void TokenRing::AddTokenToNext(int my_tid, TokenType ttype) {
  ASSERT(my_tid < num_elems, "tid={} >= num elems={}", my_tid, num_elems);
  int next_tid = (my_tid + 1) % num_elems;
  tokens[next_tid].AddToken(ttype);
}

void TokenRing::AddToken(int tid, TokenType ttype) {
  ASSERT(tid < num_elems, "tid={} >= num elems={}", tid, num_elems);
  tokens[tid].AddToken(ttype);
}

WaitTokenResult TokenRing::WaitToken(int32_t tid, TokenType ttype) {
  ASSERT(tid < num_elems, "tid={} >= num elems={}", tid, num_elems);
  return tokens[tid].WaitToken(ttype);
}

Token *TokenRing::GetTokenObject(int32_t tid) {
  ASSERT(tid < num_elems, "tid={} >= num elems={}", tid, num_elems);
  return &tokens[tid];
}

TokenRing::TokenRing(int elems) {
  num_elems = elems;
  tokens = std::make_unique<Token[]>(num_elems);
  LOG_INFO("created {} tokens", num_elems);
  for (int i = 0; i < elems; i++) {
    LOG_INFO("token_{} = {}", i, tokens[i].ToString());
  }
}

void Token::AddToken(TokenType ttype) {
  switch (ttype) {
    case TokenType::kDeqToken: {
      std::lock_guard<std::mutex> lock(deq_lock_);
      deq_token_++;
      deq_token_stats_.num_produced++;
      deq_cv_.notify_one();
      return;
    }
    case TokenType::kDbwriteToken: {
      std::lock_guard<std::mutex> lock(dbwrite_lock_);
      dbwrite_token_++;
      dbwrite_token_stats_.num_produced++;
      dbwrite_cv_.notify_one();
      return;
    }
    default:
      ABORT("unknown token type={}", ttype);
  }
}

WaitTokenResult Token::WaitToken(TokenType ttype) {
  int wait_ms = 500;  // wait for 500 milli-second.
  switch (ttype) {
    case TokenType::kDeqToken: {
      std::unique_lock<std::mutex> lock(deq_lock_);
      // wait until the condition is true, or timed out.
      deq_cv_.wait_for(lock, std::chrono::milliseconds(wait_ms), [this]() { return deq_token_ > 0; });
      if (deq_token_ > 0) {
        // consume a token.
        deq_token_--;
        deq_token_stats_.num_consumed++;
        return WaitTokenResult::kSuccess;
      } else {
        return WaitTokenResult::kTimeout;
      }
    }
    case TokenType::kDbwriteToken: {
      // wait until there is a valid token, or timed out.
      std::unique_lock<std::mutex> lock(dbwrite_lock_);
      dbwrite_cv_.wait_for(lock, std::chrono::milliseconds(wait_ms), [this]() { return dbwrite_token_ > 0; });
      if (dbwrite_token_ > 0) {
        // consume a token.
        dbwrite_token_--;
        dbwrite_token_stats_.num_consumed++;
        return WaitTokenResult::kSuccess;
      } else {
        return WaitTokenResult::kTimeout;
      }
    }
    default:
      ABORT("unknown token type={}", ttype);
  }
}

std::string Token::ToString() const {
  std::stringstream ss;
  ss <<"deq token=[produced=" << deq_token_stats_.num_produced
     << ",consumed=" << deq_token_stats_.num_consumed
     << "], dbwrite token=[produced=" << dbwrite_token_stats_.num_produced
     << ",consumed=" << dbwrite_token_stats_.num_consumed
     << "]";
  return ss.str();

}

void DeleteRangeTest(Persistence* persistence) {
  pdfs_key_type_t kop = PDK_EVENT;
  cf_handle_t* cfh = &persistence->cf_handles[kop];
  pdfs_event_key_t key;
  pdfs_event_value_t value;
  int retv;

  // insert 10 keys.
  key.pek_version = 1;
  key.pek_shareid = 2;
  for (int i = 0; i < 10; i++) {
	key.pek_seqnum = i;
	value.seqnum = i;
	retv = kvs_put_cf(&persistence->kvs_handle, cfh, &key, sizeof(key), &value, sizeof(value));
	ASSERT(retv == 0, "failed to put cf: {}", i);
  }

  kvs_buffer_t buf;
  kvs_init_buffer(&buf);
  // read back the 10 objs.
  for (int i = 0; i < 10; i++) {
	key.pek_seqnum = i;
	retv = kvs_get_cf(&persistence->kvs_handle, cfh, &key, sizeof(key), &buf, nullptr);
	ASSERT(retv == 0, "failed to read key {}", i);
	ASSERT(kvs_get_buffer_size(&buf) == sizeof(value), "read data size wrong: {} vs {}",
		   kvs_get_buffer_size(&buf), sizeof(value));
	pdfs_event_value_t* rv = (pdfs_event_value_t*)(kvs_get_buffer_data(&buf));
	ASSERT(rv->seqnum == i, "read data corrupt: {} vs {}", rv->seqnum, i);
  }

  // delete a few objs.
  uint64_t k1 = 5, k2 = 55;
  pdfs_event_key_t kstart, kend;
  kstart.pek_version = kend.pek_version = 1;
  kstart.pek_shareid = kend.pek_shareid = 2;
  kstart.pek_seqnum = k1;
  kend.pek_seqnum = k2;

  LOG_INFO("delete range=[{}, {})", k1, k2);
  retv = kvs_deleterange_cf(&persistence->kvs_handle, cfh, &kstart, sizeof(kstart), &kend, sizeof(kend));
  LOG_INFO("delete range return={}", retv);

  for (uint64_t i = 0; i < 10; i++) {
	key.pek_seqnum = i;
	retv = kvs_get_cf(&persistence->kvs_handle, cfh, &key, sizeof(key), &buf, nullptr);
	if (i >= k1 && i < k2) {
	  ASSERT(retv == ENOENT, "key={} should not exist, retv={}", key.pek_seqnum, retv);
	} else {
	  ASSERT(retv == 0, "key={} should exist, retv={}", key.pek_seqnum, retv);
	  pdfs_event_value_t* rv = (pdfs_event_value_t*)(kvs_get_buffer_data(&buf));
	  ASSERT(rv->seqnum == i, "read data corrupt: {} vs {}", rv->seqnum, i);	}
  }

  // release pinnable slices.
  kvs_destroy_buffer(&buf);
}

void ScanDB(kvs_config_t *kvconfig, kvs_handle_t* kvs) {

}

void RocksBasicRw(kvs_config_t *kvconfig, kvs_handle_t* kvs, int tid) {
  int key_len = 20;
  int value_len = 3456;
  uint8_t key1[key_len];
  uint8_t value1[value_len];

  arc4random_buf(key1, key_len);
  arc4random_buf(value1, value_len);

  int32_t num_objs = 200000;
  SetThreadName(pthread_self(), "tid_" + std::to_string(tid));

  LOG_INFO("start to write {} objs, trace={}", num_objs, kvconfig->db_trace);
  if (kvconfig->db_trace) {
	LOG_INFO("please copy the current db dir={} before tracing, hit ENTER when done copy", kvconfig->db_dir);
	fgetc(stdin);
	LOG_INFO("will start trace writer={}", kvconfig->db_trace_path);
	if (kvs_start_trace(kvconfig, kvs)) {
	  return;
	}
  }
  uint64_t t1 = NowInUsec();
  // Use the first 8bytes of key as a sn,
  // and copy the sn into beginning of value buf.
  uint32_t *pk = reinterpret_cast<uint32_t*>(key1);
  for (int i = 0; i < num_objs; i++) {
    // Assign a pattern in the first 8 bytes of key/value.
	if (i >= 0) {
	  *pk = i + tid * num_objs;
	} else {
	  uint32_t tsc = (uint32_t)rdtsc();
	  *pk = (tsc >> 16) | (tsc << 16);
	}
    memcpy(value1, key1, key_len);
//    ASSERT(kvs_put(kvs, key1, key_len, value1, value_len) == 0, "failed to write db at key={}", i);
	ASSERT(kvs_write_with_cb(kvs, key1, key_len, value1, value_len) == 0, "failed to write db at key={}", i);
    if ((i + 1) % 500000 == 0) {
      LOG_INFO("has written {} objs, obj size={}", i + 1, value_len);
    }
  }
  uint64_t t2 = NowInUsec();

  double sec = (t2 - t1) / 1000000.0;
  LOG_INFO("put objs={} in {} seconds, iops={}", num_objs, sec, num_objs / sec);

  // Read out the 1st kv pair to cross check.
  std::string read_result;
  *pk = 0 + tid * num_objs;
  memcpy(value1, key1, key_len);
  ASSERT(kvs_get(kvs, key1, key_len, &read_result) == 0, "failed to read db");

  LOG_INFO("read data len={}", read_result.size());
  if (memcmp(value1, read_result.c_str(), value_len) != 0) {
    LOG_ERROR("read data mismatch");
  }
  if (kvconfig->db_trace) {
	LOG_INFO("will stop db trace");
	kvs_end_trace(kvs);
  }
}

CustomerWalFile::CustomerWalFile(std::string &name) {
  filename_ = name;
  fd_ = open(filename_.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0644);
  ASSERT(fd_ > 0, "failed to open file={}, error={}", filename_, strerror(errno));

  buf_size_ = 1024 * 1024 * 10;
  curr_offset_ = 0;
  int rv = posix_memalign((void **)&aligned_buf_, 4096, buf_size_);
  ASSERT(rv == 0, "failed to allocate aligned buf size={}", buf_size_);
  memset(aligned_buf_, 0, buf_size_);
  LOG_INFO("have opened test wal file={}", filename_);
}

CustomerWalFile::~CustomerWalFile() {
  if (fd_ > 0) {
    close(fd_);
    fd_ = -1;
  }
  if (aligned_buf_) {
    free(aligned_buf_);
    aligned_buf_ = nullptr;
  }
}

int CustomerWalFile::Write(uint8_t *data, int size, bool do_fsync) {
  ssize_t aligned_size = (size + 4095) & (~4095);
  if (curr_offset_ + aligned_size >= buf_size_) {
    curr_offset_ = 0;
  }
  uint8_t* wbuf = (uint8_t*)aligned_buf_ + curr_offset_;
  memcpy(wbuf, data, size);

  while (true) {
    ssize_t bytes = pwrite(fd_, wbuf, aligned_size, curr_offset_);
    if (bytes < 0) {
      int err = errno;
      switch (err) {
        case EINTR:
          continue;
        default:
          LOG_ERROR("wal file={} write failed at offset={} size={}, error={}",
                    filename_, curr_offset_, aligned_size, strerror(err));
          return err;
      }
    }
    if ((size_t)bytes == aligned_size) {
      curr_offset_ += aligned_size;
      if (do_fsync) {
        fsync(fd_);
      }
      return 0;
    } else {
      ABORT("wal file={} write ret={} at offset={} size={}",
            filename_, bytes,  curr_offset_, aligned_size);
    }
  }
}

std::string WorkModeStr(TestWorkMode mode) {
  switch (mode) {
    case TestWorkMode::kAsyncBusy:
      return "async_busy";
    case TestWorkMode::kPipeline:
      return "pipeline";
    default:
      return "unknown";
  }
}

std::string TestRocksConfig::ToString() const {
  std::stringstream ss;
  ss << "\n threads = " << num_threads
     << "\n syncer threads = " << num_syncers
     << "\n producer threads = " << num_producers
     << "\n runtime = " << runtime_sec
     << "\n work mode = " << WorkModeStr(work_mode)
     << "\n wal file dir = " << customer_wal_dir
     << "\n write wal file = " << do_walwrite
     << "\n wal file fsync = " << do_fsync
     << "\n write db = " << do_dbwrite
     << "\n value len = " << value_len
	 << "\n wal_callback = " << wal_cb
	 << "\n num_objs = " << num_objs
     << "\n";
  return ss.str();
}

ProducerThread::ProducerThread(int32_t tid, int32_t tcnt, TestRocksConfig *tconfig) {
  should_exit_ = false;
  is_running_ = false;
  test_config_ = tconfig;
  ctx_.Init(tid, tcnt, tconfig);
}

int ProducerThread::Start() {
  should_exit_ = false;
  LOG_INFO("try to start producer thread {}", ThreadId());
  thread_ = std::thread(&ProducerThread::Work, this);
  while (!is_running_) {  // wait for the thread to start.
    usleep(5000);
  }
  return 0;
}

int ProducerThread::Stop() {
  // set the flag, and wait for thread to exit.
  should_exit_ = true;
  LOG_INFO("try to stop producer thread {}", ThreadId());
  if (thread_.joinable()) {
    thread_.join();
  }
  return 0;
}

std::string ProducerThread::ToString() const {
  return "";
}

void ProducerThread::PrintStats(double seconds) {
  LOG_INFO("producer tid={} finished in {} seconds, enq IOPS={}, stats={}",
           ThreadId(), seconds,
           stats_.num_enq / seconds,
           stats_.ToString());
}

int ProducerThread::Work() {
  std::string tname = "producer_" + std::to_string(ThreadId());
  SetThreadName(pthread_self(), tname);

  LOG_INFO("{} started", tname);
  is_running_ = true;
  ctx_.kv_obj_->key.prefix.sn = ThreadId();
  ctx_.kv_obj_->is_processed = true;
  uint64_t t1 = NowInUsec();
  while (!should_exit_) {
    // Add a dirty data to the queue.
    auto obj = ctx_.kv_obj_.get();
    if (obj->is_processed) {
      obj->key.prefix.sn++;
      obj->is_processed = false;
      ctx_.Enqueue(obj);
      stats_.num_enq++;
    }

    // wait for the processing completion.
    obj->WaitForCompletion();
    if (!ctx_.kv_obj_->is_processed) {
      LOG_ERROR("obj processing timed out: {}", ctx_.kv_obj_->ToString());
    }
  }

  LOG_INFO("{} finished...", tname);
  double tsec = (NowInUsec() - t1) / 1000000.0;
  PrintStats(tsec);
  is_running_ = false;
  return 0;
}

bool LfQueue::Enqueue(KvObj *obj) {
  static_cast<MpscQueue<KvObj*>*>(queue_)->Enqueue(obj);
  return true;
}

size_t LfQueue::Dequeue(KvObj *objs[], int num) {
  return static_cast<MpscQueue<KvObj*>*>(queue_)->Dequeue(objs, num);
}

size_t LfQueue::DequeueWait(KvObj **objs, int num, uint64_t wait_us) {
  return static_cast<MpscQueue<KvObj*>*>(queue_)->Dequeue(objs, num, wait_us);
}

LfQueue::LfQueue(bool blocking, const std::string& name) {
  LOG_INFO("create mpsc queue is_blocking={}", blocking);
  queue_ = static_cast<void*>(new MpscQueue<KvObj*>(blocking, name));
}

LfQueue::LfQueue() {
  bool blocking = false;
  LOG_INFO("create mpsc queue is_blocking={}", blocking);
  queue_ = static_cast<void*>(new MpscQueue<KvObj*>(blocking, "default_queue"));
}

LfQueue::~LfQueue() {
  delete static_cast<MpscQueue<KvObj*>*>(queue_);
}
