#include <iostream>
#include <bsd/stdlib.h>
#include <gflags/gflags.h>
#include <unistd.h>
#include <string.h>

#include "util/logger.h"
#include "rocks/kvs_rocksdb.h"
#include "rocks/testrocks.h"
#include "util/util.h"

DEFINE_string(log, "/tmp/", "log filename for this benchmark tool");
DEFINE_string(dir, "/mnt/disk/db2", "create rocksdb in this directory");
DEFINE_string(trace_path, "/tmp/rocks_trace/trace1", "create rocksdb in this directory");
//DEFINE_string(dir, "/mnt/ramfs/testrocks/", "create rocksdb in this directory");

DEFINE_bool(use_dbwal, true, "whether to use rocksdb WAL, default no");
DEFINE_bool(db_write, true, "do db write after wal write");
DEFINE_bool(db_trace, false, "record db trace");
DEFINE_bool(db_replay, false, "replay a db trace, mutual exclusive with db_trace");

DEFINE_string(wal_dir, "", "save wal file in this dir, default in the same dir as db dir");
DEFINE_bool(wal_write, false, "write customer wal file");
DEFINE_bool(wal_fsync, true, "do fsync after writing to customer wal file");
DEFINE_bool(wal_cb, true, "use the new wal-write callback API, default not.\n");

DEFINE_int32(cf, 1, "number of colume families");
DEFINE_int32(workmode, 3, "work mode: 1=pipelined,  2=async busy loop, 3=wal_callback test");
DEFINE_int32(runtime, 30, "runtime in seconds");
DEFINE_int32(blkcache, 300, "blkcache size in MB");
DEFINE_int32(threads, 4, "number of worker threads to run");
DEFINE_int32(syncer, 2, "number of syncer threads to run");
DEFINE_int32(producer, 4, "number of producer threads to run");

DEFINE_int64(num_objs, 50000, "write this many objs to kvstore");

extern void RocksBasicRw(kvs_config_t *kvconfig, kvs_handle_t* kvs, int tid);
extern void DeleteRangeTest(Persistence* persistence);
extern void TryOddsizedStruct();
// do some basic read/write.
static void BasicRw(kvs_config_t* kvconfig, kvs_handle_t* kvs) {
  int tid = 0;
  // run normal rw.
  RocksBasicRw(kvconfig, kvs, tid);
}

static void PipelineTest(TestRocksConfig* test_config) {
  int num_threads = test_config->num_threads;
  std::vector<SyncerThread*> threads;
  threads.reserve(num_threads);
  LOG_INFO("will run {} threads on a kv store", num_threads);

  std::vector<std::thread> ths;
  ths.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(new SyncerThread(i, num_threads, test_config));
  }

  for (auto t : threads) {
    t->Start();
  }
  test_config->ring->AddToken(0, TokenType::kDeqToken);
  test_config->ring->AddToken(0, TokenType::kDbwriteToken);

  LOG_INFO("wait for {} seconds...", test_config->runtime_sec);
  sleep(test_config->runtime_sec);

  for (auto t : threads) {
    t->Stop();
    LOG_INFO("thread stats=\n{}", t->ToString());
  }
  for (auto t : threads) {
    delete t;
  }
  threads.clear();
}

static void AsyncWork(TestRocksConfig* test_config) {
  std::vector<SyncerThread*> syncers;
  syncers.reserve(test_config->num_syncers);
  LOG_INFO("will run {} syncer-threads", test_config->num_syncers);

  std::vector<ProducerThread*> producers;
  producers.reserve(test_config->num_producers);
  LOG_INFO("will run {} producer-threads", test_config->num_producers);

  // 1, launch syncer threads (consumer)
  for (int i = 0; i < test_config->num_syncers; i++) {
    syncers.push_back(new SyncerThread(i, test_config->num_syncers, test_config));
  }
  for (auto t : syncers) {
    t->Start();
  }

  // 2. launch producer threads.
  for (int i = 0; i < test_config->num_producers; i++) {
    producers.push_back(new ProducerThread(i, test_config->num_producers, test_config));
  }
  for (auto t : producers) {
    t->Start();
  }

  LOG_INFO("wait for {} seconds...", test_config->runtime_sec);
  sleep(test_config->runtime_sec);

  for (auto t : producers) {
    t->Stop();
    LOG_INFO("producer thread stats=\n{}", t->ToString());
    delete t;
  }

  for (auto t : syncers) {
    t->Stop();
    LOG_INFO("syncer thread stats=\n{}", t->ToString());
    delete t;
  }
  producers.clear();
  syncers.clear();
}


extern void Practice();
uint32_t revert(uint32_t val) {
  uint32_t rv = 0;
  int i = 0, j = 31;
  while (i < j) {
	int gap = j - i;
	rv |= ((val & (1U << j)) >> gap);
	rv |= ((val & (1U << i)) << gap);
	i++;
	j--;
  }
  return rv;
}

uint32_t revert2(uint32_t val) {
  uint32_t rv = 0;
  while (val) {
	rv *= 10;
	rv += val % 10;
	val /= 10;
  }
  return rv;
}

void Mytest() {
  int len = 22;
  char * buf1 = (char*)malloc(len);
  strcpy(buf1, "1234567890123456789012");
  buf1[len] = 3;

  char * buf2 = (char*)realloc(buf1, len + 1);
  buf2[len] = 5;

  char *str = "\"123\"";  // len = 5
  fprintf(stderr, "str len = %d\n", strlen(str));
  return;
  uint32_t val = 4321234;
  uint32_t rev = revert(val);

  LOG_INFO("val={}, rev1={}, rev2={}", val, rev, Revert(val));

  LOG_INFO("val={} dec-rev={}", val, revert2(val));

  LOG_INFO("val={} str={}", val, Val2Str(val));
  LOG_INFO("rev={} str={}", rev, Val2Str((uint64_t)rev));

  uint64_t v1 = 1234;
  uint64_t v2, v3, v4;
  uint64_t res;

  v2 = -(v1 -3);
  v3 = -(v1 +4);
  v4 = v1 - 5;
  res = v1 + v2 + v3 + v4;

  ShowVal(5678);
  ShowVal(-5678);
  ShowVal(v1);
  ShowVal(-v1);
  ShowVal(v1 -3);
  ShowVal(-(v1 - 3));
  ShowVal(v2);
  ShowVal(v3);
  ShowVal(v4);
  ShowVal(res);

  printf("int res=%ld, uint res=%lu\n", (int64_t)res, res);
}
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // Prepare the logging utility.
  InitializeLogger(FLAGS_log);
  srand(NowInUsec());

  TestRocksConfig test_config;
  test_config.Init();

//  printf("Press Enter to Continue");
//  char ch = std::cin.get();
//  printf("input = %c, will continue\n", ch);
//  sleep(1000);

  // 1. open db
  Persistence persistence;
  kvs_config_t kvs_config = DEFAULT_KVS_CONFIG;
  if (FLAGS_use_dbwal) {
	LOG_INFO("will use rocksdb WAL and sync after each write, wal dir={}", FLAGS_wal_dir);
    kvs_config.disable_dbwal = false;
    kvs_config.sync = true;
	if (!FLAGS_wal_dir.empty()) {
	  kvs_config.wal_dir = FLAGS_wal_dir.c_str();
	}
  } else {
    kvs_config.disable_dbwal = true;
    kvs_config.sync = false;
  }
  kvs_config.db_trace = FLAGS_db_trace;
  kvs_config.db_replay = FLAGS_db_replay;
  kvs_config.db_trace_path = FLAGS_trace_path.c_str();
  kvs_config.db_dir = FLAGS_dir.c_str();
  kvs_config.wal_callback = FLAGS_wal_cb;
  ASSERT(!(kvs_config.db_trace && kvs_config.db_replay),
		 "trace and replay cannot be true at the same time");

  int retv = kvs_db_open_byname(&persistence, &kvs_config, FLAGS_dir.c_str());
  if (retv) {
    LOG_ERROR("failed to open db at path={}", FLAGS_dir);
    goto out_close_db;
  }
  test_config.kvs = &persistence.kvs_handle;

  // 2. Run test
  if (kvs_config.db_replay) {
	LOG_INFO("will replay db trace");
	kvs_replay(&kvs_config, &persistence.kvs_handle);
	LOG_INFO("will scan the replayed db");
	kvs_scan(&persistence.kvs_handle, 0);
  } else {
	switch (FLAGS_workmode) {
	  case 1:
		break;
	  case 2:
		break;
	  case kWalCallback:  // wal-write callback test.
		TestWalWriteCallabck(&test_config, &kvs_config, &persistence.kvs_handle);
//		BasicRw(&kvs_config, &persistence.kvs_handle);
		break;
	  default:
		break;
	}
  }
//	BasicRw(&kvs_config, &persistence.kvs_handle);
//  DeleteRangeTest(&persistence);
//  PipelineTest(&test_config);
//  AsyncWork(&test_config);

  // 3. Close db.
out_close_db:
  LOG_INFO("will close db after test is done...");
  kvs_close_db(&persistence.kvs_handle);
  kvs_destroy_db(&persistence.kvs_handle);
  return retv;
}

