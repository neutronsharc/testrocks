//
// Created by shawn.ouyang on 5/8/2024.
//

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <bsd/stdlib.h>
#include <sstream>
#include <thread>
#include <queue>

#include "lockless_queue/lockless_queue.h"
#include "rocks/kvs.h"
#include "rocks/testrocks.h"
#include "util/logger.h"
#include "util/util.h"
#include "rocks/kvs_rocksdb.h"
#include "rocks/wal_cb_test.h"


struct WalWriteCtx {
  WalWriteCtx(int ksize, int vsize, int freeobjs) {
	key_size = ksize;
	value_size = vsize;
	free_objects = freeobjs;
	// dirty-queue is blocking queue.
	dirty_queue = std::make_unique<LfQueue>(true, "dirty_queue");
	free_obj_queue = std::make_unique<LfQueue>(false, "free_obj_queue");
//	LOG_INFO("prepare {} objs key={} value={}", free_objects, ksize, vsize);
//	for (int i = 0; i < free_objects; ++i) {
//	  auto kvobj = new KvObj(ksize, vsize);
//	  free_obj_queue->Enqueue(kvobj);
//	}
  }

  ~WalWriteCtx() {
	KvObj *kvobj;
	while (free_obj_queue->Dequeue(&kvobj, 1) > 0) {
	  delete kvobj;
	}
  }

  // syner threads fetch objs from this queue
  std::unique_ptr<LfQueue> dirty_queue;
//  std::queue<KvObj*> dirty_queue;

  // when a db-write finishes, the obj is returned to this queue for re-use.
  std::unique_ptr<LfQueue> free_obj_queue;

  int key_size;
  int value_size;
  int free_objects;

  TestRocksConfig *test_config;
  kvs_config_t* kvconfig;
  kvs_handle_t* kvs;
};

void WalWriteProducer(WalWriteCtx* ctx, int idx) {
  std::string tname = "wb_prod_";
  tname.append(std::to_string(idx));
  SetThreadName(pthread_self(), tname);

  auto test_config = ctx->test_config;
  KvObj *obj;
  uint64_t cnt = 0;

  LOG_INFO("producer started, will produce {} objs", test_config->num_objs);

  while (cnt < test_config->num_objs) {
	if (ctx->free_obj_queue->Dequeue(&obj, 1) > 0) {
	  cnt++;
	  memcpy(obj->key.key_buf, &cnt, sizeof(cnt));
	  memcpy(obj->value, &cnt, sizeof(cnt));
	  ctx->dirty_queue->Enqueue(obj);
	}
  }
  // pass NULL to the syner threads to let them stop.
  for (int i = 0; i < test_config->num_threads; ++i) {
	ctx->dirty_queue->Enqueue(nullptr);
  }

  LOG_INFO("producer finished, has produced objs={}", cnt);
}

// simulate a pdfs worker thread
void WalWriteProducerWorker(WalWriteCtx* ctx, int idx) {
  std::string tname = "pdworker_";
  tname.append(std::to_string(idx));
  SetThreadName(pthread_self(), tname);

  auto test_config = ctx->test_config;
  KvObj obj(test_config->key_len, test_config->value_len);
  uint64_t cnt = 0;
  uint64_t to_gen = test_config->num_objs / test_config->num_producers;  // each producer generates this many objs
  LOG_INFO("worker_{} started, will produce {} objs", idx, to_gen);

  uint64_t t1 = NowInUsec();
  while (cnt < to_gen) {
	// Prepare object data: key = "thread_idx, obj_cnt"
	memcpy(obj.key.key_buf, &idx, sizeof(idx));
	memcpy(obj.key.key_buf + sizeof(int), &cnt, sizeof(cnt));
	memcpy(obj.value, obj.key.key_buf, sizeof(int) + sizeof(cnt));
	obj.is_processed = false;
    ctx->dirty_queue->Enqueue(&obj);

	// wait for completion
	obj.WaitForCompletion();
	ASSERT(obj.is_processed, "obj {} is not processed", cnt);
	cnt++;
  }

  double sec = (NowInUsec() - t1) / 1000000.0;
  double iops = cnt / sec;
  LOG_INFO("worker_{} finished in {} seconds, write iops={}", cnt, sec, iops);
}

// Simulate a syncer thread.
void WalWriteSyncer(WalWriteCtx* ctx, int idx) {
  std::string tname = "wb_syncer_";
  tname.append(std::to_string(idx));

  kvs_handle_t* kvs = ctx->kvs;
  auto db_wrapper = (DbWrapper*)kvs->kvs;
  KvObj *obj;
  uint64_t cnt = 0;
  uint64_t misses = 0;

  SetThreadName(pthread_self(), tname);
  LOG_INFO("syner {} started", idx);

  uint64_t t1 = NowInUsec();
  while (true) {
//	LOG_INFO("syncer {} before lock {}", idx, cnt);
	pthread_mutex_lock(&db_wrapper->sync_mtx_);
	if (ctx->dirty_queue->DequeueWait(&obj, 1, 10UL * 1000000) > 0) {
	  if (obj == NULL) {
		LOG_INFO("syncer_{} got null obj, will exit", idx);
		pthread_mutex_unlock(&db_wrapper->sync_mtx_);
		break;
	  }
	  // have grabbed an obj, start writing.
	  ASSERT(kvs_write_with_cb(kvs, obj->key.key_buf, obj->key_len, obj->value, obj->value_len) == 0,
			 "failed to write db at key={}", cnt);
	  obj->OnComplete();
	  cnt++;
	} else {
	  misses++;
	  pthread_mutex_unlock(&db_wrapper->sync_mtx_);
	}
  }
  double sec = (NowInUsec() - t1) / 1000000.0;
  LOG_INFO("syncer_{} finished, has written objs={} in {} sec, iops={}, miss={}",
		   idx, cnt, sec, cnt / sec, misses);
}

void TestWalWriteCallabck(TestRocksConfig* test_config, kvs_config_t* kvconfig, kvs_handle_t* kvs) {
  LOG_INFO("will test wal write callback, workers={} syncers={} objs={} key_len={} value_len={}",
		   test_config->num_producers, test_config->num_syncers, test_config->num_objs,
		   test_config->key_len, test_config->value_len);

  auto db_wrapper = (DbWrapper*)kvs->kvs;
  WalWriteCtx wctx(test_config->key_len, test_config->value_len, 100);
  wctx.test_config = test_config;
  wctx.kvconfig = kvconfig;
  wctx.kvs = kvs;

  // start workers (producers).
  std::vector<std::thread> workers;
  workers.reserve(test_config->num_producers);
  for (int i = 0; i < test_config->num_producers; i++) {
	workers.emplace_back(std::thread(WalWriteProducerWorker, &wctx, i));
  }
  // start syncer threads: consumers.
  std::vector<std::thread> syncers;
  syncers.reserve(test_config->num_syncers);
  for (int i = 0; i < test_config->num_syncers; ++i) {
	syncers.emplace_back(std::thread(WalWriteSyncer, &wctx, i));
  }
  for (int i = 0; i < test_config->num_producers; i++) {
	workers[i].join();
  }
  // Add some null obj to dirty queue to wake up syncer threads.
  for (int i = 0; i < test_config->num_syncers; ++i) {
	wctx.dirty_queue->Enqueue(nullptr);
  }
  for (int i = 0; i < test_config->num_syncers; ++i) {
	syncers[i].join();
  }
  LOG_INFO("wal write callback test done, triggered callbacks={}",
		   db_wrapper->wal_cb_->num_callbacks);
}
