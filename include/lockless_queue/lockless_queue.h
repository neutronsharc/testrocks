//
// Created by shawn.ouyang on 12/22/2023.
//

#ifndef LOCKLESS_QUEUE_H_
#define LOCKLESS_QUEUE_H_

#include <pthread.h>
#include <shared_mutex>
#include <unordered_map>

#include "atomicops.h"
#include "concurrentqueue.h"
#include "readerwriterqueue.h"
#include "blockingconcurrentqueue.h"

#include "util/util.h"
#include "util/logger.h"

template<typename T> class MpscQueue;

// context of a producer of the lockless queue.
template<typename T>
class ProducerContext {
 public:
  // producer's thread id.
  uint64_t thread_id;
  // how many objects the producer has added to the queue.
  size_t num_enqueues;
  // name of producer thread.
  std::string producer_name;

  // the token to mark a unique producer, so lockless queue can run more efficiently.
  moodycamel::ProducerToken producer_token;

  MpscQueue<T> *queue;

  ProducerContext(MpscQueue<T> *q) :
    producer_token(q->is_blocking() ? moodycamel::ProducerToken(*q->blocking_queue_) :
                   moodycamel::ProducerToken(*q->queue_)) {
    queue = q;
    thread_id = GetThreadPid();
    producer_name = GetThreadName(pthread_self());
    num_enqueues = 0;
  }

  ~ProducerContext() {
  }

  std::string ToString() const {
    std::string s;
    s.append("producer=").append(producer_name)
      .append(", tid=").append(std::to_string(thread_id))
      .append(", enqueue=").append(std::to_string(num_enqueues));
    return s;
  }
};

// A multi-producer, single-consumer queue.
// Each producer enqueues to a per-thread queue internally identified by a per-thread token,
// The consumer pulls from the internal queues that are not empty.
//
// This class is thread safe.
//
template<typename T>
class MpscQueue {
 public:
  MpscQueue(bool is_blocking = false, const std::string name = "mpsc queue") {
    is_blocking_ = is_blocking;
    name_ = name;
    num_deq_objects_ = 0;
    pthread_key_create(&tl_key_, MpscQueue::DestroyProducerContext);

    if (is_blocking) {
      blocking_queue_ = std::make_unique<moodycamel::BlockingConcurrentQueue<T>>();
    } else {
      queue_ = std::make_unique<moodycamel::ConcurrentQueue<T>>();
    }
  }

  ~MpscQueue() {
    LOG_INFO("will destruct mpsc queue: {}", ToString());
    pthread_key_delete(tl_key_);
    std::unique_lock<std::shared_timed_mutex> lock(producer_map_mtx_);
    for (auto it = producer_map_.begin(); it != producer_map_.end();) {
      delete it->second;
      it = producer_map_.erase(it);
    }
  }

  std::string ToString() {
    std::string s;
    s.append("name=").append(name_)
      .append(", blocking=").append(std::to_string(is_blocking_))
      .append(", has ").append(std::to_string(GetNumProducers())).append(" producers")
      .append(", total enq=").append(std::to_string(GetNumEnqueueObjects()))
      .append(", total deq=").append(std::to_string(GetNumDequeueObjects()));
    {
      std::shared_lock<std::shared_timed_mutex> lock(producer_map_mtx_);
      for (auto &it : producer_map_) {
        s.append("\n  ").append(it.second->ToString());
      }
    }
    return s;
  }

  // how many producers have ever added objects to this lockless queue.
  inline uint32_t GetNumProducers() const { return producer_map_.size(); }

  inline bool is_blocking() const { return is_blocking_; }

  const std::string& name() const { return name_; }

  // Add an object to internal lockless queue.
  inline void Enqueue(const T& obj) {
    auto ctx = GetProducerContext();
    if (is_blocking_) {
      ASSERT(blocking_queue_->enqueue(ctx->producer_token, obj) == true,
             "producer={}, failed to enqueue at {}", ctx->ToString(), name_);
    } else {
      ASSERT(queue_->enqueue(ctx->producer_token, obj) == true,
             "producer={}, failed to enqueue at {}", ctx->ToString(), name_);
    }
    ++ctx->num_enqueues;
  }

  inline void EnqueueBulk(const T objs[], int num) {
    auto ctx = GetProducerContext();
    if (is_blocking_) {
      ASSERT(blocking_queue_->enqueue_bulk(ctx->producer_token, objs, num) == true,
             "producer={}, failed to enqueue_bulk at {}, objs={}", ctx->ToString(), name_, num);
    } else {
      ASSERT(queue_->enqueue_bulk(ctx->producer_token, objs, num) == true,
             "producer={}, failed to enqueue_bulk at {}, objs={}", ctx->ToString(), name_, num);
    }
    ctx->num_enqueues += num;
  }

  // Non-blocking dequeue an obj, will return immediately.
  // Return true if the queue is non-empty, "obj" contains the dequeued obj.
  // Return false if the queue is empty.
  inline bool Dequeue(T& obj) {
    bool ret;
    if (is_blocking_) {
      ret = blocking_queue_->try_dequeue(obj);
    } else {
      ret = queue_->try_dequeue(obj);
    }
    if (ret) {
      ++num_deq_objects_;
    }
    return ret;
  }

  // Batch dequeue.
  inline size_t Dequeue(T objs[], int maxobjs) {
    size_t deq_objs = 0;
    if (is_blocking_) {
      deq_objs = blocking_queue_->try_dequeue_bulk(objs, maxobjs);
    } else {
      deq_objs = queue_->try_dequeue_bulk(objs, maxobjs);
    }
    num_deq_objects_ += deq_objs;
    return deq_objs;
  }

  // Try to fetch up to "maxobjs" objects from the queue.
  //
  // objs:    input array to store the dequeue objects. Caller must guarantee it's big enough
  //          to hold at least "maxobjs" objects.
  // wait_us (for blocking queue only): if > 0, will wait up to this usec if the queue remains empty.
  //          if the queue is non-blocking, this argument is ignored.
  //
  // Return number of dequeued objects.
  // May return 0 if the queue is empty after "wait_us" expires.
  //
  inline size_t Dequeue(T objs[], int maxobjs, uint64_t wait_us) {
    size_t deq_objs = 0;
    if (is_blocking_) {
      deq_objs = blocking_queue_->wait_dequeue_bulk_timed(objs, maxobjs, wait_us);
    } else {
      deq_objs = queue_->try_dequeue_bulk(objs, maxobjs);
    }
    num_deq_objects_ += deq_objs;
    return deq_objs;
  }

  // how many objects have been added to this queue.
  inline uint64_t GetNumEnqueueObjects() {
    uint64_t cnt = 0;
    std::shared_lock<std::shared_timed_mutex> lock(producer_map_mtx_);
    for (auto &it : producer_map_) {
      cnt += it.second->num_enqueues;
    }
    return cnt;
  }

  // how many objects have been dequeued from this queue.
  inline uint64_t GetNumDequeueObjects() { return num_deq_objects_; }

  // How many objects are in the queue. Because of the lockless + concurrency nature of this
  // queue, the queue size changes at any moment, so the returned value is only a rough estimation.
  inline uint64_t EstimateQueueSize() {
    uint64_t enq_objs = GetNumEnqueueObjects();
    uint64_t deq_objs = GetNumDequeueObjects();
    return (enq_objs >= deq_objs ? enq_objs - deq_objs : 0);
  }

  friend class ProducerContext<T>;
 private:
  // name of this lockless queue.
  std::string name_;

  // If this queue is blocking.
  // Set this to "true" if consumer needs to block and wait for available objects.
  // If false, consumer Dequeue() will not wait for objects to become available if the queue is empty.
  bool is_blocking_;

  // the actual queue that implements lockless queue.
  std::unique_ptr<moodycamel::ConcurrentQueue<T>> queue_;

  std::unique_ptr<moodycamel::BlockingConcurrentQueue<T>> blocking_queue_;

  // The thread-specific key to identify producers.
  pthread_key_t tl_key_;

  //
  // Map a producer's thread id to its context object.
  // When a producer thread starts, it registers its "context" object into this map.
  //
  // NOTE: this map owns "context" object so it can always refer to them for misc purposes.
  // Producer thread only refer to the "context" object.
  //
  // The map will only grow, it will never shrink, i.e., producer will not delete its context
  std::unordered_map<uint64_t, ProducerContext<T>*> producer_map_;

  // lock to protect the map.
  std::shared_timed_mutex producer_map_mtx_;

  // Cumulative number of objects dequeued.
  uint64_t num_deq_objects_;

  // Called when a producer thread exits to release the per-producer resources.
  // Since the "producer ctx" object is owned by "producer_map_" we don't do release here.
  static void DestroyProducerContext(void *value) {
    auto ctx = static_cast<ProducerContext<T>*>(value);
    LOG_INFO("producer stopped {}, ctx={}", GetThreadName(pthread_self()), ctx->ToString());
    // This thread doesn't need the tld any more.
    pthread_setspecific(ctx->queue->tl_key_, nullptr);
  }

  ProducerContext<T>* GetProducerContext() {
    auto ctx = static_cast<ProducerContext<T> *>(pthread_getspecific(tl_key_));
    if (unlikely(ctx == nullptr)) {
      ctx = new ProducerContext<T>(this);
      ASSERT(ctx != nullptr, "failed to allocate producer ctx");
      {
        std::unique_lock<std::shared_timed_mutex> lock(producer_map_mtx_);
        producer_map_.emplace(pthread_self(), ctx);
      }
      pthread_setspecific(tl_key_, ctx);
      LOG_INFO("thread {} created producer ctx=({}) to add to queue {} with {} producers",
               GetThreadPid(), ctx->ToString(), name_, GetNumProducers());
    }
    return ctx;
  }
};



#endif  // LOCKLESS_QUEUE_H_

