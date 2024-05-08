//
// Created by shawn.ouyang on 12/07/2023.
//

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/mman.h>
#include <unistd.h>
#include <mutex>
#include <urcu/uatomic/generic.h>
#include <liburing.h>
#include <sstream>

#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/convenience.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "rocksdb/statistics.h"
#include "rocksdb/perf_context.h"

#include "rocks/kvs_rocksdb.h"
#include "rocks/kvs.h"
#include "util/logger.h"

void SetBaseConfig(rocksdb::Options *options, RocksConfig *rconfig) {
}

/////////////////////////  taken from pdfs :: kvs_rocksdb.cpp

#define ODIRECT_ALIGN 4096UL
#define QUEUE_DEPTH_BIG 1024
#define QUEUE_DEPTH_SMALL 16

__thread kvs_handle_t *cur_handle;
__thread cf_handle_t *cur_cfh;
__thread bool busted_delinst_cleanup;

static bool block_cache_initialized = false;
static std::shared_ptr<rocksdb::Cache> bcache = nullptr;
static std::shared_ptr<rocksdb::MemTableRepFactory> memfactory = nullptr;
static uint64_t wal_write_cycles;
static uint64_t wal_appends;
static uint64_t wal_posts;
static uint64_t wal_syncs;
static uint64_t sst_appends;
static uint64_t sst_posts;
static uint64_t sst_syncs;
static uint64_t kvs_iter_seek_max_skipped_keys = 0;


inline uint64_t rdtsc()
{
  uint64_t a, d;
  __asm__ volatile("rdtsc" : "=a"(a), "=d"(d));
  return (d << 32) | a;
}

typedef union pdfs_func_cycles {
  struct {
    uint64_t     fc_cycles;
    uint64_t     fc_calls;
  };
  __uint128_t fc_u128;
} pdfs_func_cycles_t;


static int translate_status(const rocksdb::Status &st) {
  switch (st.code()) {
    case Status::kNotFound:           return ENOENT;
    case Status::kCorruption:         return EIO;
    case Status::kNotSupported:       return EOPNOTSUPP;
    case Status::kInvalidArgument:    return EINVAL;
    case Status::kIOError:            return EIO;
    case Status::kMergeInProgress:    return EINPROGRESS;
    case Status::kIncomplete:         return EUCLEAN;
    case Status::kShutdownInProgress: return ESHUTDOWN;
    case Status::kTimedOut:           return ETIMEDOUT;
    case Status::kAborted:            return ECANCELED;
    default:			    return EIO;
  }
}

static int translate_db_op_status(const Status &st) {
  int err = translate_status(st);
  if (err == EUCLEAN) {
    /*
     * db operations like db open and db delete return EUCLEAN when there is
     * lock file contention. so in these operations, we know that EUCLEAN
     * actually means EBUSY.
     */
    return EBUSY;
  }
  return err;
}

static bool is_status_corrupted(const rocksdb::Status &st) {
  return (st.code() == rocksdb::Status::kCorruption);
}

std::string kvs_config::ToString() const {
  std::stringstream ss;
  ss << "\ncreate_if_missing = " << create_if_missing
	 << "\nerror_if_exists = " << error_if_exists
	 << "\nuse_snappy_compression = " << use_snappy_compression
	 << "\noptimize_for_level_style_compaction = " << optimize_for_level_style_compaction
	 << "\nsync = " << sync
	 << "\ndisable_dbwal = " << disable_dbwal
     << "\nblkcache_size = " << blkcache_size / 1024 / 1024 << " MB"
     << "\nodirect_read = " << odirect_read
     << "\nodirect_flush_compact = " << odirect_flush_compact
     << "\nmax_open_files = " << max_open_files
	 << "\nwrite_buffer_size= " << write_buffer_size_mb << " MB"
     << "\nncpus = " << ncpus
     << "\nwal_callback = " << wal_callback;
  return ss.str();
}

DbWrapper::DbWrapper(const char *db_path) {
  db_path_ = db_path;
  pthread_mutex_init(&sync_mtx_, NULL);
  wal_cb_ = std::make_unique<PdfsWalWriteCallback>(&sync_mtx_);
}

PdfsWalFile::PdfsWalFile(const rocksdb::EnvOptions &options, uint64_t write_buffer_sz) :
  fd(-1),
  size(0),
  preallocate(roundup(write_buffer_sz, HUGE_PAGE)),
  prealloc_end(0),
  buff(nullptr),
  buflen(0),
  mmapped(true),
  dirty(false),
  last_sync(0)
{
}

PdfsWalFile::~PdfsWalFile(void) {
  if (fd > 0) {
    Close();
  }
  if (mmapped) {
    munmap(buff, buflen);
  } else {
    free(buff);
  }
}

Status PdfsWalFile::Open(const std::string &fname) {
  uint64_t t = rdtsc();
  int err;

  buflen = roundup(preallocate / 8, HUGE_PAGE);
  buff = (caddr_t)mmap(NULL, buflen, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (buff == MAP_FAILED) {
//    pd_trc_ntc("wanted npages=%lu falling back to malloc", buflen / HUGE_PAGE);
    mmapped = false;
    err = posix_memalign((void **)&buff, HUGE_PAGE, buflen);
    if (err) {
//      pd_trc_err("open failed. err=%d", err);
//      return Status::IOError();
	  return Status::InvalidArgument();
    }
  }
//  fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_DIRECT | O_DSYNC, 0644);
  fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_DSYNC, 0644);
  if (fd < 0) {
    err = errno;
	LOG_ERROR("failed to open file={}", fname);
//    pd_trc_err("open failed. err=%d", err);
    return Status::IOError();
  }
  struct stat info;
  err = fstat(fd, &info);
  if (!err) {
    if ((uint64_t)info.st_size > preallocate) {
      prealloc_end = roundup(info.st_size, buflen);
    } else if (info.st_size) {
      prealloc_end = preallocate;
    }
  }
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  LOG_INFO("opened wal file={}, preallocate={}, buflen={}, prealloc_end={}, mmapped={}, st_size={}",
           fname, preallocate, buflen, prealloc_end, mmapped, info.st_size);
  return Status::OK();
}

int
PdfsWalFile::do_pwrite(char *data, size_t len, off_t offset)
{
  while (true) {
    ssize_t bytes = pwrite(fd, data, len, offset);
    if (bytes < 0) {
      int err = errno;
      switch (err) {
        case EINTR:
          continue;
        case EFAULT:
//          LOG_ERROR(err, "ptr=%p len=%lu offset=%lu", data, len, offset);
        default:
//          pd_trc_err("write failed. ptr=%p sz=%lu offset=%lu err=%d",
//                     data, len, offset, err);
          return err;
      }
    }
    uatomic_inc(&wal_posts);
    if ((size_t)bytes == len) {
      return 0;
    }
  }
}

Status
PdfsWalFile::FlushInternal(void)
{
  static const size_t alignment = 4096;
  size_t sz;

  if (!dirty) {
    return Status::OK();
  }

  sz = roundup(size - last_sync, alignment);
//  BUG_ON(last_sync % buflen + sz > buflen,
//         "last_sync=%lu + sz=%lu > buflen=%lu", last_sync, sz, buflen);
  int err = do_pwrite(&buff[last_sync % buflen], sz, last_sync);
  if (err) {
//    pd_trc_err("write failed. ptr=%p sz=%lu offset=%lu err=%d",
//               &buff[last_sync % buflen], sz, last_sync, err);
    return Status::IOError();
  }
  last_sync = size & ~(alignment - 1);
  dirty = false;
  uatomic_inc(&wal_syncs);
  return Status::OK();
}

Status
PdfsWalFile::Close(void)
{
  std::lock_guard<std::mutex> guard(mtx);
  int err;
  if (fd > 0) {
    uint64_t t = rdtsc();
    Status st = FlushInternal();
    if (!st.ok()) {
      return st;
    }
    err = close(fd);
    uatomic_add(&wal_write_cycles, rdtsc() - t);
    if (err) {
      err = errno;
//      pd_trc_err("close failed. err=%d", err);
      return Status::IOError();
    }
    fd = -1;
  }
  return Status::OK();
}

Status
PdfsWalFile::Sync(void)
{
  uint64_t t = rdtsc();
  std::lock_guard<std::mutex> guard(mtx);
  Status st = FlushInternal();
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  return st;
}

int
PdfsWalFile::clear_and_falloc(size_t nbytes, off_t offset)
{
  memset(buff, 0, nbytes);
  if (size + nbytes <= prealloc_end) {
    return 0;
  }
  size_t extended;
  if (prealloc_end) {
    extended = buflen;
  } else {
    extended = preallocate;
  }
  int err = posix_fallocate(fd, prealloc_end, extended);
  if (err) {
//    pd_trc_err("falloc failed, offset=%lu offset=%lu err=%d", prealloc_end,
//               extended, err);
  }
  prealloc_end += extended;
  return 0;
}

Status
PdfsWalFile::AppendSegment(const char *ptr, size_t nbytes)
{
  size_t boff = size % buflen;
  if (!boff && nbytes < buflen) {
    int err = clear_and_falloc(buflen, size);
    if (err) {
//      pd_trc_err("clear / falloc failed. err=%d", err);
      return Status::IOError();
    }
  }
//  BUG_ON(boff + nbytes > buflen, "write past end of buffer");
  memcpy(&buff[boff], ptr, nbytes);
  size += nbytes;
  dirty = true;
  uatomic_inc(&wal_appends);
  return Status::OK();
}

Status
PdfsWalFile::PositionedAppend(const Slice &data, uint64_t offset)
{
  uint64_t t = rdtsc();
  const char *ptr = data.data();
  size_t sz = data.size();
  size_t boff;
  Status st;
  std::lock_guard<std::mutex> guard(mtx);

  boff = size % buflen;
  while (sz) {
    size_t nbytes = MIN(buflen - boff, sz);
    st = AppendSegment(ptr, nbytes);
    if (!st.ok()) {
      break;
    }
    ptr += nbytes;
    sz -= nbytes;
    boff = size % buflen;
    if (sz || !boff) {
      st = FlushInternal();
      if (!st.ok()) {
        break;
      }
    }
  }
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  return st;
}

Status
PdfsWalFile::Append(const Slice &data)
{
  return PositionedAppend(data, size);
}

class PdfsUringFile : public rocksdb::WritableFile {
 public:
  explicit PdfsUringFile(const rocksdb::EnvOptions &options, uint64_t write_buffer_sz);
  ~PdfsUringFile();

  Status Append(const Slice &data);
  Status Open(const std::string &fname);
  Status Close();
  Status Sync();
  Status Truncate(uint64_t size) { return Status::OK(); }
  Status Flush(void);
  Status Allocate(uint64_t offset, uint64_t len) { return Status::OK(); }
  uint64_t GetFileSize() { return size; }

private:
  int post_to_uring(void);
  int complete_cqe(struct io_uring_cqe *cqe);
  int wait_on_pending_io(void);
  struct io_uring_sqe *get_sqe(int *err);
  int do_pwrite(const char *data, size_t len, off_t offset);

  bool is_wal;
  int fd;
  caddr_t buff;
  size_t buflen;
  bool unposted;
  int pending;
  size_t last_flush;
  size_t size;
  struct io_uring ring;
  std::mutex mtx;
};

PdfsUringFile::PdfsUringFile(const rocksdb::EnvOptions &options,
                             uint64_t write_buffer_sz) :
  is_wal(false),
  fd(-1),
  buff(nullptr),
  buflen(roundup(write_buffer_sz, HUGE_PAGE)),
  unposted(false),
  pending(0),
  last_flush(0),
  size(0) {}

PdfsUringFile::~PdfsUringFile(void) {
  if (fd > 0) {
    Close();
  }
  free(buff);
}

int PdfsUringFile::complete_cqe(struct io_uring_cqe *cqe) {
  int err = cqe->res;
  io_uring_cqe_seen(&ring, cqe);
  --pending;
  if (err < 0) {
    LOG_ERROR("I/O failure, err={}", -err);
    return -err;
  }
  return 0;
}

int PdfsUringFile::post_to_uring(void) {
  if (!unposted) {
    return 0;
  }
  if (is_wal) {
    uatomic_inc(&wal_posts);
  } else {
    uatomic_inc(&sst_posts);
  }
  int err = io_uring_submit(&ring);
  if (err < 0) {
    LOG_ERROR("failed to submit to iouring, err={}", -err);
    return -err;
  }
  last_flush = size;
  unposted = false;
  return 0;
}

int PdfsUringFile::wait_on_pending_io(void) {
  uint64_t t = rdtsc();
  struct io_uring_cqe *cqe;
  int err;

  err = post_to_uring();
  if (err) {
    return err;
  }
  while (pending) {
    if (!io_uring_wait_cqe(&ring, &cqe)) {
      int res = complete_cqe(cqe);
      if (res) {
        err = res;
      }
    }
  }
  if (!err) {
    err = fdatasync(fd);
    if (err) {
      err = -errno;
    }
  }
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  return err;
}

struct io_uring_sqe* PdfsUringFile::get_sqe(int *err) {
  struct io_uring_sqe *sqe;
  struct io_uring_cqe *cqe;

  for (sqe = io_uring_get_sqe(&ring); !sqe; sqe = io_uring_get_sqe(&ring)) {
    *err = post_to_uring();
    if (*err < 0) {
      return NULL;
    }
    if (!io_uring_wait_cqe(&ring, &cqe)) {
      *err = complete_cqe(cqe);
      if (*err) {
        return NULL;
      }
    }
  }
  unposted = true;
  ++pending;
  return sqe;
}

Status PdfsUringFile::Open(const std::string &fname) {
  uint64_t t = rdtsc();
  int err;
  int qd;

  if (fname.find(".log") != std::string::npos) {
    qd = QUEUE_DEPTH_SMALL;
    is_wal = true;
  } else {
    qd = QUEUE_DEPTH_BIG;
    buflen = 64 * 1024 * 1024; // assume 64MB
  }
  err = io_uring_queue_init(qd, &ring, 0);
  ASSERT(err >= 0, "io_uring queue init failed, err={}", -err);

  buff = (caddr_t)malloc(buflen);
  if (!buff) {
    LOG_ERROR("open failed. err={}", err);
    io_uring_queue_exit(&ring);
    return Status::IOError();
  }

  fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    err = errno;
    LOG_ERROR("open failed. err={}", err);
    io_uring_queue_exit(&ring);
    return Status::IOError();
  }
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  return Status::OK();
}

int PdfsUringFile::do_pwrite(const char *data, size_t len, off_t offset) {
  uint64_t t = rdtsc();
  struct io_uring_sqe *sqe;
  int err;

  if ((sqe = get_sqe(&err)) == NULL) {
    return err;
  }
  if (last_flush % buflen == 0 && is_wal) {
    io_uring_prep_fallocate(sqe, fd, 0, last_flush, buflen);
    sqe->flags |= IOSQE_IO_LINK;
    if ((sqe = get_sqe(&err)) == NULL) {
      return EIO;
    }
  }
  io_uring_prep_write(sqe, fd, data, len, offset);
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  return 0;
}

Status PdfsUringFile::Flush(void) {
  return Status::OK();
}

Status PdfsUringFile::Close(void) {
  uint64_t t = rdtsc();
  std::lock_guard<std::mutex> guard(mtx);
  int err;
  if (fd > 0) {
    err = wait_on_pending_io();
    if (err) {
      return Status::IOError();
    }
    err = close(fd);
    if (err) {
      err = errno;
      LOG_ERROR("close failed. err={}", err);
      return Status::IOError();
    }
    fd = -1;
    io_uring_queue_exit(&ring);
  }
  uatomic_add(&wal_write_cycles, rdtsc() - t);
  return Status::OK();
}

Status
PdfsUringFile::Sync(void) {
  std::lock_guard<std::mutex> guard(mtx);
  int err = wait_on_pending_io();
  if (is_wal) {
    uatomic_inc(&wal_syncs);
  } else {
    uatomic_inc(&sst_syncs);
  }
  return err ? Status::IOError() : Status::OK();
}

Status
PdfsUringFile::Append(const Slice &data) {
  std::lock_guard<std::mutex> guard(mtx);
  const char *ptr = data.data();
  size_t sz = data.size();
  int err = 0;
  while (sz) {
    size_t boff = size % buflen;
    size_t nbytes;
    if (!boff && last_flush != size) {
      err = wait_on_pending_io();
      if (err) {
        break;
      }
    }
    if (boff + sz > buflen) {
      nbytes = buflen - boff;
    } else {
      nbytes = sz;
    }
    memcpy(&buff[boff], ptr, nbytes);
    err = do_pwrite(&buff[boff], nbytes, size);
    if (err) {
      break;
    }
    sz -= nbytes;
    ptr += nbytes;
    size += nbytes;
  }
  if (err) {
    return Status::IOError();
  }
  if (is_wal) {
    uatomic_inc(&wal_appends);
  } else {
    uatomic_inc(&sst_appends);
  }
  return Status::OK();
}


class PdfsEnv : public rocksdb::EnvWrapper
{
public:
  PdfsEnv(rocksdb::Env *base_env, rocksdb::Options *kvs_options) : EnvWrapper(base_env) {
    write_buffer_size = kvs_options->write_buffer_size;
    saved_options = *kvs_options;
  }
  // NewWritableFile opens a file for sequential writing.
  virtual Status NewWritableFile(const std::string &fname,
                                 std::unique_ptr<rocksdb::WritableFile> *result,
                                 const rocksdb::EnvOptions &options) override {
    Status st;
//    LOG_INFO("will create a new file={}, write_buffer_size={}, recycle_log_file_num={}",
//             fname, write_buffer_size, saved_options.recycle_log_file_num);
    if (fname.find(".log") != std::string::npos) {
      PdfsWalFile *rval = new PdfsWalFile(options, write_buffer_size);
      if (saved_options.recycle_log_file_num) {
        reusable_wal_mtx.lock();
        if (!reusable_wal_files.empty()) {
          std::string old = reusable_wal_files.back();
          reusable_wal_files.pop_back();
          st = EnvWrapper::RenameFile(old, fname);
          if (st != Status::OK()) {
            st = EnvWrapper::DeleteFile(fname);
            if (st != Status::OK()) {
              LOG_ERROR("failed to rename/unlink file={}", old);
            }
          }
        }
        reusable_wal_mtx.unlock();
      }
      st = rval->Open(fname);
      if (st == Status::OK()) {
        result->reset(dynamic_cast<rocksdb::WritableFile *>(rval));
      } else {
        delete rval;
      }
      return st;
    } else if (fname.find(".blob") != std::string::npos ||
               fname.find(".sst") != std::string::npos) {
      PdfsUringFile *rval = new PdfsUringFile(options, write_buffer_size);
      st = rval->Open(fname);
      if (st == Status::OK()) {
        result->reset(dynamic_cast<rocksdb::WritableFile *>(rval));
      } else {
        delete rval;
      }
      return st;
    }
    return EnvWrapper::NewWritableFile(fname, result, options);
  }

  Status DeleteFile(const std::string &fname) {
//    LOG_INFO("will delete a file={}", fname);
    if (saved_options.recycle_log_file_num &&
        fname.find(".log") != std::string::npos) {
      reusable_wal_mtx.lock();
      if (reusable_wal_files.size() >= saved_options.recycle_log_file_num) {
        reusable_wal_mtx.unlock();
        return EnvWrapper::DeleteFile(fname);
      }
      reusable_wal_files.emplace_back(fname);
      reusable_wal_mtx.unlock();
      return Status::OK();
    }
    return EnvWrapper::DeleteFile(fname);
  }

private:
  uint64_t write_buffer_size;
  rocksdb::Options saved_options;
  std::mutex reusable_wal_mtx;
  std::vector<std::string> reusable_wal_files;
};



static void alloc_block_cache(std::shared_ptr<rocksdb::Cache> &bcache, int ncpus, size_t sz) {
  bcache = rocksdb::HyperClockCacheOptions(sz, 4096).MakeSharedCache();
//  bcache = rocksdb::NewLRUCache(sz, 16);
  if (!bcache) {
    LOG_ERROR("hyper clock cache not supported. Not using block cache");
  } else {
    LOG_INFO("block cache size={}", sz);
  }

  int buckets = ncpus * 1024;
  memfactory.reset(rocksdb::NewHashSkipListRepFactory(buckets));
  LOG_INFO("use NewHashSkipListRepFactory memtable");
}

static void set_base_configuration(rocksdb::Options *kvs_options, kvs_config_t *kvsc) {
  // dirent x2, inode, data, obj x3, time index, event, layout record
  static const int max_bgnd = PDK_COUNT;

  kvs_options->create_if_missing = kvsc->create_if_missing;
  kvs_options->error_if_exists = kvsc->error_if_exists;
  kvs_options->paranoid_checks = false;  // XXX: avoid HS-10238
  kvs_options->create_missing_column_families = true;

  kvs_options->write_buffer_size = 1024UL * 1024 * kvsc->write_buffer_size_mb;

  kvs_options->max_total_wal_size = kvs_options->write_buffer_size * max_bgnd * 2;

  kvs_options->recycle_log_file_num = kvsc->wal_reuse ? 2 : 0;  // always "false"
  kvs_options->use_direct_reads = kvsc->odirect_read;  // always "false".
  kvs_options->use_direct_io_for_flush_and_compaction = kvsc->odirect_flush_compact; // always "true"??
  kvs_options->delete_obsolete_files_period_micros = 5UL * 1000000UL;
  kvs_options->keep_log_file_num = 2;

  kvs_options->check_flush_compaction_key_order = false; // XXX: avoid HS-10238
  kvs_options->allow_concurrent_memtable_write = false;
  kvs_options->avoid_flush_during_recovery = true;
  if (kvsc->use_snappy_compression) {
    kvs_options->compression = rocksdb::kLZ4Compression;
    kvs_options->bottommost_compression = rocksdb::kLZ4Compression;
  } else {
    kvs_options->compression = rocksdb::kNoCompression;
    kvs_options->bottommost_compression = rocksdb::kNoCompression;
  }
  if (kvsc->optimize_for_universal_style_compaction) {
    kvs_options->OptimizeUniversalStyleCompaction();
  }
  if (kvsc->optimize_for_level_style_compaction) {
    kvs_options->OptimizeLevelStyleCompaction();
  }
  kvs_options->max_background_jobs = max_bgnd;
  kvs_options->env->SetBackgroundThreads(max_bgnd, rocksdb::Env::LOW);
  kvs_options->env->SetBackgroundThreads(max_bgnd / 2, rocksdb::Env::HIGH);
  if (kvsc->blkcache_size > 1024UL * 1024 * 1024) {  // big blkcache, so memtable uses hugepage.
    kvs_options->memtable_huge_page_size = HUGE_PAGE;
  }
  kvs_options->memtable_factory = memfactory;
  // set_info_log_level(kvs_options);
  if (kvsc->wal_dir) {
	kvs_options->wal_dir = std::string(kvsc->wal_dir);
  }

  kvs_options->avoid_unnecessary_blocking_io = true;
  kvs_options->statistics = rocksdb::CreateDBStatistics();
  kvs_options->stats_dump_period_sec = 0;

  kvs_options->env = new PdfsEnv(rocksdb::Env::Default(), kvs_options);
}

static void
set_cf_configuration(DbWrapper *db_wrapper, const char *cf_name,
					 rocksdb::ColumnFamilyOptions &cf_options, kvs_config_t *kvsc,
					 bool universal_compaction,
					 bool use_blobdb, size_t blob_minsz) {
  if (kvsc->key_cmp_func) {
	LOG_ERROR("cf={} use customer cmp func", cf_name);
    KvsComparator *cmp = new KvsComparator(kvsc->key_cmp_func, cf_name);
	cf_options.comparator = cmp;
    db_wrapper->comparators_.push_back(cmp);
  } else {
    LOG_INFO("cf={} use default cmp function", cf_name);
  }
  // Config Merger op if needed.

  if (kvsc->use_snappy_compression) {
	cf_options.compression = rocksdb::kLZ4Compression;
	cf_options.bottommost_compression = rocksdb::kLZ4Compression;
  } else {
	cf_options.compression = rocksdb::kNoCompression;
	cf_options.bottommost_compression = rocksdb::kNoCompression;
  }
  if (universal_compaction) {
    LOG_INFO("cf={} use universal compaction", cf_name);
    cf_options.OptimizeUniversalStyleCompaction();
	cf_options.compaction_options_universal.allow_trivial_move = true;
  } else {
//    LOG_INFO("cf={} use level compaction", cf_name);
    cf_options.OptimizeLevelStyleCompaction();
  }
//  cf_options.write_buffer_size = kvsc->write_buffer_size_mb * 1024 * 1024;
  cf_options.min_write_buffer_number_to_merge = 1;
  cf_options.level0_slowdown_writes_trigger = 100;
  cf_options.level0_stop_writes_trigger = 1000;
  if (kvsc->blkcache_size > 1024UL * 1024 * 1024) {
	cf_options.memtable_huge_page_size = HUGE_PAGE;
  }
  cf_options.memtable_factory = memfactory;
  cf_options.force_consistency_checks = false;  // XXX: avoid HS-10238
  if (!strcmp(cf_name, "PDK_SBP")) {
	cf_options.level0_file_num_compaction_trigger = 1;
  } else {
	cf_options.level0_file_num_compaction_trigger = 4;
  }

  cf_options.enable_blob_files = use_blobdb;
  if (use_blobdb) {
	cf_options.min_blob_size = blob_minsz;
	cf_options.blob_compression_type = rocksdb::kLZ4Compression;
  }
}

static int
kvs_add_column_family(kvs_handle_t *kvs_handle, kvs_config_t *config, const char *cfname, cf_handle_t* cf_hdl) {
  DbWrapper *db_wrapper = (DbWrapper *)(kvs_handle->kvs);
  bool use_blob = false;
  if (db_wrapper->db_) {
	LOG_ERROR("db already opened before init CF={}", cfname);
	return -1;
  }
  rocksdb::ColumnFamilyOptions cf_opts(db_wrapper->kvs_options_);
  set_cf_configuration(db_wrapper, cfname, cf_opts, config, false, false, 0);

  rocksdb::ColumnFamilyDescriptor cfd(cfname, cf_opts);
  db_wrapper->cf_descriptors_.push_back(cfd);
  cf_hdl->handle = (void*)(db_wrapper->cf_descriptors_.size() - 1);
  LOG_INFO("cf=\"{}\" cf_handle={}, cfds={}",
		   cfname, (uint64_t)cf_hdl->handle, db_wrapper->cf_descriptors_.size());
  return 0;
}

static int
setup_column_families(kvs_handle_t *kvs_handle, kvs_config_t *kvs_config, cf_handle_t* cfs) {
  int err, i;
  const char* cfname;
  for (i = PDK_SBP; i < PDK_COUNT; ++i) {
	cfname = pdfs_key_type_to_str((pdfs_key_type_t)i);
	if (i == PDK_EVENT) {
	  // pass a special comparator to EVENT cf.
	  kvs_config->key_cmp_func = pdfs_event_key_cmp;
	} else {
	  kvs_config->key_cmp_func = nullptr;
	}
	LOG_INFO("will add cf name={} idx={}", cfname, i);
	//PDK_CF idx i  =>  persistence->cf_handles[] idx i,
	// cf_handle[i].handle is an index into actual kvdb.cf_handles_[], which is actual DB column family.
	// PDK_0  =>  rocksdb CF=1,  PDK_1 => rocksdb CF=2, and so on.
    err = kvs_add_column_family(kvs_handle, kvs_config, cfname, cfs + i);
  }
  return err;
}

int kvs_init_db(kvs_handle_t *kvs,
                kvs_config_t *kvconfig) {
  DbWrapper *db_wrapper = new DbWrapper(kvconfig->db_dir);
  if (!db_wrapper) {
    return ENOMEM;
  }
  // 1. set the base options, which applies to the whole db (all CFs).
  rocksdb::Options* options = &db_wrapper->kvs_options_;
  set_base_configuration(options, kvconfig);

  options->avoid_unnecessary_blocking_io = true;
  options->statistics = rocksdb::CreateDBStatistics();
  options->enable_pipelined_write = kvconfig->wal_callback;

  LOG_INFO("db_wrapper={} db_path='{}' wal_path='{}' bcache={} block cache size={} MB",
           (void*)db_wrapper, kvconfig->db_dir, options->wal_dir,
		   (void*)(bcache.get()), kvconfig->blkcache_size / 1024 / 1024);

  // 2. prepare block cache shared by all CFs.
  if (!block_cache_initialized) {
    alloc_block_cache(bcache, kvconfig->ncpus, kvconfig->blkcache_size);
    block_cache_initialized = true;
  }

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = bcache;
  table_options.prepopulate_block_cache =
	rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kDisable;
  options->table_factory.reset(NewBlockBasedTableFactory(table_options));

  // 3. Set column family options for the default CF.
  bool univ_compaction = false;  // default to level-compaction.
  bool use_blob = false;
  rocksdb::ColumnFamilyOptions cf_options(*options);
  set_cf_configuration(db_wrapper, rocksdb::kDefaultColumnFamilyName.c_str(),
                       cf_options, kvconfig, univ_compaction, use_blob, 0);

  rocksdb::ColumnFamilyDescriptor cfd(rocksdb::kDefaultColumnFamilyName, cf_options);
  db_wrapper->cf_descriptors_.push_back(cfd);
  db_wrapper->default_cfh_.handle = 0;  // default_cf is at index=0.
  LOG_INFO("cf=\"{}\" cf_handle={}, number of cfds={}",
		   cfd.name, (uint64_t)db_wrapper->default_cfh_.handle, db_wrapper->cf_descriptors_.size());

  // 4. global read/write options.
  db_wrapper->woptions_default_.sync = kvconfig->sync;
  db_wrapper->woptions_default_.disableWAL = kvconfig->disable_dbwal;
  db_wrapper->woptions_nosync_.sync = false;
  db_wrapper->woptions_nosync_.disableWAL = kvconfig->disable_dbwal;

  kvs->kvs = db_wrapper;
  strncpy(kvs->name, kvconfig->db_dir, PATH_MAX - 1);
  return 0;
}


int kvs_open_db(kvs_handle_t *kvs,
                bool read_only) {
  DbWrapper *db_wrapper = (DbWrapper *)kvs->kvs;
  rocksdb::DBOptions dboption = rocksdb::DBOptions(db_wrapper->kvs_options_);
  rocksdb::Status st;
  ASSERT(block_cache_initialized, "block cache not initialized yet");

  LOG_INFO("will open db path={} with column family={}, write_buffer_size={}, pipelined_write={}",
		   db_wrapper->db_path_, db_wrapper->cf_descriptors_.size(),
		   db_wrapper->kvs_options_.write_buffer_size, dboption.enable_pipelined_write);
  if (read_only) {
    st = rocksdb::DB::OpenForReadOnly(dboption, db_wrapper->db_path_,
									  db_wrapper->cf_descriptors_,
									  &db_wrapper->cf_handles_, &db_wrapper->db_);
  } else {
    st = rocksdb::DB::Open(dboption, db_wrapper->db_path_, db_wrapper->cf_descriptors_,
						   &db_wrapper->cf_handles_, &db_wrapper->db_);
  }
  if (!st.ok()) {
    LOG_ERROR("failed to open db. path='{}' error='{}'",
               db_wrapper->db_path_, st.ToString());
    // if error_if_exists is triggered, the status is Aborted - there is no
    // EEXIST equivalent.
    if (db_wrapper->kvs_options_.error_if_exists && st.IsNotFound()) {
      return EEXIST;
    }
    return translate_db_op_status(st);
  }
  for (size_t i = 0; i < db_wrapper->cf_handles_.size(); i++) {
	auto handle = db_wrapper->cf_handles_[i];
	LOG_INFO("have opened rocksdb cf idx={} name={} cf_id={}", i, handle->GetName(), handle->GetID());
  }
  return 0;
}

/**
 *
 * The expected call-seq to destroy a db is:
    kvs_close_db(kvs_handle);
	kvs_destroy_db(kvs_handle);
	kvs_delete_db(kvs_handle->name);
 */
void kvs_close_db(kvs_handle_t *kvs) {
  DbWrapper *db_wrapper = (DbWrapper *)(kvs->kvs);
  rocksdb::DB *db = db_wrapper->db_;
  LOG_INFO("will close db={}", db_wrapper->db_path_);

  LOG_INFO("delete {} cf...", db_wrapper->cf_handles_.size());
  for (auto *cf : db_wrapper->cf_handles_) {
    delete cf;
  }
  // close the db.  db instance will be deleted in DbWrapper.dtor.
  rocksdb::Status s = db->Close();
  LOG_INFO("close db={} ret={}", db_wrapper->db_path_, s.ToString());

  db_wrapper->cf_handles_.clear();
  db_wrapper->cf_descriptors_.clear();
  db_wrapper->db_path_.clear();

  for (auto *cmp : db_wrapper->comparators_) {
    delete cmp;
  }
  db_wrapper->comparators_.clear();
}

void kvs_destroy_db(kvs_handle_t *kvs) {
  // Will delete DB::db at DbWrapper dtor.
  delete (DbWrapper *)kvs->kvs;
}

int kvs_delete_db(const char *path) {
  rocksdb::Options options;
  rocksdb::Status st = rocksdb::DestroyDB(path, options);
  if (!st.ok()) {
    LOG_ERROR("failed to delete the DB. path='{}' state='{}'", path, st.getState());
    return translate_db_op_status(st);
  }
  return 0;
}


// "dbname" is the db path.
int kvs_db_open_byname(Persistence* persistence, kvs_config_t* kvs_config, const char* dbname) {
  persistence->dbname = std::string(dbname);  // dbname is db-path.
  LOG_INFO("will open db path={} with config=\n{}", dbname, kvs_config->ToString());

  // 1, prepare db options.
  int err = kvs_init_db(&persistence->kvs_handle, kvs_config);
  if (err) {
    LOG_ERROR("failed to init db={}", dbname);
    return err;
  }

  // 2, set up column-family options for more CFs.
  // Default CF has been setup in kvs_init_db().
  setup_column_families(&persistence->kvs_handle, kvs_config, persistence->cf_handles);

  // 3, open db
  bool readonly = false;
  err = kvs_open_db(&persistence->kvs_handle, readonly);
  if (err) {
    LOG_ERROR("failed to open db={}", dbname);
    return err;
  }

  // 4. prepare trace writer if required.
  /*if (kvs_config->db_trace) {
	DbWrapper *db_wrapper = (DbWrapper*)(persistence->kvs_handle.kvs);
	rocksdb::EnvOptions env_options(db_wrapper->kvs_options_);
	rocksdb::Status st = rocksdb::NewFileTraceWriter(db_wrapper->kvs_options_.env,
													 env_options,
													 std::string(kvs_config->db_trace_path),
													 &db_wrapper->trace_writer_);
	if (!st.ok()) {
	  LOG_ERROR("failed to open trace writer={} err={}", kvs_config->db_trace_path, st.ToString());
	  return translate_db_op_status(st);
	} else {
	  LOG_INFO("have opened trace writer={}", kvs_config->db_trace_path);
	}
  } */
  return 0;
}

int kvs_start_trace(kvs_config_t *kvconfig, kvs_handle_t *kvs) {
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  rocksdb::EnvOptions env_options(wrapper->kvs_options_);
  rocksdb::Status st = rocksdb::NewFileTraceWriter(wrapper->db_->GetEnv(),  // #kvs_options_.env,
												   env_options,
												   std::string(kvconfig->db_trace_path),
												   &wrapper->trace_writer_);
  if (!st.ok()) {
	LOG_ERROR("failed to open trace writer={} err={}", kvconfig->db_trace_path, st.ToString());
	return translate_db_op_status(st);
  }
  rocksdb::TraceOptions trace_options;
  st = wrapper->db_->StartTrace(trace_options, std::move(wrapper->trace_writer_));
  if (!st.ok()) {
	LOG_ERROR("failed to start to record trace err={}", st.ToString());
	return translate_db_op_status(st);
  }
  return 0;
}

int kvs_end_trace(kvs_handle_t *kvs) {
  DbWrapper *wrapper = (DbWrapper *)(kvs->kvs);
  rocksdb::Status st = wrapper->db_->EndTrace();
  if (!st.ok()) {
	LOG_ERROR("failed to stop trace err={}", st.ToString());
	return translate_db_op_status(st);
  }
  return 0;
}

int kvs_replay(kvs_config_t *kvconfig, kvs_handle_t *kvs) {
  DbWrapper *wrapper = (DbWrapper *)(kvs->kvs);
  rocksdb::EnvOptions env_options(wrapper->kvs_options_);
  rocksdb::Status st;
  std::unique_ptr<rocksdb::TraceReader> trace_reader;
  std::unique_ptr<rocksdb::Replayer> replayer;

  LOG_INFO("will replay trace={}", kvconfig->db_trace_path);
  st = rocksdb::NewFileTraceReader(wrapper->db_->GetEnv(), env_options, kvconfig->db_trace_path, &trace_reader);
  if (!st.ok()) {
	LOG_ERROR("failed to create trace reader path={} err={}", kvconfig->db_trace_path, st.ToString());
	return translate_db_op_status(st);
  }
  st = wrapper->db_->NewDefaultReplayer(wrapper->cf_handles_, std::move(trace_reader), &replayer);
  if (!st.ok()) {
	LOG_ERROR("failed to create replayer path={} err={}", kvconfig->db_trace_path, st.ToString());
	return translate_db_op_status(st);
  }
  st = replayer->Prepare();
  LOG_INFO("repare replayer result={}", st.ToString());
  st = replayer->Replay(rocksdb::ReplayOptions(1, 2.0), nullptr);
  LOG_INFO("replay result={}", st.ToString());
  return 0;
}

int kvs_scan(kvs_handle_t *kvs, int cf_id) {
  LOG_INFO("will scan cf_id={}", cf_id);
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  rocksdb::DB *db = wrapper->db_;
  // scan the default cf only.
  // will pass a column-family handle later.
  auto iter = db->NewIterator(wrapper->roptions_);

  iter->SeekToFirst();
  while (iter->Valid()) {
	auto key_prefix= (uint32_t*)iter->key().data();
	auto val_prefix = (uint32_t*)iter->value().data();

	LOG_INFO("key={}, value={}", *key_prefix, *val_prefix);
	iter->Next();
  }
  delete iter;
  return 0;
}

int kvs_put(kvs_handle_t *kvs,
            const void *key, size_t klen,
            const void *val, size_t vlen) {
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  rocksdb::DB *db = wrapper->db_;
//  LOG_INFO("will write to db, key={} klen={} value={} vlen={}", (void*)key, klen, (void*)val, vlen);
  rocksdb::Status st = db->Put(wrapper->woptions_default_,
                               Slice((const char *)key, klen),
                               Slice((const char *)val, vlen));
  if (!st.ok()) {
    LOG_ERROR("put failed. error='{}'", st.ToString());
    if (is_status_corrupted(st)) {
      LOG_ERROR("**** ROCKSDB ERROR : error='{}' ****", st.ToString());
    }
    return translate_status(st);
  }
  return 0;
}

int kvs_write_with_cb(kvs_handle_t *kvs, const void *key, size_t klen,
					  const void *val, size_t vlen) {
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  rocksdb::WriteBatch batch;
  size_t handle_index = (size_t)wrapper->default_cfh_.handle;
//  LOG_INFO("will put to handle index={}", handle_index);
  batch.Put(wrapper->cf_handles_[handle_index],
			Slice((const char *)key, klen), Slice((const char *)val, vlen));

  wrapper->wal_cb_->writer_pid = pthread_self();
  rocksdb::DB *db = wrapper->db_;
//  LOG_INFO("will write to db, key={} klen={} value={} vlen={}", (void*)key, klen, (void*)val, vlen);
  rocksdb::Status st = db->WriteWithCallback(wrapper->woptions_default_, &batch, wrapper->wal_cb_.get());
  if (!st.ok()) {
	LOG_ERROR("put failed. error='{}'", st.ToString());
	if (is_status_corrupted(st)) {
	  LOG_ERROR("**** ROCKSDB ERROR : error='{}' ****", st.ToString());
	}
	return translate_status(st);
  }
  return 0;
}

int kvs_put_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
			   const void *key, size_t klen, const void *val, size_t vlen) {
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  size_t handle_index = (size_t)cfh->handle;
  if (handle_index >= wrapper->cf_handles_.size()) {
	return EINVAL;
  }
  rocksdb::ColumnFamilyHandle *handle = wrapper->cf_handles_[handle_index];
  rocksdb::DB *db = ((DbWrapper *)kvs->kvs)->db_;
  Status st = db->Put(wrapper->woptions_default_, handle,
					  Slice((const char *)key, klen),
					  Slice((const char *)val, vlen));
  if (!st.ok()) {
	LOG_ERROR("put failed. error='{}'", st.ToString().c_str());
	return translate_status(st);
  }
  return 0;
}

int kvs_get(kvs_handle_t *kvs, const void *key, size_t klen, std::string* value) {
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  rocksdb::Slice skey((const char*)key, klen);

//  rocksdb::ReadOptions ropt = wrapper->roptions_;
//  rocksdb::PinnableSlice *s = (rocksdb::PinnableSlice *)val->buffer;
  rocksdb::Status status = wrapper->db_->Get(wrapper->roptions_, skey, value);

  if (!status.ok()) {
    if (status.IsNotFound()) {
      return ENOENT;
    } else {
      LOG_ERROR("failed to read key size={}, ret={}", klen, status.ToString());
      return translate_status(status);
    }
  }
  return 0;
}

int kvs_get_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
			   const void *key, size_t klen, kvs_buffer_t *val, void *db_snap) {
  DbWrapper *wrapper = (DbWrapper *) kvs->kvs;
  size_t handle_index = (size_t) cfh->handle;
  if (handle_index >= wrapper->cf_handles_.size()) {
	return EINVAL;
  }
  rocksdb::PinnableSlice *s = (rocksdb::PinnableSlice *) val->buffer;
  rocksdb::ColumnFamilyHandle *handle = wrapper->cf_handles_[handle_index];
  rocksdb::DB *db = ((DbWrapper *) kvs->kvs)->db_;
  rocksdb::ReadOptions ropt = wrapper->roptions_;
  if (db_snap) {
	ropt.snapshot = (const rocksdb::Snapshot *) db_snap;
  }
  rocksdb::Status status = wrapper->db_->Get(ropt, handle, rocksdb::Slice((const char *) key, klen), s);
  if (!status.ok()) {
	s->clear();
	if (status.IsNotFound()) {
	  LOG_ERROR("key not exists in cf idx={}", handle_index);
	  return ENOENT;
	} else {
	  LOG_ERROR("read failure={} in cf idx={}", status.ToString(), handle_index);
	  return translate_status(status);
	}
  }
  return 0;
}

// Delete the range of [start_key, end_key).
// "start" is inclusive,  "end" is exclusive.
int kvs_deleterange_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
					   const void *start_key, size_t sklen, const void *end_key, size_t eklen) {
  DbWrapper *wrapper = (DbWrapper *)kvs->kvs;
  size_t handle_index = (size_t)cfh->handle;
  if (handle_index >= wrapper->cf_handles_.size()) {
	return EINVAL;
  }
  rocksdb::ColumnFamilyHandle *handle = wrapper->cf_handles_[handle_index];
  rocksdb::DB *db = ((DbWrapper *) kvs->kvs)->db_;
  Status st = wrapper->db_->DeleteRange(wrapper->woptions_default_, handle,
										rocksdb::Slice((const char*)start_key, sklen),
										rocksdb::Slice((const char*)end_key, eklen));
  if (!st.ok()) {
	LOG_ERROR("range delete failed, error={}", st.ToString());
	return translate_status(st);
  }
  return 0;
}

int kvs_init_buffer(kvs_buffer_t *buff) {
  if (!buff) {
	return EINVAL;
  }
  rocksdb::PinnableSlice *s = new rocksdb::PinnableSlice();
  if (!s) {
	return ENOMEM;
  }
  buff->buffer = s;
  return 0;
}

const char *kvs_get_buffer_data(kvs_buffer_t *buffer) {
  auto s = (rocksdb::PinnableSlice*)buffer->buffer;
  return s->data();
}

size_t kvs_get_buffer_size(kvs_buffer_t *buffer) {
  auto s = (rocksdb::PinnableSlice*)buffer->buffer;
  return s->size();
}

void kvs_destroy_buffer(kvs_buffer_t *buffer) {
  delete (rocksdb::PinnableSlice *)buffer->buffer;
}
