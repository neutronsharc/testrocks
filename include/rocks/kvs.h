/* Copyright (C) 2019, Hammerspace, Inc. All rights reserved.
 * General  This file defines a generic KV-Store C interface.
 *          It attempts to abstract the underlying KVS but many notions are
 *          taken from RockDB so support other types of KVS might require to
 *          change the API.
 *
 *          As a rule all the API functions that may fail return 0 for success
 *          or a positive number for failure.
 */

#ifndef __KVS_H_INCLUDED
#define __KVS_H_INCLUDED


#include <cstdint>
#include <cstddef>
#include <linux/limits.h>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

/*
 *  Three-way comparison function.  Returns value:
 *   < 0 iff "a" < "b",
 *   == 0 iff "a" == "b",
 *   > 0 iff "a" > "b"
 *   Setting a kvs_compare_t function in the kvs_config options will replace the
 *   default key comparator. In case of an existing DB the function must
 *   maintain the relative ordering of exiting keys.
 */
typedef int (*kvs_compare_t)(size_t a_size, const char *a_data,
                             size_t b_size, const char *b_data);

/**
 * KVS Configuration struct
 * Exposes a small subset of the available KVS configuration options.
 * More options will be added upon need.
 */
typedef struct kvs_config {
  size_t memtable_pcnt_per_share;
  uint64_t block_cache_pcnt;
  bool create_if_missing;
  bool error_if_exists;
  bool use_snappy_compression;
  bool optimize_for_universal_style_compaction;
  bool optimize_for_level_style_compaction;
  bool sync;
  bool disable_dbwal;
  bool wal_reuse;
  bool wal_callback;  // do callback after wal write completes.
  bool odirect_read;
  bool odirect_flush_compact;
  const char *wal_dir;
  const char *db_dir;

  kvs_compare_t key_cmp_func;
  int max_open_files;
  int write_buffer_size_mb;  // write buf size in MB.
  size_t blkcache_size;  // db block cache size in bytes.
  int ncpus;
  bool db_trace;  // record db trace
  bool db_replay;  // record db trace
  const char* db_trace_path;  // full path of a trace file.

  std::string ToString() const;
} kvs_config_t;


// default kvs-config params.
static const kvs_config_t DEFAULT_KVS_CONFIG = {
  .memtable_pcnt_per_share = 5,
  .block_cache_pcnt = 5,
  .create_if_missing = true,
  .error_if_exists = false,
  .use_snappy_compression = true,
  .optimize_for_universal_style_compaction = false,
  .optimize_for_level_style_compaction = false,
  .sync = true,  // fsync after each db write (fsync wal file).
  .disable_dbwal = false,
  .wal_reuse = false,
  .odirect_read = false,
  .odirect_flush_compact = false,   // true,
  .wal_dir = NULL,
  .key_cmp_func = NULL,
  .max_open_files = 10000,
  .write_buffer_size_mb = 32,
  .blkcache_size = 1024UL * 1024 * 512,
  .ncpus = 2,
  .db_trace = false,
  .db_replay = false,
  .db_trace_path = NULL,
};

/*
 * The following structs define the various handles used by the KVS operations.
 */
class PdfsWalWriteCallback;
typedef struct kvs_handle {
	char name[PATH_MAX];
	void *kvs;  // pointer to DbWrapper.
} kvs_handle_t;

typedef struct kvs_buffer {
	void *buffer;   // pointer to PinnableSlice.
} kvs_buffer_t;

typedef struct kvs_txn {
	void *txn;
	char wrapper_mem[120]; /* >= sizeof(TxnWrapper) */
} kvs_txn_t;

typedef struct kvs_snap {
	const void *snap;
} kvs_snap_t;

/* column family handle */
typedef struct cf_handle {
	void *handle;   // this is actually index into the kvs.column_families[].
} cf_handle_t;

typedef struct kvs_iter {
	void *iter;
	kvs_handle_t *handle;
	cf_handle_t *cfh;
} kvs_iter_t;

typedef struct kvs_tmp_iter {
	void *iter;
	kvs_handle_t *handle;
} kvs_tmp_iter_t;

extern __thread kvs_handle_t *cur_handle;
extern __thread cf_handle_t *cur_cfh;
extern __thread bool busted_delinst_cleanup;

//// newly-added test code.
typedef enum {
  PDK_SBP = 0,          /* MUST BE FIRST!!!           uint_8 => sb                  */
  PDK_DIRENT_SLOT,      /*      dirent_name_key => dirent_slot         */
  PDK_DIRENT_NAME,      /*      dirent_slot_key => dirent_name         */
  PDK_INODE,            /*            inode_key => inode               */
  PDK_CHUNK_LOC,        /*             cloc_key => chunk_loc           */
  PDK_CHUNK_REF,        /*            chunk_key => cloc_key[]          */
  PDK_CHUNK,            /*            chunk_key => chunk               */
  PDK_CHUNK_PREALLOC,   /*   chunk_prealloc_key => chunk_prealloc      */
  PDK_ORPHAN_INODE,     /*            inode_key => inode               */
  PDK_ORPHAN_OBJ,       /*       orphan_obj_key => event_update_object */
  PDK_SNAP,             /*             snap_key => snap                */
  PDK_EVENT,            /*            event_key => event_update_object */
  PDK_PROFILE_SET,      /*      profile_set_key => profile_set DEPRECATED starting 6.4 */
  PDK_EXPLICIT_PROFILE, /* explicit_profile_key => <null> DEPRECATED starting 6.4 */
  PDK_LAYOUT_RECORD,    /*    layout_record_key => <null>              */
  PDK_CHUNK_COMPLIANCE, /*       compliance_key => <blob>              */
  PDK_IMPORT_HARDLINKS, /*  import_hardlink_key => import_hardlink     */
  PDK_UNASSIGNED_INST,  /*  unassigned_inst_key => <null>              */
  PDK_OBS_STATS,        /*        obs_stats_key => obs_stats_val       */
  PDK_DELINST,          /*          delinst_key => <null>              */
  PDK_REPL_DIFF,        /*        repl_diff_key => repl_diff           */
  PDK_REPL_RECV,        /*        repl_recv_key => repl_recv           */
  PDK_TIME_INDEX,       /*       time_index_key => <null>              */
  PDK_ASSIM_STATE,      /*      assim_state_key => assim_state         */
  PDK_ASSIM_INODE,      /*      assim_inode_key => assim_inode         */
  PDK_HOLDING_PEN,      /*      holding_pen_key => holding_pen         */
  PDK_ICONTEXT,         /*         icontext_key => icontext            */
  /*         slo_sweeper_restart_info    */
  PDK_SLO_SWEEPER_RESTART_INFO, /* slo_sweeper_restart_info_key ==>    */
  PDK_FH_CACHE,         /*         fh_cache_key => fh_cache            */
  PDK_KAFKA_CACHE,      /*      kafka_cache_key => kafka_cache         */
#ifdef PDFS_OD_UPGRADE_TEST
  /* keep this as the last entry                                       */
	PDK_TEST_PT_PT,       /*        od_test_ptkey => od_test_ptval       */
#endif
  PDK_COUNT
} pdfs_key_type_t;

typedef struct Persistence {
  kvs_handle_t  kvs_handle;   // kvdb, a pointer to DBWrapper.
  std::string dbname;  // the path of db.
  cf_handle_t   cf_handles[PDK_COUNT];  // It's actually index into DBWrapper.column_families[].
} Persistence;


#define CMP_RETURN(A, B) \
    if (A != B) { \
        return A < B ? -1 : 1; \
    }
/**
 * struct pdfs_event_key - key for on-disk event used for ACK/NACK-ing things
 *                         to the data mgr.
 * @pek_seqnum:         rocksdb sequence number assigned at commit
 */
typedef struct pdfs_event_key_v1 {
  uint32_t pek_version;
  uint32_t pek_shareid;
  uint64_t pek_seqnum;
} pdfs_event_key_v1_t;
typedef pdfs_event_key_v1_t pdfs_event_key_t;

typedef struct pdfs_event_value_v1 {
  uint64_t seqnum;
} pdfs_event_value_v1_t;
typedef pdfs_event_value_v1_t pdfs_event_value_t;

typedef void (*kvs_callback_t)(void *ctx);
typedef void (*kvs_put_cb_t)(int keytype, const void *key, size_t klen,
                             const void *val, size_t vlen, void *ctx);
typedef void (*kvs_delete_cb_t)(int keytype, const void *key, size_t klen,
                                void *ctx);
typedef void (*kvs_merge_cb_t)(int keytype, const void *key, size_t klen,
                               const void *val, size_t vlen, void *ctx);

void get_wal_cycles(uint64_t *write_cycles, uint64_t *wa, uint64_t *wp,
                    uint64_t *ws, uint64_t *sa, uint64_t *sp, uint64_t *ss);
void clear_wal_cycles(void);

/* DB Allocation functions */
/* Initializes the given KVS handle */
int kvs_init_db(kvs_handle_t *kvs, kvs_config_t *kvsc);

/* Adds a column family to the DB, must be called before the DB is opened.
 * Basically column families create a separation within the key space of the
 * DB and can have different configurations.
 *
 * For further understanding please refer to the RocksDB documentation.
 */
int kvs_add_column_family(kvs_handle_t *kvs, kvs_config_t *config,
                          const char *key_name, cf_handle_t *handle,
                          size_t total_mem, bool universal_compaction,
                          bool use_blobdb, size_t blob_minsz);
int kvs_drop_column_family(kvs_handle_t *kvs, cf_handle_t *cfh);

/* open the data base */
//int kvs_open_db(kvs_handle_t *kvs, bool read_only, uint64_t total_mem, uint64_t block_cache_pcnt);
int kvs_open_db(kvs_handle_t *kvs, bool read_only);

/* Close the data base */
void kvs_close_db(kvs_handle_t *kvs);
/* Destroys the given KVS handle, DB must be closed before this call */
void kvs_destroy_db(kvs_handle_t *kvs);
/* Destroy the contents of the specified database,
 * Be very careful using this method. */
int kvs_delete_db(const char *path);

void kvs_report_db_memory_usage(const char *outfile);


/* Buffer operations
 * Buffers are used in by the get functions in order to return the data to the
 * caller
 */

/* Initialize the given buffer handle */
int kvs_init_buffer(kvs_buffer_t *buff);

/* Returns a pointer to the buffer data */
const char *kvs_get_buffer_data(kvs_buffer_t *buffer);

/* Destroys the given buffer handle */
void kvs_destroy_buffer(kvs_buffer_t *buffer);

/* Returns the size of the buffer */
size_t kvs_get_buffer_size(kvs_buffer_t *buffer);


int kvs_start_trace(kvs_config *kvconfig, kvs_handle_t *kvs);
int kvs_end_trace(kvs_handle_t *kvs);
int kvs_replay(kvs_config_t *kvconfig, kvs_handle_t *kvs);
int kvs_scan(kvs_handle_t *kvs, int cf_id);

/* Basic CRUD operations */
int kvs_put(kvs_handle_t *kvs, const void *key, size_t klen,
            const void *val, size_t vlen);

int kvs_write_with_cb(kvs_handle_t *kvs, const void *key, size_t klen,
					  const void *val, size_t vlen);

/* Set the database entry for "key" to "val".
 * If "key" already exists, it will be overwritten.
 * */
int kvs_put_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
               const void *key, size_t klen, const void *val, size_t vlen);

/* If the database contains an entry for "key" store the
 * corresponding value in *val. Access the data stored in val is done by the
 * kvs_get_buffer_* functions.
 *
 * If there is no entry for "key" returns ENOENT.
 */
int kvs_get_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
               const void *key, size_t klen, kvs_buffer_t *val, void *db_snap);

/**
 * Read the default column-family.
 *
 * If there is no entry for "key" returns ENOENT.
 */
// int kvs_get(kvs_handle_t *kvs, const void *key, size_t klen, kvs_buffer_t *val);
int kvs_get(kvs_handle_t *kvs, const void *key, size_t klen, std::string* val);


/* Remove the database entry (if any) for "key".
*  It is not an error if "key" did not exist in the database.
 */
int kvs_delete_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
                  const void *key, size_t klen);
int kvs_txn_compact_range_cf(kvs_txn_t *txn, cf_handle_t *cfh,
                             const void *start_key, size_t start_klen,
                             const void *end_key, size_t end_klen);

int kvs_deleterange_cf(kvs_handle_t *kvs, cf_handle_t *cfh,
					   const void *start_key, size_t sklen, const void* end_key, size_t eklen);

/* Snapshot operations */
int kvs_snap_create(kvs_handle_t *kvs, void **rval);
void kvs_snap_release(kvs_handle_t *kvs, void *db_snap);

/* Iterator operations */

/* Initialize the given iterator handle */
int kvs_init_iter_cf(kvs_handle_t *kvs, cf_handle_t *cfh, kvs_iter_t *iter);
/* For long running scans use the following function. */
int kvs_init_iter_cf_snap(kvs_handle_t *kvs, cf_handle_t *cfh,
                          kvs_iter_t *iter, void *db_snap);

/* Destroys the given iterator handle */
void kvs_destroy_iter(kvs_iter_t *iter);

/* Refresh a previously created iterator to "now" */
int kvs_refresh_iter(kvs_iter_t *iter);

/* Returns true if the iterator is currently pointing to a key/value pair */
int  kvs_iter_valid(kvs_iter_t *iter);

/* rocksdb perf stats */
void kvs_perf_tracking(bool enabled);
typedef struct rocks_perf_stat rocks_perf_stat_t;
void kvs_perf_collect(rocks_perf_stat_t *st);
void kvs_perf_clear(void);
void kvs_perf_append(rocks_perf_stat_t *src, rocks_perf_stat_t *dst);

/* The following calls move the iterator to the requested position.
 * After this call the caller should verify the iterator is still valid.
 */
void kvs_seek_to_first(kvs_iter_t *iter);
void kvs_seek_to_last(kvs_iter_t *iter);
void kvs_iter_seek(kvs_iter_t *iter, void *key, size_t klen);
void kvs_iter_next(kvs_iter_t *iter);
void kvs_iter_prev(kvs_iter_t *iter);

/* Returns the key of the current entry, The pointer for the returned buffer
* is valid only until the next modification of the iterator.
 */
const void *kvs_iter_key(kvs_iter_t *iter, size_t *klen);
/* Returns the value of the current entry, The pointer for the returned buffer
* is valid only until the next modification of the iterator.
 */
const void *kvs_iter_val(kvs_iter_t *iter, size_t *vlen);


/* Transaction operations */

/* Initialize the given transaction handle */
int kvs_init_txn(kvs_handle_t *kvs, kvs_txn_t *txn);
/* Destroys the given transaction handle */
void kvs_destroy_txn(kvs_txn_t *txn);

/* Adds a put operation to the transaction */
int kvs_txn_put_cf(kvs_txn_t *txn, cf_handle_t *cfh, const void *key,
                   size_t klen, const void *val, size_t vlen);

/* Adds a merge operation to the transaction */
int
kvs_txn_merge_cf(kvs_txn_t *txn, cf_handle_t *cfh, const void *key, size_t klen,
                 const void *val, size_t vlen);

/* Adds a delete operation to the transaction */
int kvs_txn_delete_cf(kvs_txn_t *txn, cf_handle_t *cfh,
                      const void *key, size_t klen);
/* Atomically commits the changes registered in the transaction */
int kvs_txn_commit(kvs_txn_t *txn, bool sync);

/* return the size of the byte stream in the write batch */
size_t kvs_txn_size(kvs_txn_t *txn);

/* tmp KVS stuff */
int kvs_init_iter_tmp(kvs_handle_t *kvs, kvs_tmp_iter_t *iter);

/* Testing API */

/**
 * Usage of checkpoint with offline upgrade
 *  we ideally don't need checkpoints. We must anyway support downgrade of
 *  ondisk struct to previous version should the software update fails.
 *  However, we use checkpoints to improve our chance successful revert
 *  if something goes wrong.
 *  workflow,
 *    create checkpoint
 *    perform ondisk upgrade
 *    if (upgrade fail) {
 *      restore from checkpoint
 *    }
 *    delete checkpoint
 *
 * Usage of checkpoint with regular backup of share DB
 * workflow (cron job),
 *    for each share:
 *        if condition-X:
 *            delete oldest checkpoint
 *        create new checkpoint
 */

/* create checkpoint of DB */
int kvs_checkpoint_create(kvs_handle_t *kvs, const char *name);

/* delete checkpoint of DB */
int kvs_checkpoint_delete(const char *base_dir, const char *name);

/* restore DB from checkpoint */
int kvs_checkpoint_restore(const char *base_dir, const char *name, bool backup);

/* get checkpoint info */
int kvs_checkpoint_info(const char *base_dir, const char *name,
                        int64_t *created, int64_t *space_used);

int kvs_stats_get(kvs_handle_t *kvs, void *s);
int kvs_stats_clear(kvs_handle_t *kvs);

// comparator of PDK_EVENT cf.
int pdfs_event_key_cmp(size_t a_size, const char *a_data, size_t b_size, const char *b_data);

int kvs_db_open_byname(Persistence* persistence, kvs_config_t* kvs_config, const char* dbname);

const char * pdfs_key_type_to_str(pdfs_key_type_t key_type);


#ifdef __cplusplus
};
#endif

#endif
