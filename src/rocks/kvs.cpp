//
// Created by shawn.ouyang on 12/10/2023.
//
#include <string>

#include "rocks/kvs.h"
#include "rocks/kvs_rocksdb.h"
#include "util/logger.h"


int pdfs_event_key_cmp(size_t a_size, const char *a_data, size_t b_size, const char *b_data) {
  const pdfs_event_key_t *a = (const pdfs_event_key_t *)a_data;
  const pdfs_event_key_t *b = (const pdfs_event_key_t *)b_data;

  CMP_RETURN(a->pek_shareid, b->pek_shareid);
  CMP_RETURN(a->pek_seqnum, b->pek_seqnum);
  return 0;
}

const char * pdfs_key_type_to_str(pdfs_key_type_t key_type) {
  switch (key_type) {
	case PDK_SBP: return "PDK_SBP";
	case PDK_DIRENT_SLOT: return "PDK_DIRENT_SLOT";
	case PDK_DIRENT_NAME: return "PDK_DIRENT_NAME";
	case PDK_INODE: return "PDK_INODE";
	case PDK_CHUNK_LOC: return "PDK_CHUNK_LOC";
	case PDK_CHUNK_REF: return "PDK_CHUNK_REF";
	case PDK_CHUNK: return "PDK_CHUNK";
	case PDK_CHUNK_PREALLOC: return "PDK_CHUNK_PREALLOC";
	case PDK_ORPHAN_INODE: return "PDK_ORPHAN_INODE";
	case PDK_ORPHAN_OBJ: return "PDK_ORPHAN_OBJ";
	case PDK_SNAP: return "PDK_SNAP";
	case PDK_EVENT: return "PDK_EVENT";
	case PDK_PROFILE_SET: return "PDK_PROFILE_SET";
	case PDK_EXPLICIT_PROFILE: return "PDK_EXPLICIT_PROFILE";
	case PDK_LAYOUT_RECORD: return "PDK_LAYOUT_RECORD";
	case PDK_CHUNK_COMPLIANCE: return "PDK_CHUNK_COMPLIANCE";
	case PDK_IMPORT_HARDLINKS: return "PDK_IMPORT_HARDLINKS";
	case PDK_UNASSIGNED_INST: return "PDK_UNASSIGNED_INST";
	case PDK_OBS_STATS: return "PDK_OBS_STATS";
	case PDK_DELINST: return "PDK_DELINST";
	case PDK_REPL_DIFF: return "PDK_REPL_DIFF";
	case PDK_REPL_RECV: return "PDK_REPL_RECV";
	case PDK_TIME_INDEX: return "PDK_TIME_INDEX";
	case PDK_ASSIM_STATE: return "PDK_ASSIM_STATE";
	case PDK_ASSIM_INODE: return "PDK_ASSIM_INODE";
	case PDK_HOLDING_PEN: return "PDK_HOLDING_PEN";
	case PDK_ICONTEXT: return "PDK_ICONTEXT";
	case PDK_SLO_SWEEPER_RESTART_INFO: return "PDK_SLO_SWEEPER_RESTART_INFO";
	case PDK_FH_CACHE: return "PDK_FH_CACHE";
	case PDK_KAFKA_CACHE: return "PDK_KAFKA_CACHE";
	case PDK_COUNT: /* fall through */
	default:
	  return "INVALID KEY TYPE";
  }
}
