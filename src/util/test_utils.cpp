//
// Created by shawn.ouyang on 3/28/2024.
//
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <string>
#include <sstream>

#include "util/logger.h"
#include "util/util.h"

typedef struct kvs_trace_entry {
  uint16_t  op;
  uint16_t  cf_id;   // column-family id, PDK_key type.
  uint32_t klen;
  uint32_t vlen;
  uint8_t  keybuf[0];  // key content. A dynamic area following the entry.

  size_t  size() {
	return sizeof(struct kvs_trace_entry) + klen;
  }
} __attribute__((__packed__)) kvs_trace_entry_t;

typedef struct kvs_trace_batch_header {
  uint32_t num_entries; // number of kv entries in this batch.
  uint32_t size;  // total bytes in this batch.
} kvs_trace_batch_header_t;

#include <ctype.h>
void TestAscii() {
  std::stringstream ss;
  uint8_t c;
  for (c = 0; c <= 127; c++) {
	printf("%d::  isascii=%d  ascii=%c\n", c, isascii(c), c >= 0x20 && c < 0x7f ? c : 0xff);
	ss << (char)(c >= 0x20 && c < 0x7f ? c : 0x80);
  }
  printf("ss::  %s\n", ss.str().c_str());
}

void TryOddsizedStruct() {
  uint64_t arr[7][4];
  printf("sizeof arr=%ld\n", sizeof(arr));
  return;
  std::unique_ptr<uint8_t[]> ubuf = std::make_unique<uint8_t[]>(100);
  uint8_t *pos = ubuf.get();
  for (int i = 0; i < 100; i++) {
	pos[i] = i;
  }
  return;

  uint8_t *buf;
  size_t bufsize = 1024 * 1024;
  posix_memalign((void**)&buf, 4096, bufsize);

  uint8_t *currpos = buf;

  kvs_trace_batch_header_t *hdr = (kvs_trace_batch_header_t*)currpos;
  hdr->num_entries = 2;
  hdr->size = 1234;
  currpos += sizeof(kvs_trace_batch_header_t);

  kvs_trace_entry_t *ent = (kvs_trace_entry_t*)currpos;
  ent->op = 1;
  ent->cf_id = 1;
  ent->klen = 35;
  ent->vlen = 96;
  LOG_INFO("ent1.size={} cfid={}", ent->size(), ent->cf_id);
  currpos += ent->size();

  ent = (kvs_trace_entry_t*)currpos;
  ent->op = 10;
  ent->cf_id = 2;
  ent->klen = 19;
  ent->vlen = 97;
  LOG_INFO("ent2.size={} cfid={}", ent->size(), ent->cf_id);
  currpos += ent->size();

  ent = (kvs_trace_entry_t*)currpos;
  ent->op = 10;
  ent->cf_id = 3;
  ent->klen = 25;
  ent->vlen = 97;
  LOG_INFO("ent3.size={} cfid={}", ent->size(), ent->cf_id);

  currpos = buf + sizeof(kvs_trace_batch_header_t);
  ent = (kvs_trace_entry_t*)currpos;  // ent1
  currpos += ent->size();
  ent = (kvs_trace_entry_t*)currpos;  // ent2
  currpos += ent->size();
  ent = (kvs_trace_entry_t*)currpos;  // ent3

  LOG_INFO("rewind ent3.size={}, cfid={}", ent->size(), ent->cf_id);

  free(buf);
};