//
// Created by shawn.ouyang on 12/12/2023.
//


#include <ctime>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <string>
#include <sstream>

#include "util/logger.h"
#include "util/util.h"

uint64_t rdtsc() {
  uint64_t a, d;
  __asm__ volatile("rdtsc" : "=a"(a), "=d"(d));
  return (d << 32) | a;
}

uint64_t NowInUsec() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (ts.tv_sec*1000000 + ts.tv_nsec/1000);
}

bool SetThreadName(const pthread_t& tid, const std::string& thread_name) {
  int ret = pthread_setname_np(tid, thread_name.c_str());
  if (ret != 0) {
    LOG_ERROR("Cannot set name={} to threadid={}: {} {}", thread_name, tid, ret, strerror(ret));
    if (ret == ERANGE) {
      LOG_ERROR("The thread name is restricted to 16 characters, including ending zero");
    }
    return false;
  }
  return true;
}

std::string GetThreadName(const pthread_t& tid) {
  char name[300];
  if (pthread_getname_np(tid, name, 300) != 0) {
    LOG_ERROR("Cannot get name of threadid={}: {}", tid, strerror(errno));
    return "";
  }
  return std::string(name);
}

std::string CurrentThreadName() {
  return GetThreadName(pthread_self());
}

// Get pid of the calling thread.
pid_t GetThreadPid() {
  pid_t tid;
  tid = syscall(SYS_gettid);
  return tid;
}

void ShowVal(uint64_t val) {
  LOG_INFO("val={} : {}", val, Val2Str<uint64_t>(val));
}

template std::string Val2Str<uint64_t>(uint64_t val);
template std::string Val2Str<uint32_t>(uint32_t val);

template<typename T>
std::string Val2Str(T val) {
  T rev = 0;
  // bit-wise revert the original value.
  for (int i = 0; i < sizeof(T) * 8; i++) {
	rev <<= 1;
	rev |= (val & (T)1);
	val >>= 1;
  }
  // now convert the reverted value into string, the high-bits will be at beginning of string.
  std::stringstream ss;
  for (int i = 0; i < sizeof(val) * 8; i++) {
	ss << (rev & 0x1UL) ? "1" : "0";
	rev >>= 1;
  }
  return ss.str();
}

template uint64_t Revert(uint64_t val);
template uint32_t Revert(uint32_t val);

template<typename T>
T Revert(T val) {
  int bits = sizeof(T) * 8;
  T retv = 0;
  for (int i = 0; i < bits; i++) {
	retv <<= 1;
	retv |= (val & (T)1);
	val >>= 1;
  }
  return retv;
}
