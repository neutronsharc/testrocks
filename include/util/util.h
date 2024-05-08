//
// Created by shawn.ouyang on 12/9/2023.
//

#ifndef TESTROCKS_UTIL_H
#define TESTROCKS_UTIL_H

#include <string>
#include <sstream>

uint64_t NowInUsec();

bool SetThreadName(const pthread_t& tid, const std::string& thread_name);

std::string GetThreadName(const pthread_t& tid);

std::string CurrentThreadName();

pid_t GetThreadPid();

uint64_t rdtsc();

template<typename T>
std::string Val2Str(T val);

template<typename T>
T Revert(T val);

void ShowVal(uint64_t val);

#endif //TESTROCKS_UTIL_H
