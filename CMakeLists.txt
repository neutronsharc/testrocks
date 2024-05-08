cmake_minimum_required(VERSION 3.20)
project(testrocks VERSION 1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
add_definitions(-fPIC)

find_package(gflags REQUIRED)

#find_library(LIB_ROCKSDB librocksdb.a)
find_library(LIB_URING uring)

file(GLOB_RECURSE SRC ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/*/*.cpp)
file(GLOB_RECURSE HDR ${PROJECT_SOURCE_DIR}/include/*/*.h)

add_executable(testrocks ${SRC} ${HDR} src/util/test_utils.cpp src/rocks/wal_cb_test.cpp include/rocks/wal_cb_test.h)
MESSAGE("src files=" ${SRC} " , hdrs=" ${HDR})

target_include_directories(testrocks PUBLIC
        ./include/
        external/moodycamel
        external/spdlog/include)

target_link_libraries(testrocks PUBLIC
        spdlog
        rocksdb z lz4 zstd bz2 snappy bsd dl ${LIB_URING}
#        ${LIB_ROCKSDB} z lz4 zstd bz2 snappy bsd dl ${LIB_URING}
        gflags
        pthread)

add_subdirectory(external/spdlog)