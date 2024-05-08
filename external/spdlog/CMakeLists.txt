# Add project information
project(spdlog)

# Add project information
cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 14)
add_definitions(-fPIC)

file(GLOB_RECURSE SPDLOG_HDRS_ include/spdlog/*.h)
file(GLOB_RECURSE SPDLOG_SRCS_ src/spdlog.cpp src/stdout_sinks.cpp src/color_sinks.cpp src/file_sinks.cpp src/async.cpp
    src/cfg.cpp src/fmt.cpp)

option(SPDLOG_COMPILED_LIB "Build static library" ON)

# create spdlog static library
add_library(spdlog STATIC ${SPDLOG_SRCS_} ${SPDLOG_HDRS_})

target_compile_definitions(spdlog PUBLIC SPDLOG_COMPILED_LIB)
target_include_directories(spdlog PUBLIC include)
target_link_libraries(spdlog pthread)

#install(TARGETS spdlog DESTINATION lib COMPONENT libraries)
#install(FILES ${SPDLOG_HDRS_} DESTINATION include/* COMPONENT headers)