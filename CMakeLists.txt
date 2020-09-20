cmake_minimum_required(VERSION 3.7)
project(multiqueue)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3 -Wall -Wextra -pedantic -pthread -DPADDING=128 -DALIGNMENT=128 -DDISTPADDING=not_padded -DQUEUEPADDING=not_padded")
find_package(benchmark REQUIRED)

add_executable(benchmark benchmark.cpp dijkstra.h multiqueue.h)
TARGET_LINK_LIBRARIES(benchmark LINK_PUBLIC benchmark::benchmark)

add_executable(test multiqueue_tests.cpp multiqueue.h)